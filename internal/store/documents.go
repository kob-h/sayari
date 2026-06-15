package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/kob-h/docpipeline/internal/domain"
)

const docColumns = `id, text, content_hash, status, run_version, total_tokens, classified_count,
	extraction_started_at, extraction_completed_at,
	classification_started_at, classification_completed_at,
	created_at, updated_at`

// GetDocument returns a document by id, or domain.ErrNotFound.
func (s *Store) GetDocument(ctx context.Context, id string) (domain.Document, error) {
	rows, _ := s.pool.Query(ctx, `SELECT `+docColumns+` FROM documents WHERE id=$1`, id)
	return pgx.CollectExactlyOneRow(rows, scanDocument)
}

func getDocumentTx(ctx context.Context, tx pgx.Tx, id string, forUpdate bool) (domain.Document, error) {
	q := `SELECT ` + docColumns + ` FROM documents WHERE id=$1`
	if forUpdate {
		q += ` FOR UPDATE`
	}
	rows, _ := tx.Query(ctx, q, id)
	doc, err := pgx.CollectExactlyOneRow(rows, scanDocument)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Document{}, domain.ErrNotFound
	}
	return doc, err
}

func scanDocument(row pgx.CollectableRow) (domain.Document, error) {
	var d domain.Document
	err := row.Scan(
		&d.ID, &d.Text, &d.ContentHash, &d.Status, &d.RunVersion, &d.TotalTokens, &d.ClassifiedCount,
		&d.ExtractionStartedAt, &d.ExtractionCompletedAt,
		&d.ClassificationStartedAt, &d.ClassificationCompletedAt,
		&d.CreatedAt, &d.UpdatedAt,
	)
	return d, err
}

// BeginExtraction transitions a document PENDING -> EXTRACTING and stamps the
// extraction start time. It is idempotent and safe under concurrency: it uses a
// conditional UPDATE so only one worker wins the transition. The returned bool
// reports whether this call won (true) or the document was already past PENDING
// (false), in which case the caller should treat extraction as already underway.
//
// It returns the current document (at runVersion) so the worker knows which run
// it is processing.
func (s *Store) BeginExtraction(ctx context.Context, id string) (doc domain.Document, won bool, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		d, e := getDocumentTx(ctx, tx, id, true)
		if e != nil {
			return e
		}
		doc = d
		if d.Status != domain.DocPending {
			won = false
			return nil
		}
		_, e = tx.Exec(ctx,
			`UPDATE documents
			 SET status='EXTRACTING', extraction_started_at=now(), updated_at=now()
			 WHERE id=$1`, id)
		if e != nil {
			return fmt.Errorf("begin extraction: %w", e)
		}
		won = true
		doc.Status = domain.DocExtracting
		return nil
	})
	return doc, won, err
}

// SaveExtraction persists extracted entities as tokens and advances the document
// to CLASSIFYING in a single transaction. It is idempotent: tokens are upserted
// on their natural key, so a retried extraction converges to the same rows
// without duplicates. total_tokens is recomputed from the actual token count.
//
// It returns the persisted tokens (with their assigned IDs) so the caller can
// enqueue one classification job per token, and it fences stale runs.
func (s *Store) SaveExtraction(ctx context.Context, id string, runVersion int, entities []domain.Entity) ([]domain.Token, error) {
	var tokens []domain.Token
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		cur, err := getDocumentTx(ctx, tx, id, true)
		if err != nil {
			return err
		}
		if cur.RunVersion != runVersion {
			return domain.ErrStaleWrite
		}

		for _, e := range entities {
			if _, err := tx.Exec(ctx,
				`INSERT INTO tokens
				   (document_id, run_version, text, nlp_entity_type, page, sentence, char_offset, status)
				 VALUES ($1,$2,$3,$4,$5,$6,$7,'PENDING')
				 ON CONFLICT (document_id, run_version, sentence, char_offset, text) DO NOTHING`,
				id, runVersion, e.Text, e.Type, e.Position.Page, e.Position.Sentence, e.Position.CharOffset,
			); err != nil {
				return fmt.Errorf("insert token: %w", err)
			}
		}

		// Recompute total from the actual rows so reruns/dedup stay consistent.
		var total int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM tokens WHERE document_id=$1 AND run_version=$2`,
			id, runVersion).Scan(&total); err != nil {
			return fmt.Errorf("count tokens: %w", err)
		}

		// Advance to CLASSIFYING. If there are zero tokens, the document is
		// immediately COMPLETED (nothing to classify).
		newStatus := domain.DocClassifying
		completedClause := ""
		if total == 0 {
			newStatus = domain.DocCompleted
			completedClause = ", classification_completed_at=now()"
		}
		if _, err := tx.Exec(ctx,
			`UPDATE documents
			 SET status=$2, total_tokens=$3,
			     extraction_completed_at=now(),
			     classification_started_at=now()`+completedClause+`,
			     updated_at=now()
			 WHERE id=$1`, id, newStatus, total); err != nil {
			return fmt.Errorf("finish extraction: %w", err)
		}

		rows, _ := tx.Query(ctx,
			`SELECT `+tokenColumns+` FROM tokens
			 WHERE document_id=$1 AND run_version=$2 ORDER BY id`, id, runVersion)
		tks, err := pgx.CollectRows(rows, scanToken)
		if err != nil {
			return fmt.Errorf("load tokens: %w", err)
		}
		tokens = tks
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tokens, nil
}

// MarkFailed transitions a document to FAILED. Used when a stage hits an
// unrecoverable error.
func (s *Store) MarkFailed(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE documents SET status='FAILED', updated_at=now() WHERE id=$1`, id)
	return err
}
