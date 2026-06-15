package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/kob-h/docpipeline/internal/domain"
)

const tokenColumns = `id, document_id, run_version, text, nlp_entity_type,
	page, sentence, char_offset, status, classification, confidence, reasoning,
	created_at, classified_at`

func scanToken(row pgx.CollectableRow) (domain.Token, error) {
	var t domain.Token
	var class *string
	err := row.Scan(
		&t.ID, &t.DocumentID, &t.RunVersion, &t.Text, &t.NLPEntityType,
		&t.Position.Page, &t.Position.Sentence, &t.Position.CharOffset,
		&t.Status, &class, &t.Confidence, &t.Reasoning,
		&t.CreatedAt, &t.ClassifiedAt,
	)
	if err != nil {
		return t, err
	}
	if class != nil {
		c := domain.Category(*class)
		t.Classification = &c
	}
	return t, nil
}

// GetToken returns a single token by id.
func (s *Store) GetToken(ctx context.Context, id int64) (domain.Token, error) {
	rows, _ := s.pool.Query(ctx, `SELECT `+tokenColumns+` FROM tokens WHERE id=$1`, id)
	t, err := pgx.CollectExactlyOneRow(rows, scanToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Token{}, domain.ErrNotFound
	}
	return t, err
}

// ApplyClassification persists a token's classification and advances the
// document's progress counter in a single transaction. This atomicity is the
// crux of crash safety: the result and the counter move together, so the
// "classified count" can never diverge from the classified rows.
//
// It is idempotent. If the token is already CLASSIFIED (e.g. a redelivered
// message), it is a no-op and the counter is not double-incremented. Stale runs
// (runVersion behind the document) are rejected with domain.ErrStaleWrite.
//
// When the increment makes classified_count reach total_tokens, the document is
// transitioned to COMPLETED and classification_completed_at is stamped.
func (s *Store) ApplyClassification(ctx context.Context, tokenID int64, runVersion int, c domain.Classification) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Lock the token row to serialise concurrent attempts on the same token.
		var (
			docID  string
			status domain.TokenStatus
			tokRun int
		)
		err := tx.QueryRow(ctx,
			`SELECT document_id, status, run_version FROM tokens WHERE id=$1 FOR UPDATE`, tokenID).
			Scan(&docID, &status, &tokRun)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock token: %w", err)
		}
		if tokRun != runVersion {
			return domain.ErrStaleWrite
		}
		if status == domain.TokenClassified {
			return nil // idempotent: already done, do not double-count
		}

		// Guard against a superseded run at the document level too.
		var docRun int
		if err := tx.QueryRow(ctx,
			`SELECT run_version FROM documents WHERE id=$1 FOR UPDATE`, docID).Scan(&docRun); err != nil {
			return fmt.Errorf("lock document: %w", err)
		}
		if docRun != runVersion {
			return domain.ErrStaleWrite
		}

		if _, err := tx.Exec(ctx,
			`UPDATE tokens
			 SET status='CLASSIFIED', classification=$2, confidence=$3, reasoning=$4, classified_at=now()
			 WHERE id=$1`, tokenID, string(c.Category), c.Confidence, c.Reasoning); err != nil {
			return fmt.Errorf("update token: %w", err)
		}

		if _, err := tx.Exec(ctx,
			`UPDATE documents
			 SET classified_count = classified_count + 1,
			     status = CASE WHEN classified_count + 1 >= total_tokens THEN 'COMPLETED' ELSE status END,
			     classification_completed_at = CASE
			         WHEN classified_count + 1 >= total_tokens THEN now()
			         ELSE classification_completed_at END,
			     updated_at = now()
			 WHERE id=$1`, docID); err != nil {
			return fmt.Errorf("advance progress: %w", err)
		}
		return nil
	})
}

// ListTokens returns tokens for a document (current run only) matching filter.
func (s *Store) ListTokens(ctx context.Context, docID string, f domain.TokenFilter) ([]domain.Token, error) {
	// Restrict to the document's current run so callers never see stale tokens
	// from a superseded run.
	args := []any{docID}
	where := []string{
		`document_id = $1`,
		`run_version = (SELECT run_version FROM documents WHERE id = $1)`,
	}
	add := func(clause string, val any) {
		args = append(args, val)
		where = append(where, clause+" $"+strconv.Itoa(len(args)))
	}
	if f.Classification != nil {
		add(`classification =`, string(*f.Classification))
	}
	if f.NLPEntityType != nil {
		add(`nlp_entity_type =`, string(*f.NLPEntityType))
	}
	if f.Status != nil {
		add(`status =`, string(*f.Status))
	}
	if f.Page != nil {
		add(`page =`, *f.Page)
	}

	q := `SELECT ` + tokenColumns + ` FROM tokens WHERE ` + strings.Join(where, " AND ") + ` ORDER BY id`
	if f.Limit > 0 {
		args = append(args, f.Limit)
		q += " LIMIT $" + strconv.Itoa(len(args))
	}
	if f.Offset > 0 {
		args = append(args, f.Offset)
		q += " OFFSET $" + strconv.Itoa(len(args))
	}

	rows, _ := s.pool.Query(ctx, q, args...)
	return pgx.CollectRows(rows, scanToken)
}

// OrphanJob describes a unit of work the reconciler must re-enqueue.
type OrphanJob struct {
	Kind       string // "extract" or "classify"
	DocumentID string
	TokenID    int64 // set when Kind == "classify"
	RunVersion int
}

// FindOrphans returns work that should be in flight but may have been lost
// between a Postgres write and a Redis publish (the only place the two systems
// can disagree). The reconciler republishes these jobs. Because all consumers
// are idempotent, re-enqueuing already-in-flight work is harmless.
//
//   - Documents stuck in PENDING/EXTRACTING -> need an extract job.
//   - Tokens still PENDING under a CLASSIFYING document -> need a classify job.
func (s *Store) FindOrphans(ctx context.Context, limit int) ([]OrphanJob, error) {
	var jobs []OrphanJob

	docRows, _ := s.pool.Query(ctx,
		`SELECT id, run_version FROM documents
		 WHERE status IN ('PENDING','EXTRACTING')
		 ORDER BY updated_at ASC LIMIT $1`, limit)
	docs, err := pgx.CollectRows(docRows, func(r pgx.CollectableRow) (OrphanJob, error) {
		var j OrphanJob
		j.Kind = "extract"
		return j, r.Scan(&j.DocumentID, &j.RunVersion)
	})
	if err != nil {
		return nil, fmt.Errorf("find orphan documents: %w", err)
	}
	jobs = append(jobs, docs...)

	tokRows, _ := s.pool.Query(ctx,
		`SELECT t.id, t.document_id, t.run_version
		 FROM tokens t
		 JOIN documents d ON d.id = t.document_id AND d.run_version = t.run_version
		 WHERE t.status='PENDING' AND d.status='CLASSIFYING'
		 ORDER BY t.created_at ASC LIMIT $1`, limit)
	toks, err := pgx.CollectRows(tokRows, func(r pgx.CollectableRow) (OrphanJob, error) {
		var j OrphanJob
		j.Kind = "classify"
		return j, r.Scan(&j.TokenID, &j.DocumentID, &j.RunVersion)
	})
	if err != nil {
		return nil, fmt.Errorf("find orphan tokens: %w", err)
	}
	jobs = append(jobs, toks...)

	return jobs, nil
}
