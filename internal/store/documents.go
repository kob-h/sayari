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

// --- Document Tx primitives -------------------------------------------------
//
// These are unconditional, single-purpose persistence operations. They contain
// no state-machine decisions: the choice of *which* transition to apply belongs
// to the service layer (the pipeline workers), which calls these inside a
// WithinTx unit of work.

// SetExtractionStarted marks a document as EXTRACTING and stamps the start time.
func (t *Tx) SetExtractionStarted(ctx context.Context, id string) error {
	if _, err := t.tx.Exec(ctx,
		`UPDATE documents
		 SET status='EXTRACTING', extraction_started_at=now(), updated_at=now()
		 WHERE id=$1`, id); err != nil {
		return fmt.Errorf("set extraction started: %w", err)
	}
	return nil
}

// AdvanceToClassifying records the extracted token total and moves the document
// into the classification stage, stamping the extraction-end / classification-start
// boundary.
func (t *Tx) AdvanceToClassifying(ctx context.Context, id string, total int) error {
	if _, err := t.tx.Exec(ctx,
		`UPDATE documents
		 SET status='CLASSIFYING', total_tokens=$2,
		     extraction_completed_at=now(), classification_started_at=now(),
		     updated_at=now()
		 WHERE id=$1`, id, total); err != nil {
		return fmt.Errorf("advance to classifying: %w", err)
	}
	return nil
}

// MarkExtractionCompletedEmpty completes a document that extracted zero tokens
// (nothing to classify), stamping all stage boundaries at once.
func (t *Tx) MarkExtractionCompletedEmpty(ctx context.Context, id string) error {
	if _, err := t.tx.Exec(ctx,
		`UPDATE documents
		 SET status='COMPLETED', total_tokens=0,
		     extraction_completed_at=now(), classification_started_at=now(),
		     classification_completed_at=now(), updated_at=now()
		 WHERE id=$1`, id); err != nil {
		return fmt.Errorf("complete empty extraction: %w", err)
	}
	return nil
}

// IncrementClassifiedCount bumps the progress counter by one and returns the new
// value, so the caller can decide whether the document is now complete.
func (t *Tx) IncrementClassifiedCount(ctx context.Context, id string) (int, error) {
	var n int
	if err := t.tx.QueryRow(ctx,
		`UPDATE documents SET classified_count = classified_count + 1, updated_at=now()
		 WHERE id=$1 RETURNING classified_count`, id).Scan(&n); err != nil {
		return 0, fmt.Errorf("increment classified count: %w", err)
	}
	return n, nil
}

// MarkDocumentCompleted moves a document to COMPLETED and stamps completion time.
func (t *Tx) MarkDocumentCompleted(ctx context.Context, id string) error {
	if _, err := t.tx.Exec(ctx,
		`UPDATE documents
		 SET status='COMPLETED', classification_completed_at=now(), updated_at=now()
		 WHERE id=$1`, id); err != nil {
		return fmt.Errorf("mark document completed: %w", err)
	}
	return nil
}

// MarkFailed transitions a document to FAILED. Used when a stage hits an
// unrecoverable error.
func (s *Store) MarkFailed(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE documents SET status='FAILED', updated_at=now() WHERE id=$1`, id)
	return err
}
