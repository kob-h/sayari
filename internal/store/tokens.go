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

const tokenColumns = `id, document_id, run_version, text, context,
	page, sentence, char_offset, status, classification, confidence, reasoning,
	created_at, classified_at`

func scanToken(row pgx.CollectableRow) (domain.Token, error) {
	var t domain.Token
	var class *string
	err := row.Scan(
		&t.ID, &t.DocumentID, &t.RunVersion, &t.Text, &t.Context,
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

// --- Token Tx primitives ----------------------------------------------------
//
// Single-purpose persistence operations with no state-machine decisions. The
// classification service (the pipeline classification worker) composes these
// inside a WithinTx unit of work and owns the idempotency / stale-run / completion
// decisions.

// UpsertTokens inserts the entities as PENDING tokens, idempotently on their
// natural key (a retried extraction converges to the same rows, no duplicates).
func (t *Tx) UpsertTokens(ctx context.Context, docID string, runVersion int, entities []domain.Entity) error {
	for _, e := range entities {
		if _, err := t.tx.Exec(ctx,
			`INSERT INTO tokens
			   (document_id, run_version, text, context, page, sentence, char_offset, status)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,'PENDING')
			 ON CONFLICT (document_id, run_version, sentence, char_offset, text) DO NOTHING`,
			docID, runVersion, e.Text, e.Context, e.Position.Page, e.Position.Sentence, e.Position.CharOffset,
		); err != nil {
			return fmt.Errorf("insert token: %w", err)
		}
	}
	return nil
}

// CountTokens returns how many tokens exist for a document's run.
func (t *Tx) CountTokens(ctx context.Context, docID string, runVersion int) (int, error) {
	var n int
	if err := t.tx.QueryRow(ctx,
		`SELECT count(*) FROM tokens WHERE document_id=$1 AND run_version=$2`,
		docID, runVersion).Scan(&n); err != nil {
		return 0, fmt.Errorf("count tokens: %w", err)
	}
	return n, nil
}

// ListTokensForRun returns all tokens for a document's run, ordered by id.
func (t *Tx) ListTokensForRun(ctx context.Context, docID string, runVersion int) ([]domain.Token, error) {
	rows, _ := t.tx.Query(ctx,
		`SELECT `+tokenColumns+` FROM tokens
		 WHERE document_id=$1 AND run_version=$2 ORDER BY id`, docID, runVersion)
	tokens, err := pgx.CollectRows(rows, scanToken)
	if err != nil {
		return nil, fmt.Errorf("load tokens: %w", err)
	}
	return tokens, nil
}

// GetTokenForUpdate returns a token, locking its row for the rest of the
// transaction, or domain.ErrNotFound.
func (t *Tx) GetTokenForUpdate(ctx context.Context, id int64) (domain.Token, error) {
	rows, _ := t.tx.Query(ctx, `SELECT `+tokenColumns+` FROM tokens WHERE id=$1 FOR UPDATE`, id)
	tok, err := pgx.CollectExactlyOneRow(rows, scanToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Token{}, domain.ErrNotFound
	}
	return tok, err
}

// SetTokenClassified records a token's classification result and marks it
// CLASSIFIED.
func (t *Tx) SetTokenClassified(ctx context.Context, id int64, c domain.Classification) error {
	if _, err := t.tx.Exec(ctx,
		`UPDATE tokens
		 SET status='CLASSIFIED', classification=$2, confidence=$3, reasoning=$4, classified_at=now()
		 WHERE id=$1`, id, string(c.Category), c.Confidence, c.Reasoning); err != nil {
		return fmt.Errorf("update token: %w", err)
	}
	return nil
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
