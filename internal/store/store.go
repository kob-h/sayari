// Package store is the Postgres-backed persistence layer and the source of truth
// for all document and token state. Every state transition that must survive a
// crash happens here, inside a transaction where correctness demands it.
package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kob-h/docpipeline/internal/domain"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store wraps a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres using dsn and returns a ready Store.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Ping verifies connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// migrateLockKey is an arbitrary, fixed key for the advisory lock that
// serialises migrations across concurrently-starting services.
const migrateLockKey = 4927013

// Migrate applies the embedded SQL migrations. Each file is idempotent
// (IF NOT EXISTS), so running this on every boot is safe.
//
// All three services run Migrate on startup, so it takes a Postgres advisory
// lock first: `CREATE TABLE IF NOT EXISTS` is NOT atomic against concurrent
// creators (they race on the implicit row type and one fails), so the lock makes
// exactly one service create the schema while the others wait and then no-op.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration conn: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrateLockKey) }()

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := conn.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}

// HashText returns the content hash used to detect source changes.
func HashText(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// Tx is a transaction-scoped repository. It exposes the primitive document
// operations the service layer composes inside a single transaction (a
// Unit-of-Work). It deliberately carries no business logic — the decision of
// *which* operation to run for a submit lives in the service layer.
type Tx struct {
	tx pgx.Tx
}

// WithinTx runs fn inside a single database transaction, giving it a Tx repository
// bound to that transaction. The transaction commits if fn returns nil and rolls
// back otherwise. This lets callers perform read-modify-write sequences (e.g. the
// submit decision) atomically without the store owning the decision.
func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, *Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return fn(ctx, &Tx{tx: tx})
	})
}

// GetDocumentForUpdate returns the document, locking its row for the rest of the
// transaction, or domain.ErrNotFound.
func (t *Tx) GetDocumentForUpdate(ctx context.Context, id string) (domain.Document, error) {
	return getDocumentTx(ctx, t.tx, id, true)
}

// InsertDocument creates a new PENDING document at run_version 1.
func (t *Tx) InsertDocument(ctx context.Context, id, text string) (domain.Document, error) {
	if _, err := t.tx.Exec(ctx,
		`INSERT INTO documents (id, text, content_hash, status, run_version)
		 VALUES ($1, $2, $3, 'PENDING', 1)`, id, text, HashText(text)); err != nil {
		return domain.Document{}, fmt.Errorf("insert document: %w", err)
	}
	return getDocumentTx(ctx, t.tx, id, false)
}

// ResetDocument deletes the document's tokens and resets its manifest to a fresh
// PENDING run, bumping run_version to fence any in-flight workers from the old run.
func (t *Tx) ResetDocument(ctx context.Context, id, text string) (domain.Document, error) {
	if _, err := t.tx.Exec(ctx, `DELETE FROM tokens WHERE document_id=$1`, id); err != nil {
		return domain.Document{}, fmt.Errorf("delete tokens: %w", err)
	}
	if _, err := t.tx.Exec(ctx,
		`UPDATE documents
		 SET text=$2, content_hash=$3, status='PENDING',
		     run_version = run_version + 1,
		     total_tokens=0, classified_count=0,
		     extraction_started_at=NULL, extraction_completed_at=NULL,
		     classification_started_at=NULL, classification_completed_at=NULL,
		     updated_at=now()
		 WHERE id=$1`, id, text, HashText(text)); err != nil {
		return domain.Document{}, fmt.Errorf("reset document: %w", err)
	}
	return getDocumentTx(ctx, t.tx, id, false)
}

// SetDocumentPending moves a document back to PENDING (used to resume a FAILED
// document without discarding completed work).
func (t *Tx) SetDocumentPending(ctx context.Context, id string) error {
	if _, err := t.tx.Exec(ctx,
		`UPDATE documents SET status='PENDING', updated_at=now() WHERE id=$1`, id); err != nil {
		return fmt.Errorf("set document pending: %w", err)
	}
	return nil
}
