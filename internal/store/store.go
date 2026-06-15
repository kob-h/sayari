// Package store is the Postgres-backed persistence layer and the source of truth
// for all document and token state. Every state transition that must survive a
// crash happens here, inside a transaction where correctness demands it.
package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
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

// UpsertResult reports what AcceptDocument did so the caller knows whether to
// (re)enqueue extraction.
type UpsertResult struct {
	Document     domain.Document
	Enqueue      bool // whether an extract job should be published
	WasFullRerun bool
}

// AcceptDocument creates a new document or applies a rerun, returning the
// resulting manifest. It is the single entry point for POST /process.
//
//   - New document: inserted as PENDING (run_version 1).
//   - mode=full: tokens are deleted and the manifest reset in one transaction,
//     with run_version bumped to fence any in-flight workers from the old run.
//   - mode=partial on a COMPLETED doc: no-op (idempotent); Enqueue is false.
//   - mode=partial on an active/failed doc: re-enqueue to resume; state is left
//     intact so already-done work is not repeated.
func (s *Store) AcceptDocument(ctx context.Context, id, text string, mode domain.RerunMode) (UpsertResult, error) {
	hash := HashText(text)
	var res UpsertResult

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		existing, err := getDocumentTx(ctx, tx, id, true)
		switch {
		case errors.Is(err, domain.ErrNotFound):
			doc, err := insertDocument(ctx, tx, id, text, hash)
			if err != nil {
				return err
			}
			res = UpsertResult{Document: doc, Enqueue: true}
			return nil
		case err != nil:
			return err
		}

		if mode == domain.RerunFull {
			doc, err := resetDocument(ctx, tx, id, text, hash)
			if err != nil {
				return err
			}
			res = UpsertResult{Document: doc, Enqueue: true, WasFullRerun: true}
			return nil
		}

		// Partial mode.
		if existing.Status == domain.DocCompleted {
			res = UpsertResult{Document: existing, Enqueue: false}
			return nil
		}
		// Resume an active or failed document. Move FAILED back to PENDING so the
		// extractor will pick it up again; leave others as-is.
		if existing.Status == domain.DocFailed {
			if _, err := tx.Exec(ctx,
				`UPDATE documents SET status='PENDING', updated_at=now() WHERE id=$1`, id); err != nil {
				return fmt.Errorf("reset failed doc: %w", err)
			}
			existing.Status = domain.DocPending
		}
		res = UpsertResult{Document: existing, Enqueue: true}
		return nil
	})
	if err != nil {
		return UpsertResult{}, err
	}
	return res, nil
}

func insertDocument(ctx context.Context, tx pgx.Tx, id, text, hash string) (domain.Document, error) {
	_, err := tx.Exec(ctx,
		`INSERT INTO documents (id, text, content_hash, status, run_version)
		 VALUES ($1, $2, $3, 'PENDING', 1)`, id, text, hash)
	if err != nil {
		return domain.Document{}, fmt.Errorf("insert document: %w", err)
	}
	return getDocumentTx(ctx, tx, id, false)
}

func resetDocument(ctx context.Context, tx pgx.Tx, id, text, hash string) (domain.Document, error) {
	if _, err := tx.Exec(ctx, `DELETE FROM tokens WHERE document_id=$1`, id); err != nil {
		return domain.Document{}, fmt.Errorf("delete tokens: %w", err)
	}
	_, err := tx.Exec(ctx,
		`UPDATE documents
		 SET text=$2, content_hash=$3, status='PENDING',
		     run_version = run_version + 1,
		     total_tokens=0, classified_count=0,
		     extraction_started_at=NULL, extraction_completed_at=NULL,
		     classification_started_at=NULL, classification_completed_at=NULL,
		     updated_at=now()
		 WHERE id=$1`, id, text, hash)
	if err != nil {
		return domain.Document{}, fmt.Errorf("reset document: %w", err)
	}
	return getDocumentTx(ctx, tx, id, false)
}
