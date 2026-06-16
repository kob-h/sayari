package integration_test

import (
	"context"
	"testing"

	"github.com/kob-h/docpipeline/internal/domain"
	"github.com/kob-h/docpipeline/internal/store"
)

// newStore returns a migrated, empty store backed by the test Postgres.
func newStore(t *testing.T) *store.Store {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker, skipped with -short")
	}
	ctx := context.Background()
	st, err := store.New(ctx, pgDSN)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := stExec(ctx, st, `TRUNCATE tokens, documents, outbox RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

// insertDoc creates a fresh PENDING document via the store's Unit-of-Work.
func insertDoc(t *testing.T, st *store.Store, id, text string) domain.Document {
	t.Helper()
	var doc domain.Document
	err := st.WithinTx(context.Background(), func(ctx context.Context, tx *store.Tx) error {
		d, e := tx.InsertDocument(ctx, id, text)
		doc = d
		return e
	})
	if err != nil {
		t.Fatalf("insert doc: %v", err)
	}
	return doc
}

// resetDoc performs a full-rerun reset via the store's Unit-of-Work.
func resetDoc(t *testing.T, st *store.Store, id, text string) domain.Document {
	t.Helper()
	var doc domain.Document
	err := st.WithinTx(context.Background(), func(ctx context.Context, tx *store.Tx) error {
		d, e := tx.ResetDocument(ctx, id, text)
		doc = d
		return e
	})
	if err != nil {
		t.Fatalf("reset doc: %v", err)
	}
	return doc
}

// Behaviour that used to live in the store (classification idempotency, stale-run
// fencing, completion) now lives in the pipeline service layer and is tested in
// pipeline_test.go. The transactional outbox + relay are tested in outbox_test.go.
