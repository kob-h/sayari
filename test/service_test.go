package integration_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/kob-h/docpipeline/internal/domain"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/service"
	"github.com/kob-h/docpipeline/internal/store"
)

// newSubmitService returns a DocumentService and its store over the test
// Postgres and Redis, with a clean slate. The store handle lets tests set up
// preconditions (e.g. drive a document to COMPLETED) directly.
func newSubmitService(t *testing.T) (*service.DocumentService, *store.Store) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker, skipped with -short")
	}
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

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
	broker := queue.NewRedisBroker(redisAddr, "", time.Second, log)
	if err := flushRedis(ctx, broker); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	t.Cleanup(func() { st.Close(); _ = broker.Close() })

	return service.NewDocumentService(st, broker, log), st
}

// The submit decision matrix is business logic owned by the service layer (not
// the store). This verifies each branch.
func TestService_SubmitDecisionMatrix(t *testing.T) {
	ctx := context.Background()
	svc, st := newSubmitService(t)

	// New document -> accepted, run_version 1, PENDING.
	res, err := svc.Submit(ctx, "doc", "Jane Doe joined Acme Corp.", domain.RerunPartial)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted || res.FullRerun || res.Document.RunVersion != 1 {
		t.Errorf("new doc: got %+v, want accepted run_version=1 no full-rerun", res)
	}
	if res.Document.Status != domain.DocPending {
		t.Errorf("new doc status: got %s, want PENDING", res.Document.Status)
	}

	// Partial resubmit of a still-PENDING doc -> accepted (resume in place), same run.
	again, err := svc.Submit(ctx, "doc", "Jane Doe joined Acme Corp.", domain.RerunPartial)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Accepted || again.Document.RunVersion != 1 {
		t.Errorf("partial resubmit: got %+v, want accepted run_version=1", again)
	}

	// Full rerun -> accepted, FullRerun, run_version bumped.
	full, err := svc.Submit(ctx, "doc", "Different text now.", domain.RerunFull)
	if err != nil {
		t.Fatal(err)
	}
	if !full.Accepted || !full.FullRerun || full.Document.RunVersion != 2 {
		t.Errorf("full rerun: got %+v, want accepted full-rerun run_version=2", full)
	}

	// Partial resubmit of a COMPLETED doc -> NOT accepted (idempotent no-op).
	driveToCompleted(t, svc, st, "done", "Acme Corp.")
	noop, err := svc.Submit(ctx, "done", "Acme Corp.", domain.RerunPartial)
	if err != nil {
		t.Fatal(err)
	}
	if noop.Accepted {
		t.Errorf("resubmit of completed doc should not be accepted, got %+v", noop)
	}
	if noop.Document.Status != domain.DocCompleted {
		t.Errorf("completed doc status: got %s, want COMPLETED", noop.Document.Status)
	}
}

// driveToCompleted submits a document and drives it to COMPLETED via an empty
// extraction (zero tokens to classify), using the store's primitives directly.
// This gives a deterministic COMPLETED document without standing up the workers.
func driveToCompleted(t *testing.T, svc *service.DocumentService, st *store.Store, id, text string) {
	t.Helper()
	ctx := context.Background()
	if _, err := svc.Submit(ctx, id, text, domain.RerunPartial); err != nil {
		t.Fatal(err)
	}
	err := st.WithinTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.SetExtractionStarted(ctx, id); err != nil {
			return err
		}
		return tx.MarkExtractionCompletedEmpty(ctx, id)
	})
	if err != nil {
		t.Fatal(err)
	}
	final, _ := st.GetDocument(ctx, id)
	if final.Status != domain.DocCompleted {
		t.Fatalf("precondition: expected %s to be COMPLETED, got %s", id, final.Status)
	}
}
