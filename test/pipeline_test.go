package integration_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/kob-h/docpipeline/internal/domain"
	"github.com/kob-h/docpipeline/internal/llm"
	"github.com/kob-h/docpipeline/internal/nlp"
	"github.com/kob-h/docpipeline/internal/pipeline"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

// workers bundles a store plus the two stage workers for a test, all over the
// shared test Postgres and Redis with a clean slate.
type workers struct {
	st *store.Store
	ew *pipeline.ExtractionWorker
	cw *pipeline.ClassificationWorker
}

func newWorkers(t *testing.T) workers {
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

	return workers{
		st: st,
		ew: pipeline.NewExtractionWorker(st, broker, nlp.NewMockExtractor(), 1, log),
		cw: pipeline.NewClassificationWorker(st, broker, llm.NewMockClassifier(), 1, log),
	}
}

// Classification is idempotent and progress completes correctly. This behaviour
// moved from the store to the pipeline service layer; here we drive it through
// the worker Process methods.
func TestPipeline_ClassificationIdempotentAndCompletes(t *testing.T) {
	ctx := context.Background()
	w := newWorkers(t)

	insertDoc(t, w.st, "doc", "Jane Doe joined Acme Corporation.")
	if err := w.ew.Process(ctx, pipeline.ExtractJob{DocumentID: "doc", RunVersion: 1}); err != nil {
		t.Fatalf("extract: %v", err)
	}
	tokens, err := w.st.ListTokens(ctx, "doc", domain.TokenFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) < 2 {
		t.Fatalf("expected >=2 tokens, got %d", len(tokens))
	}
	// Context (the entity's sentence) must be captured at extraction and persisted
	// so the classifier receives "entity text + context".
	for _, tok := range tokens {
		if tok.Context == "" {
			t.Errorf("token %q has no persisted context", tok.Text)
		}
	}

	// Classify the first token three times (simulating redelivery): the counter
	// must advance exactly once.
	job := pipeline.ClassifyJob{TokenID: tokens[0].ID, DocumentID: "doc", RunVersion: 1}
	for i := 0; i < 3; i++ {
		if err := w.cw.Process(ctx, job); err != nil {
			t.Fatalf("classify %d: %v", i, err)
		}
	}
	doc, _ := w.st.GetDocument(ctx, "doc")
	if doc.ClassifiedCount != 1 {
		t.Errorf("classified_count should be 1 after repeated classify, got %d", doc.ClassifiedCount)
	}
	if doc.Status != domain.DocClassifying {
		t.Errorf("status should still be CLASSIFYING, got %s", doc.Status)
	}

	// Classify the rest -> document completes.
	for _, tok := range tokens[1:] {
		if err := w.cw.Process(ctx, pipeline.ClassifyJob{TokenID: tok.ID, DocumentID: "doc", RunVersion: 1}); err != nil {
			t.Fatal(err)
		}
	}
	doc, _ = w.st.GetDocument(ctx, "doc")
	if doc.Status != domain.DocCompleted {
		t.Errorf("status should be COMPLETED, got %s", doc.Status)
	}
	if doc.ClassifiedCount != doc.TotalTokens {
		t.Errorf("classified_count %d != total %d", doc.ClassifiedCount, doc.TotalTokens)
	}
	if doc.ClassificationCompletedAt == nil {
		t.Error("classification_completed_at should be set on completion")
	}
}

// A full rerun fences writes from the previous run: a late classify for an
// old-run token is dropped.
func TestPipeline_StaleWriteFenced(t *testing.T) {
	ctx := context.Background()
	w := newWorkers(t)

	insertDoc(t, w.st, "doc", "Acme Corporation grew.")
	if err := w.ew.Process(ctx, pipeline.ExtractJob{DocumentID: "doc", RunVersion: 1}); err != nil {
		t.Fatalf("extract: %v", err)
	}
	tokens, _ := w.st.ListTokens(ctx, "doc", domain.TokenFilter{})
	oldToken := tokens[0].ID

	// Full rerun: bump run_version and delete old tokens.
	rerun := resetDoc(t, w.st, "doc", "Acme Corporation grew.")
	if rerun.RunVersion != 2 {
		t.Fatalf("expected run_version 2 after reset, got %d", rerun.RunVersion)
	}

	// A late classify from the old run must be dropped (token is gone / stale run).
	err := w.cw.Process(ctx, pipeline.ClassifyJob{TokenID: oldToken, DocumentID: "doc", RunVersion: 1})
	if !errors.Is(err, domain.ErrStaleWrite) && !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("expected stale/not-found for old-run classify, got %v", err)
	}
}
