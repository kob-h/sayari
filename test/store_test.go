package integration_test

import (
	"context"
	"errors"
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
	if _, err := stExec(ctx, st, `TRUNCATE tokens, documents RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

// ApplyClassification must be idempotent: a redelivered classify never
// double-counts progress. This is the heart of crash safety.
func TestStore_ClassificationIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	res, err := st.AcceptDocument(ctx, "doc", "Jane Doe joined Acme Corporation.", domain.RerunPartial)
	if err != nil {
		t.Fatal(err)
	}
	ents := []domain.Entity{
		{Text: "Jane Doe", Type: domain.EntityPerson, Position: domain.Position{Sentence: 0, CharOffset: 0}},
		{Text: "Acme Corporation", Type: domain.EntityOrg, Position: domain.Position{Sentence: 0, CharOffset: 16}},
	}
	tokens, err := st.SaveExtraction(ctx, "doc", res.Document.RunVersion, ents)
	if err != nil {
		t.Fatal(err)
	}

	c := domain.Classification{Category: domain.CategoryPerson, Confidence: 0.9, Reasoning: "x"}
	// Apply the same classification three times (simulating redelivery).
	for i := 0; i < 3; i++ {
		if err := st.ApplyClassification(ctx, tokens[0].ID, res.Document.RunVersion, c); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	doc, _ := st.GetDocument(ctx, "doc")
	if doc.ClassifiedCount != 1 {
		t.Errorf("classified_count should be 1 after repeated apply, got %d", doc.ClassifiedCount)
	}
	if doc.Status != domain.DocClassifying {
		t.Errorf("status should still be CLASSIFYING (1 of 2), got %s", doc.Status)
	}

	// Classify the second token -> document completes.
	if err := st.ApplyClassification(ctx, tokens[1].ID, res.Document.RunVersion, c); err != nil {
		t.Fatal(err)
	}
	doc, _ = st.GetDocument(ctx, "doc")
	if doc.Status != domain.DocCompleted {
		t.Errorf("status should be COMPLETED, got %s", doc.Status)
	}
	if doc.ClassificationCompletedAt == nil {
		t.Error("classification_completed_at should be set on completion")
	}
}

// A full rerun must fence stale writes from the previous run.
func TestStore_StaleWriteRejected(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	res, _ := st.AcceptDocument(ctx, "doc", "Acme Corporation.", domain.RerunPartial)
	ents := []domain.Entity{{Text: "Acme Corporation", Type: domain.EntityOrg}}
	tokens, _ := st.SaveExtraction(ctx, "doc", res.Document.RunVersion, ents)
	oldRun := res.Document.RunVersion
	oldToken := tokens[0].ID

	// Full rerun bumps run_version and deletes old tokens.
	rerun, _ := st.AcceptDocument(ctx, "doc", "Acme Corporation.", domain.RerunFull)
	if rerun.Document.RunVersion <= oldRun {
		t.Fatalf("run_version did not advance: %d -> %d", oldRun, rerun.Document.RunVersion)
	}

	// A late write from the old run must be rejected (token is gone / stale run).
	err := st.ApplyClassification(ctx, oldToken, oldRun,
		domain.Classification{Category: domain.CategoryCompany, Confidence: 1})
	if !errors.Is(err, domain.ErrStaleWrite) && !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("expected stale/not-found error for old-run write, got %v", err)
	}
}

// FindOrphans must surface work that has no in-flight message so the reconciler
// can recover it.
func TestStore_FindOrphans(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	// A freshly accepted document is a PENDING orphan until its extract job runs.
	if _, err := st.AcceptDocument(ctx, "orphan", "Jane Doe.", domain.RerunPartial); err != nil {
		t.Fatal(err)
	}
	orphans, err := st.FindOrphans(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, o := range orphans {
		if o.Kind == "extract" && o.DocumentID == "orphan" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a pending document to be reported as an extract orphan, got %+v", orphans)
	}
}
