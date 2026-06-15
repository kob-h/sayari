package integration_test

import (
	"os"
	"testing"
	"time"
)

// loadDoc reads a test document from testdata.
func loadDoc(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("../testdata/docs/" + name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// Scenario 1: happy path — process end-to-end and query results.
func TestIntegration_HappyPath(t *testing.T) {
	e := newEnv(t, 0)
	e.process("happy", loadDoc(t, "small.txt"), "partial")

	s := e.waitForStatus("happy", "COMPLETED", 30*time.Second)
	if s.Progress.Total == 0 {
		t.Fatal("expected some tokens")
	}
	if s.Progress.Classified != s.Progress.Total {
		t.Errorf("classified %d != total %d", s.Progress.Classified, s.Progress.Total)
	}
	// Every token classified means at least one PERSON in the small doc.
	if e.tokenCount("happy", "classification=PERSON") == 0 {
		t.Error("expected at least one PERSON token")
	}
}

// Scenario 2: progress visibility — classified count advances over time.
func TestIntegration_ProgressTracking(t *testing.T) {
	e := newEnv(t, 15*time.Millisecond) // slow classification so progress is observable
	e.process("progress", loadDoc(t, "medium.txt"), "partial")

	// Wait until extraction sets a non-zero total.
	var total int
	for i := 0; i < 100 && total == 0; i++ {
		total = e.status("progress").Progress.Total
		time.Sleep(50 * time.Millisecond)
	}
	if total == 0 {
		t.Fatal("total never became non-zero")
	}

	// Observe a partial state (0 < classified < total) at least once.
	sawPartial := false
	for i := 0; i < 200; i++ {
		s := e.status("progress")
		if s.Progress.Classified > 0 && s.Progress.Classified < s.Progress.Total {
			sawPartial = true
			break
		}
		if s.Status == "COMPLETED" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawPartial {
		t.Error("never observed a partial progress state")
	}
	e.waitForStatus("progress", "COMPLETED", 30*time.Second)
}

// Scenario 3: partial rerun — kill the classifier mid-way, restart, resume.
func TestIntegration_PartialRerun(t *testing.T) {
	e := newEnv(t, 20*time.Millisecond)
	e.process("partial", loadDoc(t, "large.txt"), "partial")

	// Let some classification happen, then crash the classifier.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c := e.status("partial").Progress.Classified; c > 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	e.stopClassifier()

	mid := e.status("partial")
	if mid.Status == "COMPLETED" {
		t.Skip("classification finished before we could interrupt; rerun the test")
	}
	if mid.Progress.Classified == 0 {
		t.Fatal("expected some tokens classified before the crash")
	}
	t.Logf("crashed classifier at %d/%d", mid.Progress.Classified, mid.Progress.Total)

	// Restart the classifier; it must resume and finish without redoing work.
	e.startClassifier()
	final := e.waitForStatus("partial", "COMPLETED", 60*time.Second)
	if final.Progress.Classified != final.Progress.Total {
		t.Errorf("after resume: classified %d != total %d", final.Progress.Classified, final.Progress.Total)
	}
	// Counter never exceeds total => no double-counting on redelivery.
	if final.Progress.Classified > final.Progress.Total {
		t.Errorf("progress overshoot indicates double counting: %d > %d", final.Progress.Classified, final.Progress.Total)
	}
}

// Scenario 4: full rerun — reprocess from scratch, bump run_version, replace data.
func TestIntegration_FullRerun(t *testing.T) {
	e := newEnv(t, 0)
	e.process("full", loadDoc(t, "small.txt"), "partial")
	first := e.waitForStatus("full", "COMPLETED", 30*time.Second)

	e.process("full", loadDoc(t, "medium.txt"), "full") // new source text
	second := e.waitForStatus("full", "COMPLETED", 30*time.Second)

	if second.RunVersion <= first.RunVersion {
		t.Errorf("run_version should increase: %d -> %d", first.RunVersion, second.RunVersion)
	}
	// Token set fully replaced: only the current run's tokens are visible, and the
	// medium doc has more tokens than the small one.
	if second.Progress.Total <= first.Progress.Total {
		t.Errorf("expected more tokens after reprocessing larger doc: %d -> %d", first.Progress.Total, second.Progress.Total)
	}
	if e.tokenCount("full", "") != second.Progress.Total {
		t.Errorf("visible token count %d != current-run total %d (stale tokens leaked)",
			e.tokenCount("full", ""), second.Progress.Total)
	}
}

// Scenario 5: concurrent documents — three at once all complete.
func TestIntegration_ConcurrentDocuments(t *testing.T) {
	e := newEnv(t, 0)
	docs := map[string]string{
		"c1": loadDoc(t, "small.txt"),
		"c2": loadDoc(t, "medium.txt"),
		"c3": loadDoc(t, "large.txt"),
	}
	for id, text := range docs {
		e.process(id, text, "partial")
	}
	for id := range docs {
		s := e.waitForStatus(id, "COMPLETED", 60*time.Second)
		if s.Progress.Classified != s.Progress.Total || s.Progress.Total == 0 {
			t.Errorf("%s incomplete: %d/%d", id, s.Progress.Classified, s.Progress.Total)
		}
	}
}

// Scenario 6: query — filter tokens by classification and verify durations.
func TestIntegration_QueryAndDurations(t *testing.T) {
	e := newEnv(t, 0)
	e.process("query", loadDoc(t, "medium.txt"), "partial")
	s := e.waitForStatus("query", "COMPLETED", 30*time.Second)

	all := e.tokenCount("query", "")
	persons := e.tokenCount("query", "classification=PERSON")
	companies := e.tokenCount("query", "classification=COMPANY")
	dates := e.tokenCount("query", "classification=DATE")
	if persons == 0 || companies == 0 {
		t.Errorf("expected PERSON and COMPANY tokens, got person=%d company=%d", persons, companies)
	}
	if persons+companies+dates > all {
		t.Errorf("filtered subsets exceed total: %d+%d+%d > %d", persons, companies, dates, all)
	}

	// Durations must be recorded and non-negative.
	if s.Durations.ExtractionSeconds < 0 || s.Durations.ClassificationSeconds < 0 {
		t.Errorf("negative durations: %+v", s.Durations)
	}
}
