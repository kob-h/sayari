package llm

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kob-h/docpipeline/internal/domain"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestOllamaClassifier_ParsesStructuredResponse(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("missing/incorrect auth header: %q", got)
		}
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"message":{"role":"assistant","content":"{\"category\":\"COMPANY\",\"confidence\":0.91,\"reasoning\":\"corporate suffix\"}"}}`)
	}))
	defer srv.Close()

	c := NewOllamaClassifier(srv.URL, "test-key", "test-model", quietLogger())
	got, err := c.Classify(context.Background(), domain.Token{
		Text:    "Acme Corp",
		Context: "He works at Acme Corp downtown.",
	})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	// The token's context must be passed to the model (interface requires
	// "entity text + context").
	if !strings.Contains(gotBody, "Acme Corp downtown") {
		t.Errorf("request body should include the token context, got: %s", gotBody)
	}
	if got.Category != domain.CategoryCompany {
		t.Errorf("category: got %s, want COMPANY", got.Category)
	}
	if got.Confidence != 0.91 {
		t.Errorf("confidence: got %v, want 0.91", got.Confidence)
	}
}

func TestOllamaClassifier_RetriesOn429(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, "rate limited")
			return
		}
		_, _ = io.WriteString(w, `{"message":{"content":"{\"category\":\"PERSON\",\"confidence\":0.8,\"reasoning\":\"name\"}"}}`)
	}))
	defer srv.Close()

	c := NewOllamaClassifier(srv.URL, "", "m", quietLogger(), WithMaxRetries(5))
	got, err := c.Classify(context.Background(), domain.Token{Text: "Jane Doe"})
	if err != nil {
		t.Fatalf("Classify after retries: %v", err)
	}
	if got.Category != domain.CategoryPerson {
		t.Errorf("category: got %s, want PERSON", got.Category)
	}
	if calls.Load() != 3 {
		t.Errorf("expected 3 calls (2 x 429 + 1 success), got %d", calls.Load())
	}
}

func TestOllamaClassifier_InvalidCategoryFallsBackToUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"message":{"content":"{\"category\":\"BANANA\",\"confidence\":2.5,\"reasoning\":\"?\"}"}}`)
	}))
	defer srv.Close()

	c := NewOllamaClassifier(srv.URL, "", "m", quietLogger())
	got, err := c.Classify(context.Background(), domain.Token{Text: "?"})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if got.Category != domain.CategoryUnknown {
		t.Errorf("invalid category should fall back to UNKNOWN, got %s", got.Category)
	}
	if got.Confidence != 1.0 {
		t.Errorf("confidence should clamp to 1.0, got %v", got.Confidence)
	}
}

func TestOllamaClassifier_NonRetryableFailsFast(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := NewOllamaClassifier(srv.URL, "", "m", quietLogger(), WithMaxRetries(5))
	if _, err := c.Classify(context.Background(), domain.Token{Text: "x"}); err == nil {
		t.Fatal("expected error on 400")
	}
	if calls.Load() != 1 {
		t.Errorf("400 should not be retried; got %d calls", calls.Load())
	}
}
