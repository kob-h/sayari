package llm

import (
	"context"
	"hash/fnv"
	"regexp"
	"strings"
	"time"

	"github.com/kob-h/docpipeline/internal/domain"
)

// MockClassifier is a deterministic Classifier. Given only the token text and
// its context (extraction does not provide a type), it applies ordered
// rule-based heuristics to decide the business category and derives a stable
// pseudo-confidence so output looks realistic and is reproducible across runs.
type MockClassifier struct {
	// delay optionally simulates per-token LLM latency so progress and
	// partial-rerun behaviour are observable. Zero means no delay.
	delay time.Duration
}

// MockOption configures a MockClassifier.
type MockOption func(*MockClassifier)

// WithDelay sets simulated per-token latency.
func WithDelay(d time.Duration) MockOption {
	return func(m *MockClassifier) { m.delay = d }
}

// NewMockClassifier returns a ready MockClassifier.
func NewMockClassifier(opts ...MockOption) *MockClassifier {
	m := &MockClassifier{}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Classification rules, evaluated in order against the token text. These live in
// the classifier (not the extractor) because labeling is the classifier's job.
var (
	dateRe        = regexp.MustCompile(`(?i)^(?:(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}|(?:19|20)\d{2}|\d{1,2}/\d{1,2}/\d{2,4})`)
	addressPrefix = regexp.MustCompile(`^\d{1,5}\s+[A-Za-z]`)
	orgSuffix     = regexp.MustCompile(`(?i)\b(inc|incorporated|corp|corporation|company|co|ltd|llc|plc|group|technologies|systems|solutions|holdings|industries|bank|partners|capital|ventures|university|institute|association|foundation|agency)\b`)
	personRe      = regexp.MustCompile(`^(?:(?:Mr|Mrs|Ms|Dr|Prof|President|CEO|CFO|CTO|Senator|Governor|Mayor|Chairman)\.?\s+)?[A-Z][a-z]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][a-z]+){1,2}$`)
)

// Classify implements Classifier.
func (m MockClassifier) Classify(ctx context.Context, tok domain.Token) (domain.Classification, error) {
	if m.delay > 0 {
		select {
		case <-ctx.Done():
			return domain.Classification{}, ctx.Err()
		case <-time.After(m.delay):
		}
	}
	category, reason := classify(tok)
	return domain.Classification{
		Category:   category,
		Confidence: confidence(tok.Text, category),
		Reasoning:  reason,
	}, nil
}

// classify decides the business category from the token text alone (the context
// is available on tok and would be used by a real model; the deterministic stub
// keys off the surface form). Rules are ordered most-specific first.
func classify(tok domain.Token) (domain.Category, string) {
	text := strings.TrimSpace(tok.Text)
	switch {
	case dateRe.MatchString(text):
		return domain.CategoryDate, "matches a date/temporal pattern"
	case addressPrefix.MatchString(text):
		return domain.CategoryAddress, "leading street number indicates a postal address"
	case orgSuffix.MatchString(text):
		return domain.CategoryCompany, "contains a corporate suffix"
	case personRe.MatchString(text):
		return domain.CategoryPerson, "capitalized personal-name pattern"
	default:
		return domain.CategoryUnknown, "no confident signal from the token text"
	}
}

// confidence derives a stable value in [0.80, 0.99] from the token text, except
// for UNKNOWN which is reported lower to look realistic.
func confidence(text string, c domain.Category) float64 {
	if c == domain.CategoryUnknown {
		return 0.40
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(text))
	return 0.80 + float64(h.Sum32()%20)/100.0
}
