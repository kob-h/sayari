package llm

import (
	"context"
	"hash/fnv"
	"regexp"
	"strings"
	"time"

	"github.com/kob-h/docpipeline/internal/domain"
)

// MockClassifier is a deterministic Classifier. It maps the NLP entity type to a
// business category, applies a few text heuristics, and derives a stable
// pseudo-confidence from the token text so output looks realistic and is
// reproducible across runs.
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

var (
	orgSuffix     = regexp.MustCompile(`(?i)\b(inc|corp|corporation|company|co|ltd|llc|plc|group|technologies|systems|holdings|bank|partners|capital|university|institute)\b`)
	addressPrefix = regexp.MustCompile(`^\d{1,5}\s`)
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

func classify(tok domain.Token) (domain.Category, string) {
	text := strings.TrimSpace(tok.Text)
	switch tok.NLPEntityType {
	case domain.EntityDate:
		return domain.CategoryDate, "NLP tagged a temporal expression"
	case domain.EntityOrg:
		return domain.CategoryCompany, "organization name with corporate form"
	case domain.EntityPerson:
		return domain.CategoryPerson, "capitalized personal-name pattern"
	case domain.EntityGPE:
		if addressPrefix.MatchString(text) {
			return domain.CategoryAddress, "leading street number indicates a postal address"
		}
		return domain.CategoryAddress, "geo-political / location reference"
	default:
		// MISC or unknown: fall back to light text heuristics.
		switch {
		case addressPrefix.MatchString(text):
			return domain.CategoryAddress, "leading street number indicates a postal address"
		case orgSuffix.MatchString(text):
			return domain.CategoryCompany, "contains a corporate suffix"
		default:
			return domain.CategoryUnknown, "no confident signal from NLP type or text"
		}
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
