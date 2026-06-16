package llm

import (
	"context"
	"testing"

	"github.com/kob-h/docpipeline/internal/domain"
)

// The classifier decides the category from the token text alone (extraction
// provides no type).
func TestMockClassifier_Classifies(t *testing.T) {
	tests := []struct {
		name string
		text string
		want domain.Category
	}{
		{"company", "Acme Corporation", domain.CategoryCompany},
		{"company-inc", "Globex Inc", domain.CategoryCompany},
		{"person", "Jane Doe", domain.CategoryPerson},
		{"person-title", "Dr Helen Park", domain.CategoryPerson},
		{"date-month", "March 3, 2024", domain.CategoryDate},
		{"date-year", "2019", domain.CategoryDate},
		{"address", "123 Main Street", domain.CategoryAddress},
		{"address-ave", "450 Lakeshore Avenue", domain.CategoryAddress},
		{"unknown", "purple monkey", domain.CategoryUnknown},
	}
	c := NewMockClassifier()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Classify(context.Background(), domain.Token{Text: tt.text})
			if err != nil {
				t.Fatalf("Classify: %v", err)
			}
			if got.Category != tt.want {
				t.Errorf("text %q: got %s, want %s", tt.text, got.Category, tt.want)
			}
			if got.Confidence < 0 || got.Confidence > 1 {
				t.Errorf("confidence out of range: %v", got.Confidence)
			}
			if got.Reasoning == "" {
				t.Errorf("reasoning should not be empty")
			}
		})
	}
}

func TestMockClassifier_DeterministicConfidence(t *testing.T) {
	c := NewMockClassifier()
	tok := domain.Token{Text: "Acme Corporation"}
	a, _ := c.Classify(context.Background(), tok)
	b, _ := c.Classify(context.Background(), tok)
	if a != b {
		t.Errorf("classification not deterministic: %+v vs %+v", a, b)
	}
}
