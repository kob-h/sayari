package llm

import (
	"context"
	"testing"

	"github.com/kob-h/docpipeline/internal/domain"
)

func TestMockClassifier_Mapping(t *testing.T) {
	tests := []struct {
		name string
		tok  domain.Token
		want domain.Category
	}{
		{"org->company", domain.Token{Text: "Acme Corporation", NLPEntityType: domain.EntityOrg}, domain.CategoryCompany},
		{"person", domain.Token{Text: "Jane Doe", NLPEntityType: domain.EntityPerson}, domain.CategoryPerson},
		{"date", domain.Token{Text: "March 3, 2024", NLPEntityType: domain.EntityDate}, domain.CategoryDate},
		{"gpe-address", domain.Token{Text: "123 Main Street", NLPEntityType: domain.EntityGPE}, domain.CategoryAddress},
		{"misc-address", domain.Token{Text: "450 Lakeshore Avenue", NLPEntityType: domain.EntityMisc}, domain.CategoryAddress},
		{"misc-org-suffix", domain.Token{Text: "Globex Inc", NLPEntityType: domain.EntityMisc}, domain.CategoryCompany},
		{"misc-unknown", domain.Token{Text: "purple monkey", NLPEntityType: domain.EntityMisc}, domain.CategoryUnknown},
	}
	c := NewMockClassifier()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Classify(context.Background(), tt.tok)
			if err != nil {
				t.Fatalf("Classify: %v", err)
			}
			if got.Category != tt.want {
				t.Errorf("category: got %s, want %s", got.Category, tt.want)
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
	tok := domain.Token{Text: "Acme Corporation", NLPEntityType: domain.EntityOrg}
	a, _ := c.Classify(context.Background(), tok)
	b, _ := c.Classify(context.Background(), tok)
	if a != b {
		t.Errorf("classification not deterministic: %+v vs %+v", a, b)
	}
}
