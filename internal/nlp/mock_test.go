package nlp

import (
	"context"
	"testing"

	"github.com/kob-h/docpipeline/internal/domain"
)

func extract(t *testing.T, text string) []domain.Entity {
	t.Helper()
	ents, err := NewMockExtractor().Extract(context.Background(), domain.Document{Text: text})
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	return ents
}

func TestMockExtractor_Types(t *testing.T) {
	tests := []struct {
		name string
		text string
		want domain.EntityType
		find string
	}{
		{"person", "Jane Doe joined the firm.", domain.EntityPerson, "Jane Doe"},
		{"org", "He works at Acme Corporation today.", domain.EntityOrg, "Acme Corporation"},
		{"org_llc", "The deal involved Globex LLC.", domain.EntityOrg, "Globex LLC"},
		{"address", "Visit us at 123 Main Street.", domain.EntityGPE, "123 Main Street"},
		{"date_month", "It happened on March 3, 2024.", domain.EntityDate, "March 3, 2024"},
		{"date_year", "The company grew in 2019.", domain.EntityDate, "2019"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ents := extract(t, tt.text)
			var found bool
			for _, e := range ents {
				if e.Text == tt.find {
					found = true
					if e.Type != tt.want {
						t.Errorf("entity %q: got type %s, want %s", tt.find, e.Type, tt.want)
					}
				}
			}
			if !found {
				t.Errorf("expected to find entity %q in %+v", tt.find, ents)
			}
		})
	}
}

func TestMockExtractor_SkipsJobTitles(t *testing.T) {
	ents := extract(t, "Jane Doe will join as Chief Executive Officer.")
	for _, e := range ents {
		if e.Text == "Chief Executive Officer" {
			t.Errorf("job title was wrongly extracted as an entity: %+v", e)
		}
	}
}

func TestMockExtractor_Positions(t *testing.T) {
	// Two sentences; the entity in the second sentence must have sentence index 1
	// and a char offset past the first sentence.
	ents := extract(t, "Acme Corporation grew. Jane Doe arrived later.")
	var jane *domain.Entity
	for i := range ents {
		if ents[i].Text == "Jane Doe" {
			jane = &ents[i]
		}
	}
	if jane == nil {
		t.Fatalf("Jane Doe not found in %+v", ents)
	}
	if jane.Position.Sentence != 1 {
		t.Errorf("sentence index: got %d, want 1", jane.Position.Sentence)
	}
	if jane.Position.CharOffset == 0 {
		t.Errorf("char offset should be > 0 for a second-sentence entity")
	}
}

func TestMockExtractor_Deterministic(t *testing.T) {
	const text = "On March 3, 2024, Jane Doe joined Acme Corporation at 123 Main Street."
	first := extract(t, text)
	second := extract(t, text)
	if len(first) != len(second) {
		t.Fatalf("non-deterministic count: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Errorf("non-deterministic entity at %d: %+v vs %+v", i, first[i], second[i])
		}
	}
}

func TestMockExtractor_EmptyDocument(t *testing.T) {
	if ents := extract(t, ""); len(ents) != 0 {
		t.Errorf("empty doc should yield no entities, got %+v", ents)
	}
}
