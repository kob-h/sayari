package domain

import (
	"testing"
	"time"
)

func TestDocumentDurations(t *testing.T) {
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	extStart := base
	extEnd := base.Add(5 * time.Second)
	classStart := extEnd
	classEnd := classStart.Add(45 * time.Second)

	d := Document{
		ExtractionStartedAt:       &extStart,
		ExtractionCompletedAt:     &extEnd,
		ClassificationStartedAt:   &classStart,
		ClassificationCompletedAt: &classEnd,
	}
	ext, class := d.Durations()
	if ext != 5*time.Second {
		t.Errorf("extraction duration: got %v, want 5s", ext)
	}
	if class != 45*time.Second {
		t.Errorf("classification duration: got %v, want 45s", class)
	}
}

func TestDocumentDurations_Incomplete(t *testing.T) {
	start := time.Now()
	d := Document{ExtractionStartedAt: &start} // not completed
	ext, class := d.Durations()
	if ext != 0 || class != 0 {
		t.Errorf("incomplete stages should report zero duration, got ext=%v class=%v", ext, class)
	}
}

func TestCategoryValid(t *testing.T) {
	for _, c := range []Category{CategoryCompany, CategoryPerson, CategoryAddress, CategoryDate, CategoryUnknown} {
		if !c.Valid() {
			t.Errorf("%s should be valid", c)
		}
	}
	if Category("BANANA").Valid() {
		t.Error("BANANA should be invalid")
	}
}

func TestDocStatusValid(t *testing.T) {
	if !DocClassifying.Valid() {
		t.Error("CLASSIFYING should be valid")
	}
	if DocStatus("WAT").Valid() {
		t.Error("WAT should be invalid")
	}
}
