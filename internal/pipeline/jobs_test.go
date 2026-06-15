package pipeline

import (
	"encoding/json"
	"testing"
)

func encodeForTest(v any) ([]byte, error) { return json.Marshal(v) }

func TestExtractJobRoundTrip(t *testing.T) {
	in := ExtractJob{DocumentID: "doc-1", RunVersion: 3}
	b, err := encodeForTest(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := decode[ExtractJob](b)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Errorf("round trip mismatch: got %+v, want %+v", out, in)
	}
}

func TestClassifyJobRoundTrip(t *testing.T) {
	in := ClassifyJob{TokenID: 42, DocumentID: "doc-2", RunVersion: 1}
	b, err := encodeForTest(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := decode[ClassifyJob](b)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Errorf("round trip mismatch: got %+v, want %+v", out, in)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := decode[ExtractJob]([]byte("not json")); err == nil {
		t.Error("expected decode error for invalid JSON")
	}
}
