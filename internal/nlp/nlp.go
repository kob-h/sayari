// Package nlp defines the extraction contract and a deterministic, rule-based
// mock implementation. The interface is the important artifact: a real NLP
// service (spaCy, AWS Comprehend, a fine-tuned model) can be dropped in behind
// it without touching the pipeline.
package nlp

import (
	"context"

	"github.com/kob-h/docpipeline/internal/domain"
)

// Extractor scans a document and returns candidate entities with their type and
// position. Implementations must be safe for concurrent use.
type Extractor interface {
	// Extract returns the entities found in doc.Text. The returned slice may be
	// empty (a document with no entities is valid).
	Extract(ctx context.Context, doc domain.Document) ([]domain.Entity, error)
}
