// Package llm defines the classification contract and two implementations: a
// deterministic mock and a real Ollama-backed classifier. The pipeline depends
// only on the Classifier interface, so the backing model is a configuration
// choice, not a code change.
package llm

import (
	"context"

	"github.com/kob-h/docpipeline/internal/domain"
)

// Classifier maps an extracted token to a business category with a confidence
// and a short reasoning string. Implementations must be safe for concurrent use.
type Classifier interface {
	// Classify returns a classification for tok. The token carries its NLP type
	// and position, which implementations may use as context.
	Classify(ctx context.Context, tok domain.Token) (domain.Classification, error)
}
