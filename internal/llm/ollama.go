package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/kob-h/docpipeline/internal/domain"
)

// OllamaClassifier classifies tokens using an Ollama chat model (local or the
// hosted ollama.com endpoint). It uses Ollama's structured-output `format` field
// to force a JSON response that matches our schema, and retries transient
// failures (429 / 5xx / network) with exponential backoff.
//
// Cost/latency posture: temperature 0 for determinism, a tight token budget
// (num_predict), and a minimal prompt carrying only the token text, its NLP
// type, and a short context window — never the whole document.
type OllamaClassifier struct {
	http       *http.Client
	baseURL    string
	apiKey     string
	model      string
	maxRetries int
	log        *slog.Logger
}

// OllamaOption configures an OllamaClassifier.
type OllamaOption func(*OllamaClassifier)

// WithHTTPClient overrides the default HTTP client (used in tests).
func WithHTTPClient(c *http.Client) OllamaOption {
	return func(o *OllamaClassifier) { o.http = c }
}

// WithMaxRetries sets how many times a transient failure is retried.
func WithMaxRetries(n int) OllamaOption {
	return func(o *OllamaClassifier) { o.maxRetries = n }
}

// NewOllamaClassifier builds a classifier targeting baseURL with the given model.
// apiKey may be empty for a local Ollama; it is required for the hosted service.
func NewOllamaClassifier(baseURL, apiKey, model string, log *slog.Logger, opts ...OllamaOption) *OllamaClassifier {
	o := &OllamaClassifier{
		http:       &http.Client{Timeout: 30 * time.Second},
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		model:      model,
		maxRetries: 4,
		log:        log,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// responseSchema constrains the model output to our classification shape.
var responseSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "category": {"type": "string", "enum": ["COMPANY","PERSON","ADDRESS","DATE","UNKNOWN"]},
    "confidence": {"type": "number"},
    "reasoning": {"type": "string"}
  },
  "required": ["category","confidence","reasoning"]
}`)

type chatRequest struct {
	Model    string          `json:"model"`
	Messages []chatMessage   `json:"messages"`
	Stream   bool            `json:"stream"`
	Format   json.RawMessage `json:"format,omitempty"`
	Options  map[string]any  `json:"options,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Message chatMessage `json:"message"`
}

type classificationJSON struct {
	Category   string  `json:"category"`
	Confidence float64 `json:"confidence"`
	Reasoning  string  `json:"reasoning"`
}

const systemPrompt = `You are an entity classifier in a document-processing pipeline.
Classify the given token into exactly one category:
- COMPANY: a business, organization, or institution
- PERSON: an individual's name
- ADDRESS: a street address or physical location
- DATE: a date or time expression
- UNKNOWN: none of the above, or insufficient signal
Respond ONLY with the requested JSON. Confidence is a number between 0 and 1.`

// Classify implements Classifier.
func (o *OllamaClassifier) Classify(ctx context.Context, tok domain.Token) (domain.Classification, error) {
	user := fmt.Sprintf("Token: %q\nNLP entity type: %s\nClassify this token.", tok.Text, tok.NLPEntityType)
	reqBody := chatRequest{
		Model: o.model,
		Messages: []chatMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: user},
		},
		Stream: false,
		Format: responseSchema,
		Options: map[string]any{
			"temperature": 0,
			"num_predict": 200, // tight budget: the JSON answer is small
		},
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return domain.Classification{}, fmt.Errorf("marshal request: %w", err)
	}

	raw, err := o.doWithRetry(ctx, payload)
	if err != nil {
		return domain.Classification{}, err
	}

	var resp chatResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return domain.Classification{}, fmt.Errorf("decode chat response: %w", err)
	}
	var parsed classificationJSON
	if err := json.Unmarshal([]byte(resp.Message.Content), &parsed); err != nil {
		return domain.Classification{}, fmt.Errorf("decode classification JSON %q: %w", resp.Message.Content, err)
	}

	category := domain.Category(strings.ToUpper(strings.TrimSpace(parsed.Category)))
	if !category.Valid() {
		category = domain.CategoryUnknown
	}
	return domain.Classification{
		Category:   category,
		Confidence: math.Max(0, math.Min(1, parsed.Confidence)),
		Reasoning:  parsed.Reasoning,
	}, nil
}

// doWithRetry posts the chat request, retrying transient failures with
// exponential backoff. Non-retryable errors (4xx other than 429) fail fast.
func (o *OllamaClassifier) doWithRetry(ctx context.Context, payload []byte) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= o.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * 250 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		body, status, err := o.do(ctx, payload)
		switch {
		case err != nil:
			lastErr = err // network/timeout: retry
		case status == http.StatusOK:
			return body, nil
		case status == http.StatusTooManyRequests || status >= 500:
			lastErr = fmt.Errorf("ollama transient status %d: %s", status, truncate(body))
		default:
			return nil, fmt.Errorf("ollama status %d: %s", status, truncate(body)) // non-retryable
		}
		o.log.Warn("ollama call failed; will retry", "attempt", attempt, "err", lastErr)
	}
	return nil, fmt.Errorf("ollama exhausted retries: %w", lastErr)
}

func (o *OllamaClassifier) do(ctx context.Context, payload []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/api/chat", bytes.NewReader(payload))
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if o.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+o.apiKey)
	}
	resp, err := o.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("http do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read body: %w", err)
	}
	return body, resp.StatusCode, nil
}

func truncate(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}
