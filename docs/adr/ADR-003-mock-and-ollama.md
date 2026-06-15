# ADR-003: Mock NLP/LLM by default, with a real Ollama classifier behind the interface

## Status
Accepted

## Context
The pipeline depends on NLP extraction and LLM classification, but the assignment
allows mocking them. The evaluator must be able to run everything with no API keys,
while we still want to demonstrate the concerns of a real LLM integration.

## Decision
Define two small interfaces — `nlp.Extractor` and `llm.Classifier` — and provide:
- a deterministic, rule-based **mock** for each (the default), and
- a real **Ollama** `Classifier` selected by `LLM_PROVIDER=ollama`.

The Ollama client uses structured JSON output (`format` schema), retries transient
failures (429/5xx/network) with exponential backoff, fails fast on non-retryable
4xx, and uses a tight token budget with temperature 0.

## Alternatives Considered
- **Require a real LLM to run.** Best signal on prompting/cost, but blocks any
  evaluator without keys and makes tests non-hermetic.
- **Mock only, no real path.** Simplest, but shows nothing about real-LLM error
  handling, rate limits, or cost control.

## Consequences
- **Positive:** Zero-key, deterministic default; hermetic tests; real-LLM concerns
  demonstrated and switchable by config alone; provider is swappable.
- **Negative:** The rule-based mocks are imperfect (occasional entity false
  positives). Acceptable: entity quality is the pluggable component's concern, not
  the pipeline's.

## Trade-offs
Evaluator-friendliness and test determinism are prioritised, without sacrificing a
credible real-LLM integration path.
