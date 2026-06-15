# Duration Tracking

The goal: report, per document, "extraction took 5s, classification took 45s."

## Timestamps recorded

Four nullable `TIMESTAMPTZ` columns on `documents`, set by Postgres `now()` at the
exact moment of each stage boundary so timing reflects real processing, not API
round-trips.

| Timestamp | Set when | Set by |
|-----------|----------|--------|
| `extraction_started_at` | Document transitions `PENDING → EXTRACTING` (a worker claims it) | `store.BeginExtraction` |
| `extraction_completed_at` | All tokens persisted; `EXTRACTING → CLASSIFYING` | `store.SaveExtraction` |
| `classification_started_at` | Same instant extraction completes (classification work becomes available) | `store.SaveExtraction` |
| `classification_completed_at` | Last token classified; `CLASSIFYING → COMPLETED` | `store.ApplyClassification` |

`classification_started_at` is set at the moment extraction completes rather than
when the first token is picked up. This is deliberate: it captures the full
elapsed time classification work was outstanding (including any queue wait),
which is the more honest "how long did classification take" for a document.

## Computing durations

```
extraction_duration     = extraction_completed_at     − extraction_started_at
classification_duration = classification_completed_at  − classification_started_at
```

This is done in code: `domain.Document.Durations()` returns the two
`time.Duration`s (zero if a stage has not completed). The status endpoint exposes
them as `extraction_seconds` and `classification_seconds`.

## Why this is correct under concurrency and reruns

- Stage boundaries are set by the **single transaction** that performs the
  transition, so timestamps line up exactly with state changes.
- `classification_completed_at` is stamped by the same atomic update that flips the
  document to `COMPLETED` when `classified_count` reaches `total_tokens` — so it
  marks true completion regardless of which of the N concurrent classifier workers
  finished last.
- A full rerun resets all four timestamps to `NULL`, so a re-processed document
  reports the new run's durations, never a mix.

## Example

From a live run of the medium test document (see the demo output):

```json
"durations": { "extraction_seconds": 0.02, "classification_seconds": 2.17 }
```

Extraction is near-instant (in-process mock NLP); classification dominates,
reflecting the per-token LLM latency — exactly the shape the requirement describes.
