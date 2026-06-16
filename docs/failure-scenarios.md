# Failure Scenarios

How the design handles each required failure. The common thread:
**Postgres is the source of truth, delivery is at-least-once, and every consumer
is idempotent** — so the safe default on any failure is "don't ack; let it be
redelivered."

## 1. Classifier crashes after classifying a token but before persisting

Two sub-cases, both safe:

- **Crash before the Postgres commit.** Nothing was written. The token stays
  `PENDING` and the `classify` message was never `XACK`ed, so it remains in the
  consumer's PEL. A restarted/other classifier reclaims it via `XAUTOCLAIM` and
  classifies it. No loss.
- **Crash after the commit, before `XACK`.** The result is durably persisted. The
  message is redelivered, but the classification worker's `apply` sees the token is
  already `CLASSIFIED` and no-ops — crucially, **without** double-incrementing
  `classified_count`. No double count.

This is exactly what `TestPipeline_ClassificationIdempotentAndCompletes` and the
`TestIntegration_PartialRerun` test assert.

## 2. Extraction crashes mid-way (some tokens written, others not)

- Extraction's persist runs in **one transaction**: it upserts the tokens
  (idempotently, on the natural key), advances the document to `CLASSIFYING` with
  `total_tokens` set from the actual row count, **and** writes the `classify`
  outbox rows. Either all of that commits or none of it does.
- So there is no "tokens written but no classify messages" state: the tokens and
  their messages are in the same commit. A crash before commit leaves the document
  `EXTRACTING` and the un-`XACK`ed `extract` message redelivers; re-extraction
  converges to the same rows and commits once.
- After commit, the relay publishes the classify outbox rows. A crash before
  `XACK`ing the extract message just redelivers it; on redelivery the document is
  `CLASSIFYING` and extraction no-ops.

## 3. Database / storage temporarily unavailable

The two backing stores fail independently and both fail safe:

- **Postgres down.** Workers' handlers return an error → the message is **not**
  acked and stays pending. The API returns `500`/`503` (and `/healthz` reports
  unhealthy) so a load balancer routes away. On reconnect (the store retries with
  backoff at startup; the pool reconnects at runtime), pending messages are
  redelivered and processed. No work is lost or duplicated.
- **Redis down.** Workers cannot read or ack; they idle and retry with backoff
  (`Consume` backs off on transient `XREADGROUP` errors). The API can still accept
  documents — `Submit` commits the doc **and** its extract message to the Postgres
  outbox in one transaction. The relay's `XADD`s simply fail and retry, so the
  outbox backs up harmlessly and drains once Redis returns. Submission fully
  survives a broker outage with no lost messages.

## General principles applied

| Principle | Mechanism |
|-----------|-----------|
| Fail safe, not fast | Handlers return errors instead of acking on any doubt; messages redeliver. |
| Idempotency everywhere | Natural-key upserts (extraction); already-classified no-op (classification). |
| Exactly-once *effect* | Result + counter advance in one transaction. |
| No stale data after rerun | `run_version` fencing + current-version-scoped queries. |
| No write-then-publish gap | Transactional outbox: state change + message commit together; the relay publishes. |
| Graceful shutdown | `ctx` cancellation stops consumers cleanly; in-flight messages simply redeliver. |
