# Failure Scenarios

How the design handles each required failure. The common thread:
**Postgres is the source of truth, delivery is at-least-once, and every consumer
is idempotent** — so the safe default on any failure is "don't ack; let it be
redelivered."

## 1. Classifier crashes after classifying a token but before persisting

Two sub-cases, both safe:

- **Crash before the Postgres commit.** Nothing was written. The token stays
  `PENDING` and the `classify` message was never `XACK`ed, so it remains in the
  consumer's PEL. A restarted/other classifier reclaims it via `XAUTOCLAIM` (or the
  reconciler re-enqueues it) and classifies it. No loss.
- **Crash after the commit, before `XACK`.** The result is durably persisted. The
  message is redelivered, but `ApplyClassification` sees the token is already
  `CLASSIFIED` and no-ops — crucially, **without** double-incrementing
  `classified_count`. No double count.

This is exactly what `TestStore_ClassificationIsIdempotent` and the
`TestIntegration_PartialRerun` test assert.

## 2. Extraction crashes mid-way (some tokens written, others not)

- Token writes are idempotent upserts on the natural key, so the partial set is
  consistent (no duplicates on retry).
- The document is still `EXTRACTING` (the transition to `CLASSIFYING` only happens
  in the *final* `SaveExtraction` transaction, which also sets `total_tokens` from
  the actual row count). So a half-finished extraction never looks complete.
- The `extract` message was not `XACK`ed → it is redelivered, and re-extraction
  converges to the full token set, then advances to `CLASSIFYING`.
- If extraction *committed* its tokens but crashed before enqueuing every
  `classify` job, the **reconciler** finds the `PENDING` tokens under the
  `CLASSIFYING` document and enqueues them. (`TestStore_FindOrphans` covers the
  detection.)

## 3. Database / storage temporarily unavailable

The two backing stores fail independently and both fail safe:

- **Postgres down.** Workers' handlers return an error → the message is **not**
  acked and stays pending. The API returns `500`/`503` (and `/healthz` reports
  unhealthy) so a load balancer routes away. On reconnect (the store retries with
  backoff at startup; the pool reconnects at runtime), pending messages are
  redelivered and processed. No work is lost or duplicated.
- **Redis down.** Workers cannot read or ack; they idle and retry with backoff
  (`Consume` backs off on transient `XREADGROUP` errors). The API can still accept
  documents — `AcceptDocument` commits to Postgres — and if the subsequent `XADD`
  fails, the document is left `PENDING` and the reconciler enqueues it once Redis
  returns. So submission survives a broker outage.

## General principles applied

| Principle | Mechanism |
|-----------|-----------|
| Fail safe, not fast | Handlers return errors instead of acking on any doubt; messages redeliver. |
| Idempotency everywhere | Natural-key upserts (extraction); already-classified no-op (classification). |
| Exactly-once *effect* | Result + counter advance in one transaction. |
| No stale data after rerun | `run_version` fencing + current-version-scoped queries. |
| Recover the un-recoverable gap | Reconciler re-enqueues orphaned Postgres state. |
| Graceful shutdown | `ctx` cancellation stops consumers cleanly; in-flight messages simply redeliver. |
