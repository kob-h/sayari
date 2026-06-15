# ADR-004: Use a `run_version` fencing token for full reruns

## Status
Accepted

## Context
A full rerun reprocesses a document from scratch. A worker from the *previous* run
may still be in flight when the rerun starts; its late write must not corrupt the
fresh run's data, and clients must never see a mix of old and new tokens.

## Decision
Give each document a monotonically increasing **`run_version`**, bumped in the same
transaction that deletes old tokens and resets the manifest. Every broker message
and every worker write carries the version it was issued under. The store rejects
any write whose version is behind the document's current version
(`domain.ErrStaleWrite`). All token queries are scoped to the current version.

## Alternatives Considered
- **Delete-and-recreate only.** Relies on timing luck — a slow old-run worker can
  still write after the delete, resurrecting stale data.
- **Timestamp comparison.** Works but is fuzzier than a monotonic counter and
  sensitive to clock skew.
- **New document id per run.** Breaks the stable external id contract and the
  "status of doc-123" query.

## Consequences
- **Positive:** Deterministic, race-free fencing; clients never observe a transient
  mix; full rerun is clean and idempotent to prior runs.
- **Negative:** One extra column and a version check on the write path
  (negligible).

## Trade-offs
A small amount of bookkeeping buys unambiguous correctness across overlapping runs.
