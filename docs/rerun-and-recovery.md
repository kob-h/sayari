# Rerun & Recovery

**Source of truth for processing state:** PostgreSQL — `documents.status` and each
`tokens.status`. Redis holds only in-flight delivery state; it is derived,
disposable, and never authoritative.

## Partial rerun (crash recovery)

> *Extraction produced 100 tokens. After classifying 30, the system crashes. On
> restart it must continue from token 31 — without re-extracting or
> re-classifying completed tokens.*

Three mechanisms combine, all leaning on **idempotent, at-least-once** processing:

1. **Redis PEL redelivery.** A message a worker read but never `XACK`ed (because
   it crashed) stays in the consumer group's Pending Entries List. Another worker
   reclaims it with `XAUTOCLAIM` once it has been idle past `CLAIM_MIN_IDLE`.
2. **Idempotent persistence.** Classification writes the result **and** increments
   `classified_count` in one transaction. So:
   - A crash *before* that commit leaves the token `PENDING` → it is reprocessed.
   - A redelivery *after* the commit finds the token already `CLASSIFIED` → it is a
     no-op and the counter is **not** double-incremented.

   This is "at-least-once compute, exactly-once effect." The 30 already-classified
   tokens are skipped; only the remaining 70 run.
3. **Reconciler backstop.** A loop in the API process periodically asks Postgres
   for orphaned work — `PENDING`/`EXTRACTING` documents and `PENDING` tokens under
   a `CLASSIFYING` document — and re-publishes those jobs. This covers the one gap
   the PEL cannot: a crash *between* a Postgres write and the matching Redis
   `XADD` (e.g., extraction saved tokens but died before enqueuing all classify
   jobs). See [ADR-005](adr/ADR-005-consistency-model.md).

Extraction is itself idempotent: tokens are upserted on the natural key
`(document_id, run_version, sentence, char_offset, text)`, so a re-run extraction
converges to the same rows with no duplicates.

## Full rerun (reprocess from scratch)

> *The source text changed or the previous run was bad. Reprocess entirely,
> idempotent to previous runs, leaving no stale artifacts.*

`POST /process` with `{"mode":"full"}` performs, in **one transaction**
(`store.AcceptDocument` → `resetDocument`):

1. `DELETE` all tokens for the document.
2. Reset the manifest: `status='PENDING'`, counters to 0, all stage timestamps to
   `NULL`, and **`run_version = run_version + 1`**.
3. Update the stored text/`content_hash`.

Then a fresh `extract` job is published. Because the whole reset is one
transaction, there is never a moment where old and new tokens coexist.

### Fencing stale workers

A worker from the *previous* run might still be mid-flight when the full rerun
happens. Every write carries the `run_version` it was issued under, and the store
rejects any write whose version is behind the document's current version with
`ErrStaleWrite` (verified in `TestStore_StaleWriteRejected`). The late worker's
result is dropped, not applied. Queries also filter by current `run_version`, so
clients never observe stale tokens even briefly.

## Where the "source of truth" lives — summary

| Question | Answer |
|----------|--------|
| Is this document done? | `documents.status` |
| Is this token done? | `tokens.status` |
| How far along? | `documents.classified_count` / `total_tokens` |
| Which run is current? | `documents.run_version` |
| What work is outstanding? | Postgres rows in non-terminal states (the reconciler's query) — **not** the Redis backlog |
