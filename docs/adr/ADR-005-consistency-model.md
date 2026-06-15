# ADR-005: At-least-once delivery, idempotent consumers, and a reconciler backstop

## Status
Accepted

## Context
With Postgres as the source of truth and Redis Streams as a separate broker
([ADR-001](ADR-001-broker-redis-streams.md)), the two systems can momentarily
disagree. Redis Streams delivery is at-least-once (a message can be redelivered),
and there is a small window between a Postgres write and the matching Redis `XADD`
where a crash leaves durable state with no in-flight message to advance it.

## Decision
Adopt an **at-least-once + idempotent consumer + reconciler** model:

1. **At-least-once:** handlers ack (`XACK`) only after their Postgres work commits.
   Any error or crash leaves the message pending for redelivery.
2. **Idempotent consumers:** extraction upserts tokens on a natural key;
   classification no-ops if the token is already `CLASSIFIED` (so progress is never
   double-counted). Result + counter advance in one transaction → exactly-once
   *effect*.
3. **Reconciler:** a periodic loop scans Postgres for orphaned work (PENDING/
   EXTRACTING documents; PENDING tokens under a CLASSIFYING document) and
   re-publishes those jobs, closing the write-then-publish gap.

## Alternatives Considered
- **Transactional outbox** (write the message to Postgres in the same transaction,
  relay to Redis). Stronger exactly-once-ish delivery, but adds an outbox table and
  a relay process — more machinery than this POC needs.
- **Postgres-as-queue.** Removes the gap entirely (enqueue = state change), but was
  rejected in ADR-001 for architectural realism.
- **At-most-once.** Simpler, but loses work on crashes — unacceptable.

## Consequences
- **Positive:** No lost work and no double effects across crashes, broker outages,
  and reruns — proven by the partial-rerun and idempotency tests.
- **Negative:** Handlers *must* be idempotent (a permanent design constraint), and
  the reconciler does periodic redundant scans.

## Trade-offs
We accept idempotency as a design discipline and a lightweight reconciler in
exchange for keeping the realistic Redis broker without a heavier outbox.
