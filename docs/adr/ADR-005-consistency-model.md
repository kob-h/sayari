# ADR-005: Transactional outbox + idempotent consumers for Postgres↔Redis consistency

## Status
Accepted

## Context
With Postgres as the source of truth and Redis Streams as a separate broker
([ADR-001](ADR-001-broker-redis-streams.md)), the two systems can disagree in the
window between a state change committing and the matching message being published.
If a producer crashes there, durable state exists with no in-flight message to
advance it. Redis Streams delivery is also at-least-once (messages can be
redelivered). We need publication to be gap-free without making a backlog worse.

## Decision
Use the **transactional outbox** pattern plus **idempotent consumers**:

1. **Outbox:** producers write the message to publish into an `outbox` table in the
   *same transaction* as their state change (`store.Tx.EnqueueOutbox`). State and
   "intent to publish" therefore commit atomically — no write-then-publish gap.
   - `service.Submit` writes the document and its extract message together.
   - The extraction worker writes the tokens and one classify message per token
     together.
2. **Relay:** a single component (`internal/outbox`) drains unpublished rows with
   `FOR UPDATE SKIP LOCKED`, `XADD`s each to Redis, and deletes it. It publishes on
   the signal *"not yet published,"* never *"not yet processed,"* so it never
   re-sends in-flight work and needs no interval tuned against backlog.
3. **Idempotent consumers:** extraction upserts on a natural key; classification
   no-ops if the token is already `CLASSIFIED` (no double count). Result + counter
   advance in one transaction → exactly-once *effect*. The broker's PEL
   (`XAUTOCLAIM`) handles consumer crashes.

## Alternatives Considered
- **Direct publish after commit (no outbox).** Commit the state change, then
  `XADD`. Simple, but a crash between the commit and the publish loses the message
  — exactly the write-then-publish gap this ADR closes.
- **Change Data Capture** (e.g. Debezium tailing the WAL). Removes even the relay's
  polling, but adds significant infrastructure — overkill for this POC.

## Consequences
- **Positive:** no lost work and no double effects across producer crashes, broker
  outages, and reruns; the relay never amplifies a backlog; delivery is
  at-least-once with effectively-once results.
- **Negative:** an extra table and one insert per message (write amplification);
  the relay still polls (latency ≈ poll interval) and remains an at-least-once
  publisher, so idempotent consumers stay mandatory.

## Trade-offs
We accept a small amount of write amplification and a polling relay in exchange for
gap-free publication that never re-publishes in-flight work, so it cannot amplify a
backlog.
