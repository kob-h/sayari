# ADR-001: Use Redis Streams as the inter-stage message broker

## Status
Accepted

## Context
Pipeline stages (extraction, classification) must scale and fail independently.
That requires a transport that load-balances work across many workers per stage,
acknowledges work, and redelivers work abandoned by a crashed worker. It must also
be trivial to run locally.

## Decision
Use **Redis Streams** with consumer groups as the broker between stages.
`XADD` publishes; `XREADGROUP` consumes within a group; `XACK` acknowledges;
`XAUTOCLAIM` reclaims idle pending messages after a crash. The broker sits behind
a small `Queue.Broker` interface (`internal/queue`).

## Alternatives Considered
- **Kafka / NATS JetStream.** Higher throughput and longer retention, but heavier
  to operate and overkill for a local POC.
- **Synchronous gRPC/REST between stages.** Couples stages, makes back-pressure
  and crash recovery the caller's problem, and blocks independent scaling.

## Consequences
- **Positive:** Genuine consumer-group scaling, acks, and PEL crash redelivery at
  one-container cost; a clean swap point for Kafka/NATS later.
- **Negative:** A second stateful system and **at-least-once** delivery, creating a
  consistency gap with Postgres (addressed in [ADR-005](ADR-005-consistency-model.md)).

## Trade-offs
Production realism and operational simplicity are prioritised; the resulting
two-source consistency gap is handled by a transactional outbox
([ADR-005](ADR-005-consistency-model.md)).
