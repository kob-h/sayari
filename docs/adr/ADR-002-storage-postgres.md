# ADR-002: Use PostgreSQL as the system of record

## Status
Accepted

## Context
The system needs durable storage for document manifests and per-token state, with:
atomic progress updates, relational queries (tokens by classification/document/page),
a clean transactional reset for full reruns, and strong correctness under
concurrent workers.

## Decision
Use **PostgreSQL** as the single source of truth. Document and token state, progress
counters, and stage timestamps all live here. Correctness-critical operations run
in transactions with row locks (`SELECT … FOR UPDATE`).

## Alternatives Considered
- **MongoDB / DynamoDB.** Flexible schema and easy horizontal scaling, but the
  central invariant (advance a document counter *and* update a token together,
  exactly once) and cross-document relational queries push complexity into the
  application layer.
- **Blob storage + search index.** Good for large documents and full-text search,
  but weak for transactional state and counters.

## Consequences
- **Positive:** ACID multi-row transactions, mature relational querying and
  indexing, and a simple transactional full-rerun reset.
- **Negative:** Vertical-scaling ceiling on writes; horizontal scaling needs
  partitioning/replicas later.

## Trade-offs
Transactional correctness and query flexibility are prioritised over out-of-the-box
horizontal write scalability.
