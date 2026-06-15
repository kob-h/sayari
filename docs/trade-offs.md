# Trade-off Analysis

For each major decision: the alternatives, why we chose what we did, and what we
gave up.

## 1. Redis Streams as the broker (vs. Postgres-as-queue, vs. Kafka/NATS)

- **Alternatives:** (a) Postgres `SELECT … FOR UPDATE SKIP LOCKED` as the queue —
  one dependency, transactional enqueue with state. (b) Kafka / NATS JetStream —
  high throughput, durable retention.
- **Chosen:** Redis Streams with consumer groups.
- **Why:** It delivers genuine broker semantics — consumer-group load balancing,
  per-message acks, and PEL-based crash redelivery — at near-zero operational cost
  (one container). That demonstrates the real production decoupling between stages
  better than coupling the queue to the database, while staying far lighter than
  Kafka for a POC.
- **Given up:** A second system to operate and reason about, and the resulting
  **at-least-once / two-source consistency** problem (Postgres + Redis can momentarily
  disagree). We pay for this with idempotent consumers and a reconciler. With
  Postgres-as-queue, enqueue and state change would be one ACID transaction and
  that class of problem disappears — at the cost of a less realistic architecture
  and DB queue contention at scale.

## 2. PostgreSQL for state (vs. a document/NoSQL store)

- **Alternatives:** MongoDB/DynamoDB (flexible schema, easy horizontal scale).
- **Chosen:** PostgreSQL.
- **Why:** The core invariant — *classify a token and advance its document's
  progress counter together, exactly once* — is a multi-row atomic update. Plus the
  required token queries are relational filters, and full rerun is a transactional
  delete-and-reset. Postgres does all three natively.
- **Given up:** Effortless horizontal write scaling. At very large scale we'd
  partition `tokens` by `document_id` and add read replicas; a NoSQL store would
  give that more cheaply but force application-side handling of the atomic
  counter and cross-document queries.

## 3. Progress as a denormalized counter (vs. `COUNT(*)` on demand)

- **Alternatives:** Compute `classified_count` with `SELECT COUNT(*) … WHERE
  status='CLASSIFIED'` at read time.
- **Chosen:** A counter column advanced in the classification transaction.
- **Why:** O(1) status reads, and the counter is updated atomically with the row
  it counts, so it cannot drift.
- **Given up:** A tiny bit of write cost and the theoretical risk of the counter
  and rows disagreeing — mitigated by doing both in one transaction (and the
  reconciler/`COUNT` could re-derive it if ever needed).

## 4. Separate binaries per stage (vs. one binary with role flags / one process)

- **Alternatives:** A single process running all stages; or one binary switched by
  a `--role` flag.
- **Chosen:** Three `cmd/` binaries sharing internal packages.
- **Why:** It makes "scale stages independently" literal — each is its own
  container scaled to its own replica count — and keeps each `main` tiny.
- **Given up:** Slightly more deployment surface (three services) than a single
  process. Mitigated by a single shared Docker image selected via `command`.

## 5. `run_version` fencing token (vs. timestamps / generation via deletes only)

- **Alternatives:** Rely solely on deleting old tokens during a full rerun and
  hope no old worker writes afterwards; or compare timestamps.
- **Chosen:** A monotonic integer stamped on writes and checked by the store.
- **Why:** It is an unambiguous, race-free fence: a stale worker's write is
  rejected deterministically, and queries scope to the current version so clients
  never see a transient mix.
- **Given up:** A column and a version check on the hot write path — negligible.

## 6. Mocked NLP/LLM by default (vs. requiring real services)

- **Alternatives:** Require a real LLM key to run anything.
- **Chosen:** Deterministic mocks by default; real Ollama available via config.
- **Why:** The evaluator can run everything with zero keys, tests are hermetic and
  reproducible, and the real-LLM concerns (prompting, retries, rate limits, cost)
  are still demonstrated behind the same interface.
- **Given up:** The mocks are rule-based and imperfect (some entity false
  positives). That is acceptable for exercising the *system*; entity quality is the
  swappable component's concern, not the pipeline's.
