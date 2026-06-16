# Technology Selection

| Component | Options considered | Choice | Justification |
|-----------|--------------------|--------|---------------|
| **Inter-component communication** | Dedicated broker (Redis Streams, Kafka, NATS, RabbitMQ); synchronous gRPC/REST | **Redis Streams** (consumer groups) | Gives real broker semantics — consumer groups for load-balanced scaling, per-message acks, and a Pending Entries List for crash redelivery — while running as a single trivial container. Decouples stages so they scale and fail independently. Hidden behind a `Queue` interface so a higher-throughput broker (Kafka/NATS) can replace it without touching pipeline code. See [ADR-001](adr/ADR-001-broker-redis-streams.md). |
| **Data storage** | SQL (Postgres/MySQL); NoSQL (Mongo/Dynamo); key-value; blob + index | **PostgreSQL** | The workload needs *atomic* state transitions (classify a token **and** advance its document's progress counter in one step), relational filtering (tokens by classification/document/page), and a transactional full-rerun reset. These are exactly relational strengths. JSON columns remain available for flexible metadata. See [ADR-002](adr/ADR-002-storage-postgres.md). |
| **Local development** | Cloud emulators; in-memory mocks; containers | **Docker Compose** (Postgres + Redis + the three services) | One command (`./start.sh`) brings up the entire system with real (not mocked) infrastructure, so local behaviour matches production semantics. The NLP/LLM dependencies are mocked in-process, so **no API keys are required** to run or evaluate. Integration tests use the same images via testcontainers. |
| **Language / runtime** | Go, Python, TypeScript | **Go 1.22+** | First-class concurrency (the worker model is goroutines + a consumer group), static binaries that make tiny container images, and strong typing for the data contracts. |
| **NLP (extraction)** | spaCy / cloud NLP; rule-based stub | **Rule-based mock** behind the `Extractor` interface | The assignment allows mocking. A deterministic regex-based stub returns realistic entities and positions, makes the pipeline reproducible, and keeps tests hermetic. A real extractor implements the same interface. |
| **LLM (classification)** | Real LLM (Ollama/Claude/OpenAI); rule-based stub | **Mock by default; real Ollama optional** (`LLM_PROVIDER=ollama`) | Default mock needs no keys and is deterministic. The Ollama implementation shows production concerns — structured JSON output, retries with backoff, rate-limit handling, a tight token budget — selectable purely by configuration. See [ADR-003](adr/ADR-003-mock-and-ollama.md). |

## Production evolution

The choices above are tuned for a correct, runnable POC. At larger scale:

- **Broker:** Redis Streams → **Kafka or NATS JetStream** for higher throughput, longer retention, and partition-level ordering. The `Queue` interface is the swap point.
- **Storage:** introduce read replicas and partition `tokens` by `document_id`; move document text to blob storage with a hash reference if documents grow large.
