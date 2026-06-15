# Document Processing Pipeline

A two-stage pipeline that **extracts** candidate entities from documents and
**classifies** each one into a business category (`COMPANY`, `PERSON`, `ADDRESS`,
`DATE`, `UNKNOWN`). Built as separately scalable services over PostgreSQL (state)
and Redis Streams (transport), with first-class support for reruns, progress, and
per-stage duration tracking.

```
Document ──► Extraction (NLP) ──► tokens ──► Classification (LLM) ──► classified tokens
              cmd/extractor                    cmd/classifier
                     │   Redis Streams (extract / classify)   │
                     └──────────────► PostgreSQL ◄────────────┘  (source of truth)
                                       cmd/api  (HTTP + reconciler)
```

> **Example:** `"John Smith works at Acme Corp, located at 123 Main St."` →
> `John Smith → PERSON`, `Acme Corp → COMPANY`, `123 Main St → ADDRESS`.

## Quick start

Prerequisites: **Docker** (with Compose) and, for development, **Go 1.22+**.

```bash
./start.sh            # builds and starts Postgres, Redis, api, extractor, classifier
# …then, in another shell:
./scripts/demo.sh     # runs all six scenarios end-to-end
```

`start.sh` waits until the API is healthy and prints example commands. No API keys
are required — classification uses a deterministic mock by default.

### The API (commands from the assignment work verbatim)

```bash
# Process a document
curl -X POST http://localhost:8080/process \
  -H "Content-Type: application/json" \
  -d '{"document_id": "doc-123", "text": "John Smith works at Acme Corp, located at 123 Main St."}'

# Check status (state, progress, durations)
curl http://localhost:8080/documents/doc-123/status

# Query tokens, filtered
curl "http://localhost:8080/documents/doc-123/tokens?classification=PERSON"
```

`POST /process` accepts an optional `"mode"`: `"partial"` (default — resume) or
`"full"` (reprocess from scratch). Token queries accept
`classification`, `type`, `status`, `page`, `limit`, `offset`.

## The six demo scenarios

`./scripts/demo.sh` demonstrates each requirement against the live stack:

| # | Scenario | What it shows |
|---|----------|---------------|
| 1 | Happy path | Process end-to-end, query results |
| 2 | Progress visibility | Live `classified / total` while processing |
| 3 | **Partial rerun** | Kill the classifier mid-run, restart, resume from the checkpoint |
| 4 | **Full rerun** | Reprocess with `mode=full`; `run_version` bumps, data replaced |
| 5 | Concurrent documents | Three documents processed at once |
| 6 | Query | Filter tokens by classification / type / page |

## Running tests

```bash
make test-unit        # fast, no Docker
make test             # unit + integration (testcontainers spins up Postgres + Redis)
```

Integration tests (`test/`) drive the real API over HTTP with real Postgres and
Redis containers, asserting all six scenarios plus store-level idempotency and
stale-write fencing. They auto-detect the Docker socket from your active context
(Docker Desktop, OrbStack, Colima, …).

## Configuration

Everything is environment-based (`internal/config`). Defaults run a key-free,
fully-mocked stack. Copy `.env.example` → `.env` to override.

| Variable | Default | Purpose |
|----------|---------|---------|
| `POSTGRES_DSN` | `postgres://docpipe:docpipe@localhost:5432/docpipe?sslmode=disable` | State store |
| `REDIS_ADDR` | `localhost:6379` | Broker |
| `HTTP_ADDR` | `:8080` | API listen address |
| `WORKER_CONCURRENCY` | `8` | Concurrent messages per worker process |
| `LLM_PROVIDER` | `mock` | `mock` or `ollama` |
| `MOCK_CLASSIFY_DELAY` | `0` (compose: `120ms`) | Simulated per-token latency, to make progress visible |
| `OLLAMA_BASE_URL` / `OLLAMA_API_KEY` / `OLLAMA_MODEL` | `https://ollama.com` / – / `gpt-oss:20b` | Real classifier (see below) |
| `RECONCILE_INTERVAL` / `CLAIM_MIN_IDLE` | `15s` / `30s` | Recovery timing |

### Using a real LLM (Ollama)

Set these (e.g. in `.env`) and restart — classification then calls a real model
through the same `Classifier` interface, with structured JSON output, retries,
and rate-limit handling:

```bash
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=https://ollama.com      # or http://host.docker.internal:11434 for local
OLLAMA_API_KEY=...                      # required for the hosted service
OLLAMA_MODEL=gpt-oss:20b
```

## Project structure

```
cmd/{api,extractor,classifier}   three service binaries (independent scaling)
internal/
  domain/      core types & contracts (Document, Token, states)
  config/      env-based configuration
  store/       PostgreSQL persistence + migrations (source of truth)
  queue/       Broker interface + Redis Streams implementation
  reconciler/  re-enqueues orphaned work (consistency backstop)
  nlp/         Extractor interface + rule-based mock
  llm/         Classifier interface + mock + Ollama
  pipeline/    worker loops & job contracts
  api/         HTTP handlers
test/          integration tests (testcontainers)
testdata/docs/ small / medium / large sample documents
docs/          architecture, ADRs, data model, trade-offs, failure analysis
prompts/       AI prompts used to build this (see AI proficiency below)
```

## Architecture & design docs

Start with **[docs/architecture.md](docs/architecture.md)** (overview + diagram +
contracts). Then:

- [Technology selection](docs/tech-selection.md)
- [Data model](docs/data-model.md)
- [Rerun & recovery](docs/rerun-and-recovery.md)
- [Duration tracking](docs/duration-tracking.md)
- [Trade-off analysis](docs/trade-offs.md)
- [Failure scenarios](docs/failure-scenarios.md)
- [Architecture Decision Records](docs/adr/)

## AI proficiency

This project was built with AI assistance, used deliberately and reviewed at every
step. The prompts and a description of how AI was used are committed under
[`prompts/`](prompts/).

## Make targets

`make help` lists them. Common: `make up`, `make down`, `make logs`, `make demo`,
`make test`, `make build`, `make fmt`, `make vet`, `make lint`.
