# Data Model

Two tables in PostgreSQL. The canonical schema is
[`internal/store/migrations/0001_init.sql`](../internal/store/migrations/0001_init.sql);
the typed Go definitions are in [`internal/domain/domain.go`](../internal/domain/domain.go).

## `documents` — the manifest (processing state, progress, timing)

| Column | Type | Notes |
|--------|------|-------|
| `id` | `TEXT` PK | Caller-supplied document id. |
| `text` | `TEXT` | Source text. |
| `content_hash` | `TEXT` | SHA-256 of the text; detects source changes across runs. |
| `status` | `TEXT` | `PENDING → EXTRACTING → CLASSIFYING → COMPLETED` (+ `FAILED`). |
| `run_version` | `INT` | Fencing token; bumped on every full rerun. |
| `total_tokens` | `INT` | Set when extraction completes. |
| `classified_count` | `INT` | Incremented atomically per classified token. |
| `extraction_started_at` / `extraction_completed_at` | `TIMESTAMPTZ?` | Extraction stage bounds. |
| `classification_started_at` / `classification_completed_at` | `TIMESTAMPTZ?` | Classification stage bounds. |
| `created_at` / `updated_at` | `TIMESTAMPTZ` | Audit timestamps. |

## `tokens` — the extracted & classified entities (per-token storage)

| Column | Type | Notes |
|--------|------|-------|
| `id` | `BIGSERIAL` PK | |
| `document_id` | `TEXT` FK → `documents(id)` `ON DELETE CASCADE` | |
| `run_version` | `INT` | The run this token belongs to. |
| `text` | `TEXT` | The extracted snippet (untyped — extraction does not label it). |
| `context` | `TEXT` | The entity's sentence, captured at extraction, so classification receives "entity text + context". |
| `page` / `sentence` / `char_offset` | `INT` | Position in the document (offsets are rune-based). |
| `status` | `TEXT` | `PENDING → CLASSIFIED`. |
| `classification` | `TEXT?` | From classification: `COMPANY`/`PERSON`/`ADDRESS`/`DATE`/`UNKNOWN`. |
| `confidence` | `REAL?` | Classifier confidence. |
| `reasoning` | `TEXT?` | Short rationale. |
| `created_at` / `classified_at` | `TIMESTAMPTZ?` | |

### Constraints & indexes

```sql
UNIQUE (document_id, run_version, sentence, char_offset, text)   -- idempotent extraction
INDEX  (document_id, run_version, status)                        -- progress / pending lookups
INDEX  (document_id, run_version, classification)                -- token queries by category
```

## How the model satisfies the requirements

**Query tokens by classification, document, or page.** All are indexed columns on
`tokens`; the API (`ListTokens`) builds a parameterised `WHERE` and always scopes
to the document's *current* `run_version`, so stale-run tokens are never returned.

**Track progress (“150 of 500 classified”).** `documents.classified_count` and
`documents.total_tokens` give an O(1) read. The counter is advanced with an
atomic `UPDATE … SET classified_count = classified_count + 1` in the same
transaction that marks the token `CLASSIFIED`, so the count can never drift from
the number of classified rows.

**Concurrent updates without conflicts.**
- Token claiming is done by the broker (Redis consumer group), not by polling the
  DB, so workers never contend for "the next token".
- Each classification is a single-row token update plus a single-row counter
  increment, both inside one transaction with `SELECT … FOR UPDATE` on the rows
  involved — serialising any two workers that somehow target the same token.
- `run_version` fences writes from superseded runs (see
  [Rerun & Recovery](rerun-and-recovery.md)).
