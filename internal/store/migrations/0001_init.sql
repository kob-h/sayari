-- Schema for the document-processing pipeline.
--
-- Postgres is the single source of truth for processing state and tokens.
-- Redis only ever holds in-flight delivery state; everything durable lives here.

CREATE TABLE IF NOT EXISTS documents (
    id                            TEXT PRIMARY KEY,
    text                          TEXT        NOT NULL,
    content_hash                  TEXT        NOT NULL,
    status                        TEXT        NOT NULL DEFAULT 'PENDING',
    -- Fencing token: bumped on every full rerun. Workers stamp this onto their
    -- writes; stale writes (lower version) are rejected so a superseded run
    -- cannot corrupt fresh data.
    run_version                   INTEGER     NOT NULL DEFAULT 1,
    total_tokens                  INTEGER     NOT NULL DEFAULT 0,
    -- Progress counter, incremented atomically as each token is classified.
    classified_count              INTEGER     NOT NULL DEFAULT 0,

    extraction_started_at         TIMESTAMPTZ,
    extraction_completed_at       TIMESTAMPTZ,
    classification_started_at     TIMESTAMPTZ,
    classification_completed_at   TIMESTAMPTZ,

    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT documents_status_chk
        CHECK (status IN ('PENDING','EXTRACTING','CLASSIFYING','COMPLETED','FAILED'))
);

CREATE TABLE IF NOT EXISTS tokens (
    id              BIGSERIAL PRIMARY KEY,
    document_id     TEXT        NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    run_version     INTEGER     NOT NULL,
    text            TEXT        NOT NULL,
    nlp_entity_type TEXT        NOT NULL,
    page            INTEGER     NOT NULL DEFAULT 1,
    sentence        INTEGER     NOT NULL DEFAULT 0,
    char_offset     INTEGER     NOT NULL DEFAULT 0,

    status          TEXT        NOT NULL DEFAULT 'PENDING',
    classification  TEXT,
    confidence      REAL,
    reasoning       TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    classified_at   TIMESTAMPTZ,

    CONSTRAINT tokens_status_chk CHECK (status IN ('PENDING','CLASSIFIED')),

    -- Idempotent extraction: re-running extraction for the same run upserts onto
    -- this key instead of creating duplicates, so a crashed-and-retried
    -- extraction converges to the same set of tokens.
    CONSTRAINT tokens_natural_key
        UNIQUE (document_id, run_version, sentence, char_offset, text)
);

-- Queue-claim / progress lookups: "give me this document's pending tokens".
CREATE INDEX IF NOT EXISTS tokens_doc_status_idx
    ON tokens (document_id, run_version, status);

-- Token queries filtered by business classification.
CREATE INDEX IF NOT EXISTS tokens_doc_classification_idx
    ON tokens (document_id, run_version, classification);
