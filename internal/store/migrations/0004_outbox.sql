-- Transactional outbox: producers insert the message to publish in the SAME
-- transaction that changes state, so state and "intent to publish" commit
-- atomically. A relay drains these rows to the broker and deletes them, giving
-- at-least-once publication with no write-then-publish gap.
CREATE TABLE IF NOT EXISTS outbox (
    id         BIGSERIAL   PRIMARY KEY,
    stream     TEXT        NOT NULL,
    msg_key    TEXT        NOT NULL DEFAULT '',
    payload    BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
