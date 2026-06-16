-- Adds the token context column for databases created before it was part of the
-- initial schema. Idempotent and a no-op on fresh databases (0001 already
-- includes the column).
ALTER TABLE tokens ADD COLUMN IF NOT EXISTS context TEXT NOT NULL DEFAULT '';
