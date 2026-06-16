-- Extraction returns untyped tokens (per the assignment's example flow); the
-- classifier alone assigns a category. Drop the NLP entity type for databases
-- created before this change. Idempotent and a no-op on fresh databases.
ALTER TABLE tokens DROP COLUMN IF EXISTS nlp_entity_type;
