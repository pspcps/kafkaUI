-- =============================================================================
-- 007_schema_validation.sql  —  schema-based message validation
-- =============================================================================
--
-- Adds columns to topic_schemas to support typed schema formats with
-- enforced validation on produce.
--
-- schema_format values:
--   'manual'      — legacy: user-defined fields in schema_fields (no validation)
--   'none'        — schemaless / flexible (no validation)
--   'json_schema' — JSON Schema (draft 2020-12)
--   'avro'        — Apache Avro JSON schema
--   'protobuf'    — reserved for phase 2

ALTER TABLE topic_schemas
    ADD COLUMN IF NOT EXISTS schema_format  TEXT NOT NULL DEFAULT 'manual',
    ADD COLUMN IF NOT EXISTS schema_text    TEXT,
    ADD COLUMN IF NOT EXISTS schema_version INT  NOT NULL DEFAULT 1;

-- Backfill existing rows: anything with avro_raw was an Avro schema.
UPDATE topic_schemas
   SET schema_format = 'avro',
       schema_text   = avro_raw
 WHERE avro_raw IS NOT NULL AND avro_raw <> '' AND schema_format = 'manual';
