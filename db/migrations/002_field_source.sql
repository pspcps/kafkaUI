-- 002_field_source.sql
-- Adds field_source to distinguish message value fields from header fields.

ALTER TABLE schema_fields
  ADD COLUMN IF NOT EXISTS field_source TEXT NOT NULL DEFAULT 'value';

-- 'value'  — extracted from the Kafka message body (JSON)
-- 'header' — extracted from Kafka message headers (key-value string map)
