-- =============================================================================
-- 001_init.sql  —  kafbat management database schema
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---------------------------------------------------------------------------
-- topic_schemas
-- Stores the user-defined schema for each (cluster, topic) pair.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS topic_schemas (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id    TEXT        NOT NULL,
    topic_name    TEXT        NOT NULL,
    schema_type   TEXT        NOT NULL DEFAULT 'manual',  -- 'manual' | 'avro'
    avro_raw      TEXT,                                   -- raw Avro JSON string (optional)
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cluster_id, topic_name)
);

-- ---------------------------------------------------------------------------
-- schema_fields
-- One row per field in a topic schema.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_fields (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_id     UUID        NOT NULL REFERENCES topic_schemas(id) ON DELETE CASCADE,
    name          TEXT        NOT NULL,
    field_type    TEXT        NOT NULL,  -- string | int | long | float | double | boolean | timestamp | bytes | json
    is_required   BOOLEAN     NOT NULL DEFAULT FALSE,
    is_primary_key BOOLEAN    NOT NULL DEFAULT FALSE,
    default_value TEXT,
    description   TEXT,
    position      INT         NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_schema_fields_schema_id ON schema_fields(schema_id);

-- ---------------------------------------------------------------------------
-- replication_jobs
-- One row per user-created replication job.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS replication_jobs (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name                  TEXT        NOT NULL,
    cluster_id            TEXT        NOT NULL,
    topic_name            TEXT        NOT NULL,
    consumer_group        TEXT        NOT NULL UNIQUE,
    service_name          TEXT        NOT NULL,

    -- destination
    destination_type      TEXT        NOT NULL,   -- postgresql | mysql | sqlserver | mongodb
    destination_config    JSONB       NOT NULL,   -- {host, port, database, username, password, table/collection, ...}

    -- start options
    start_position        TEXT        NOT NULL DEFAULT 'latest',  -- 'latest' | 'earliest'
    include_historical    BOOLEAN     NOT NULL DEFAULT FALSE,

    -- consumer scaling
    consumer_count_mode   TEXT        NOT NULL DEFAULT 'fixed',   -- fixed | per_partition | custom
    consumer_count        INT         NOT NULL DEFAULT 1,

    -- k8s state
    status                TEXT        NOT NULL DEFAULT 'pending', -- pending | running | paused | failed | stopped
    k8s_deployment_name   TEXT,
    k8s_namespace         TEXT        NOT NULL DEFAULT 'kafbat',

    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_cluster_topic ON replication_jobs(cluster_id, topic_name);
CREATE INDEX IF NOT EXISTS idx_jobs_status        ON replication_jobs(status);

-- ---------------------------------------------------------------------------
-- consumer_metrics
-- Updated by worker pods — one row per (job, partition).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consumer_metrics (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id            UUID        NOT NULL REFERENCES replication_jobs(id) ON DELETE CASCADE,
    partition_id      INT         NOT NULL,
    current_offset    BIGINT      NOT NULL DEFAULT 0,
    end_offset        BIGINT      NOT NULL DEFAULT 0,
    lag               BIGINT      GENERATED ALWAYS AS (GREATEST(end_offset - current_offset, 0)) STORED,
    replicated_count  BIGINT      NOT NULL DEFAULT 0,
    last_message_at   TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (job_id, partition_id)
);

CREATE INDEX IF NOT EXISTS idx_metrics_job_id ON consumer_metrics(job_id);

-- ---------------------------------------------------------------------------
-- Helper: auto-update updated_at on any table
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$;

CREATE TRIGGER trg_topic_schemas_updated_at
    BEFORE UPDATE ON topic_schemas
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_replication_jobs_updated_at
    BEFORE UPDATE ON replication_jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_consumer_metrics_updated_at
    BEFORE UPDATE ON consumer_metrics
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
