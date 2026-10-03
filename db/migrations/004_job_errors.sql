-- Track per-message errors reported by worker pods (partition + offset context)
CREATE TABLE IF NOT EXISTS job_errors (
    id           UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    job_id       UUID NOT NULL REFERENCES replication_jobs(id) ON DELETE CASCADE,
    partition_id INTEGER NOT NULL,
    "offset"     BIGINT NOT NULL,
    error_msg    TEXT NOT NULL,
    occurred_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_job_errors_job_id ON job_errors(job_id, occurred_at DESC);
