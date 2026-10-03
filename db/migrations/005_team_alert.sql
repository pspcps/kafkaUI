-- 005_team_alert.sql — add team_name and alert_channel to replication_jobs
ALTER TABLE replication_jobs
    ADD COLUMN IF NOT EXISTS team_name     TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS alert_channel TEXT NOT NULL DEFAULT '';
