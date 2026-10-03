-- Add last_error column to capture pod/deployment failure reasons
ALTER TABLE replication_jobs ADD COLUMN IF NOT EXISTS last_error TEXT;
