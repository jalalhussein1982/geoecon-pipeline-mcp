-- geoecon-pipeline-mcp: job ownership and liveness.
-- owner_id identifies the server process that runs the job's thread; heartbeat_at is refreshed
-- by that process every few seconds while the job is active. Restart recovery fails only active
-- rows whose heartbeat is stale, so a second process on the same database never kills live jobs.

ALTER TABLE mcp_jobs ADD COLUMN IF NOT EXISTS owner_id     TEXT;
ALTER TABLE mcp_jobs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS mcp_jobs_heartbeat_idx ON mcp_jobs (heartbeat_at) WHERE status IN ('working','input_required');
