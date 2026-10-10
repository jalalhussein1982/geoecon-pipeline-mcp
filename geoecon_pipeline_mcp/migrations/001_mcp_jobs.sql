-- geoecon-pipeline-mcp: durable job handles (design spec §7.1).
-- Owned by the MCP server, applied by it at startup (operator mode). The pipeline never
-- reads this table. Completed rows are kept 14 days (ttl_expires_at) and then purgeable.

CREATE TABLE IF NOT EXISTS mcp_jobs (
    job_id          UUID PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('resume_run','run_stage','reconcile_abandoned')),
    run_id          BIGINT REFERENCES runs(run_id) ON DELETE SET NULL,
    stages_planned  TEXT[] NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL
                    CHECK (status IN ('working','input_required','completed','failed','cancelled')),
    stage           TEXT,
    progress        JSONB,
    input_request   JSONB,
    input_response  JSONB,
    budget_usd      NUMERIC(10,4) NOT NULL DEFAULT 0,
    usage           JSONB NOT NULL DEFAULT '{}'::jsonb,
    result          JSONB,
    error           TEXT,
    principal       TEXT,
    trace           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    ttl_expires_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS mcp_jobs_run_idx    ON mcp_jobs (run_id);
CREATE INDEX IF NOT EXISTS mcp_jobs_status_idx ON mcp_jobs (status);

COMMENT ON TABLE mcp_jobs IS 'MCP server job handles; mirrors the MCP tasks extension Task object. One row per start_job.';
