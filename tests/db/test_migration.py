"""Our migration lands in the pipeline's ledger and the jobs table round-trips."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from geoecon_pipeline_mcp.pg import MIGRATIONS_DIR, apply_all_migrations

pytestmark = pytest.mark.db


def test_migrations_are_recorded_and_idempotent(pg_store):
    rows = pg_store.conn.execute("SELECT filename FROM schema_migrations ORDER BY filename").fetchall()
    names = [r[0] for r in rows]
    assert "001_init.sql" in names and "008_stage_discovery_search.sql" in names
    assert "001_mcp_jobs.sql" in names
    assert (MIGRATIONS_DIR / "001_mcp_jobs.sql").exists()
    assert apply_all_migrations(pg_store.conn) == []                 # second apply: nothing to do


def test_mcp_jobs_columns_match_the_spec(pg_store):
    cols = {r[0]: r[1] for r in pg_store.conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='mcp_jobs'").fetchall()}
    assert cols["job_id"] == "uuid" and cols["stages_planned"] == "ARRAY" and cols["usage"] == "jsonb"
    assert {"kind", "run_id", "status", "stage", "progress", "input_request", "input_response", "budget_usd",
            "result", "error", "principal", "trace", "created_at", "updated_at", "finished_at", "ttl_expires_at"} <= set(cols)


def test_job_insert_get_update_round_trip(pg_store, repo):
    now = datetime.now(timezone.utc)
    run_id = repo.create_run(now - timedelta(hours=24), now, now, "deepseek-v3.2", "task_a_cov")
    jid = str(uuid.uuid4())
    pg_store.job_insert({"job_id": jid, "kind": "resume_run", "run_id": run_id, "stages_planned": ["gate", "cluster"],
                         "status": "working", "budget_usd": 2.5, "usage": {"stages": {}, "usd_total": 0.0},
                         "trace": {"traceparent": "00-abc-def-01"}, "principal": "ops@example",
                         "created_at": now, "updated_at": now})
    row = pg_store.job_get(jid)
    assert row["job_id"] == jid and row["stages_planned"] == ["gate", "cluster"] and row["budget_usd"] == 2.5
    assert row["trace"] == {"traceparent": "00-abc-def-01"} and row["status"] == "working"
    assert pg_store.jobs_working()[-1]["job_id"] == jid
    assert [j["job_id"] for j in pg_store.jobs_for_run(run_id)] == [jid]
    pg_store.job_update(jid, status="input_required", input_request={"key": "budget", "message": "m", "requested_schema": {}},
                        progress={"done": 3, "total": 16, "message": "gate batch 3/16"})
    row = pg_store.job_get(jid)
    assert row["status"] == "input_required" and row["input_request"]["key"] == "budget" and row["progress"]["done"] == 3
    pg_store.job_update(jid, status="completed", result={"gate": {"gate_kept": 1}}, finished_at=now,
                        ttl_expires_at=now + timedelta(days=14), input_request=None)
    row = pg_store.job_get(jid)
    assert row["status"] == "completed" and row["result"]["gate"]["gate_kept"] == 1 and row["input_request"] is None
    assert row["ttl_expires_at"] > now
    assert pg_store.job_get("not-a-uuid") is None and pg_store.job_get(str(uuid.uuid4())) is None


def test_status_check_constraint_rejects_unknown_vocabulary(pg_store):
    import psycopg
    now = datetime.now(timezone.utc)
    with pytest.raises(psycopg.errors.CheckViolation):
        pg_store.job_insert({"job_id": str(uuid.uuid4()), "kind": "run_stage", "status": "running", "budget_usd": 1.0,
                             "created_at": now, "updated_at": now})
