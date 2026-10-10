"""A server restart cannot resume a thread: working rows become failed, honestly labelled."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from geoecon_pipeline_mcp import cost
from geoecon_pipeline_mcp.jobs import JobRegistry
from geoecon_pipeline_mcp.synth import FakeLLM

from .conftest import ROOT, call


def test_working_rows_are_failed_at_startup(store):
    now = datetime.now(timezone.utc)
    for jid, status in (("j-working", "working"), ("j-input", "input_required"), ("j-done", "completed")):
        store.job_insert({"job_id": jid, "kind": "run_stage", "run_id": 103, "stages_planned": ["gate"],
                          "status": status, "budget_usd": 2.0, "created_at": now, "updated_at": now,
                          "input_request": {"key": "budget"} if status == "input_required" else None})
    reg = JobRegistry(store, lambda c: {}, pricing=cost.Pricing.load(ROOT / "pricing.yaml"),
                      stage_models={}, llm_factory=FakeLLM)
    assert reg.recovered == 2
    for jid in ("j-working", "j-input"):
        row = reg.get(jid)
        assert row["status"] == "failed" and row["error"] == "server restarted"
        assert row["finished_at"] is not None and row["ttl_expires_at"] is not None
        assert row["input_request"] is None
    assert reg.get("j-done")["status"] == "completed"
    assert reg.active_for_run(103) is None                     # the run is free for a new job


@pytest.mark.anyio
async def test_restart_recovery_is_visible_through_the_tools(synthetic_db, fast_settings):
    from mcp import Client

    from geoecon_pipeline_mcp.server import create_server
    from geoecon_pipeline_mcp.store import SyntheticStore

    now = datetime.now(timezone.utc)
    s = SyntheticStore(synthetic_db)
    s.job_insert({"job_id": "orphan", "kind": "resume_run", "run_id": 103, "stages_planned": ["gate", "cluster"],
                  "status": "working", "stage": "gate", "budget_usd": 2.0, "created_at": now, "updated_at": now})
    s.close()
    app = create_server(fast_settings)
    try:
        async with Client(app.mcp, raise_exceptions=True) as client:
            job = await call(client, "job_status", job_id="orphan")
            assert job["status"] == "failed" and job["error"] == "server restarted"
            h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate")   # run is free again
            assert h["status"] == "working"
    finally:
        app.close()
