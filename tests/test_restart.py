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


# ── ownership and heartbeats (finding 6) ────────────────────────────────────────────────


def _registry(store, **kw):
    return JobRegistry(store, lambda c: {}, pricing=cost.Pricing.load(ROOT / "pricing.yaml"), stage_models={},
                       llm_factory=FakeLLM, **kw)


def test_recovery_only_fails_rows_whose_heartbeat_is_stale(store):
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    base = {"kind": "run_stage", "run_id": 103, "stages_planned": ["gate"], "status": "working", "budget_usd": 1.0,
            "created_at": now, "updated_at": now}
    store.job_insert({**base, "job_id": "live-elsewhere", "owner_id": "other-host:1", "heartbeat_at": now})
    store.job_insert({**base, "job_id": "dead-elsewhere", "owner_id": "other-host:2", "heartbeat_at": now - timedelta(minutes=5)})
    store.job_insert({**base, "job_id": "legacy-no-heartbeat", "owner_id": None, "heartbeat_at": None})
    reg = _registry(store)
    assert reg.recovered == 2
    assert reg.get("live-elsewhere")["status"] == "working"                 # another live process owns it
    assert reg.get("dead-elsewhere")["status"] == "failed" and reg.get("dead-elsewhere")["error"] == "server restarted"
    assert reg.get("legacy-no-heartbeat")["status"] == "failed"
    assert reg.owner_id and reg.owner_id != "other-host:1"


def test_runner_heartbeats_and_rows_carry_the_owner(store):
    import threading
    import time
    release = threading.Event()
    reg = _registry(store, heartbeat_s=0.05)
    reg.stage_factory = lambda c: {"render": lambda run, report, cancel: (release.wait(5) and None) or {"ok": 1}}
    job = reg.start("run_stage", 105, ["render"], 1.0, "test", {})
    row = reg.get(job["job_id"])
    assert row["owner_id"] == reg.owner_id and row["heartbeat_at"] is not None
    first = row["heartbeat_at"]
    deadline = time.time() + 3
    while time.time() < deadline and reg.get(job["job_id"])["heartbeat_at"] <= first:
        time.sleep(0.02)
    assert reg.get(job["job_id"])["heartbeat_at"] > first
    release.set()
    for _ in range(200):
        if reg.get(job["job_id"])["status"] == "completed":
            break
        time.sleep(0.01)
    assert reg.get(job["job_id"])["status"] == "completed"
    reg.shutdown()


def test_fail_if_active_never_overwrites_a_terminal_row(store):
    now = datetime.now(timezone.utc)
    store.job_insert({"job_id": "done", "kind": "run_stage", "run_id": 103, "status": "completed", "budget_usd": 1.0,
                      "result": {"gate": {}}, "created_at": now, "updated_at": now, "finished_at": now})
    store.job_insert({"job_id": "live", "kind": "run_stage", "run_id": 103, "status": "working", "budget_usd": 1.0,
                      "created_at": now, "updated_at": now})
    assert store.job_fail_if_active("done", "server restarted") is False
    assert store.job_get("done")["status"] == "completed" and store.job_get("done")["error"] is None
    assert store.job_fail_if_active("live", "server restarted") is True
    assert store.job_get("live")["status"] == "failed" and store.job_get("live")["ttl_expires_at"] is not None


def test_cancel_without_a_runner_uses_the_conditional_update(store):
    now = datetime.now(timezone.utc)
    reg = _registry(store)
    store.job_insert({"job_id": "orphan", "kind": "run_stage", "run_id": 103, "status": "working", "budget_usd": 1.0,
                      "owner_id": "other:1", "heartbeat_at": now, "created_at": now, "updated_at": now})
    store.job_update("orphan", status="completed", finished_at=now)          # finished between the read and the write
    assert reg.cancel("orphan")["status"] == "completed"


# ── expired rows are purged at startup (finding 11) ────────────────────────────────────


def test_expired_rows_are_purged_at_startup(store):
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    store.job_insert({"job_id": "old", "kind": "run_stage", "run_id": 103, "status": "completed", "budget_usd": 1.0,
                      "created_at": now - timedelta(days=20), "updated_at": now - timedelta(days=20),
                      "finished_at": now - timedelta(days=20), "ttl_expires_at": now - timedelta(days=6)})
    store.job_insert({"job_id": "recent", "kind": "run_stage", "run_id": 103, "status": "completed", "budget_usd": 1.0,
                      "created_at": now, "updated_at": now, "finished_at": now, "ttl_expires_at": now + timedelta(days=14)})
    reg = _registry(store)
    assert reg.purged == 1
    assert store.job_get("old") is None and store.job_get("recent") is not None
