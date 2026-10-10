"""subscriptions/listen: a host that listens on job:// and run:// is told about changes."""

from __future__ import annotations

import anyio
import pytest

from mcp.shared.subscriptions import ResourceUpdated

from .conftest import call, poll


@pytest.fixture
def fast_settings(synthetic_db):
    from geoecon_pipeline_mcp.server import Settings

    return Settings(mode="synthetic", synthetic_db=synthetic_db, fake_unit_sleep=0.04)    # ~0.7 s per gate


async def _collect(sub, n: int, timeout: float = 10.0):
    events = []
    with anyio.move_on_after(timeout):
        async for ev in sub:
            events.append(ev)
            if len(events) >= n:
                break
    return events


@pytest.mark.anyio
async def test_run_and_job_subscriptions_receive_resource_updated(client):
    async with client.listen(resource_subscriptions=["run://103"]) as run_sub:
        h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate")
        async with client.listen(resource_subscriptions=[f"job://{h['job_id']}"]) as job_sub:
            run_events = await _collect(run_sub, 2)
            job_events = await _collect(job_sub, 2)
    assert len(run_events) >= 2 and all(isinstance(e, ResourceUpdated) and e.uri == "run://103" for e in run_events)
    assert len(job_events) >= 2 and all(e.uri == f"job://{h['job_id']}" for e in job_events)
    job = await poll(client, h["job_id"])
    assert job["status"] == "completed"


@pytest.mark.anyio
async def test_two_stage_job_notifies_on_every_status_change(client):
    h = await call(client, "start_job", kind="resume_run", run_id=103, budget_usd=0.01)       # pauses at gate
    await poll(client, h["job_id"], until=("input_required",))
    async with client.listen(resource_subscriptions=[f"job://{h['job_id']}"]) as sub:
        await call(client, "answer_job", job_id=h["job_id"], approve=True, new_budget_usd=5.0)
        events = await _collect(sub, 3, timeout=30)
    assert len(events) >= 3                      # working, per-stage completions, completed
    job = await poll(client, h["job_id"], timeout=30)
    assert job["status"] == "completed"


@pytest.mark.anyio
async def test_unrelated_uris_are_not_delivered(client):
    async with client.listen(resource_subscriptions=["run://101"]) as sub:
        h = await call(client, "start_job", kind="run_stage", run_id=105, stage="render", force=True)
        await poll(client, h["job_id"])
        events = await _collect(sub, 1, timeout=0.5)
    assert events == []
