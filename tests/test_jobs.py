"""start_job / job_status over fake stages: the durable-handle contract."""

from __future__ import annotations

import pytest

from .conftest import call, poll


@pytest.mark.anyio
async def test_run_stage_returns_a_working_handle_and_completes(client, app):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate")
    assert h["status"] == "working" and h["stages_planned"] == ["gate"] and h["poll_after_s"] > 0
    job = await poll(client, h["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["gate"]["gate_total"] > 200 and job["result"]["gate"]["gate_kept"] > 0
    assert job["stages_completed"] == ["gate"]
    assert job["usage"]["stages"]["gate"] and job["usage"]["usd_total"] > 0
    assert job["ttl_s"] and job["ttl_s"] > 13 * 86400
    assert job["poll_after_s"] == 0.0
    d = app.store.run_detail(103)
    gate = next(s for s in d["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "completed" and gate["checkpoint"]["gate_kept"] == job["result"]["gate"]["gate_kept"]
    assert d["next_stage"] == "cluster"
    assert d["status"] == "partial"                   # resumed one stage of a failed run; not complete yet


@pytest.mark.anyio
async def test_run_status_shows_the_active_job_and_progress_advances(client):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate")
    d = await call(client, "run_status", run_id=103)
    assert d["active_job_id"] == h["job_id"]
    job = await poll(client, h["job_id"])
    assert job["progress"]["done"] >= 1 and job["progress"]["message"]
    d = await call(client, "run_status", run_id=103)
    assert d["active_job_id"] is None


@pytest.mark.anyio
async def test_second_job_on_a_working_run_is_refused_naming_the_job(client):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate")
    r = await client.call_tool("start_job", {"kind": "run_stage", "run_id": 103, "stage": "gate"})
    assert r.is_error is True and h["job_id"] in r.content[0].text and "cancel_job" in r.content[0].text
    await poll(client, h["job_id"])


@pytest.mark.anyio
async def test_stage_not_next_needs_force(client):
    r = await client.call_tool("start_job", {"kind": "run_stage", "run_id": 103, "stage": "synthesis"})
    assert r.is_error is True and "'gate'" in r.content[0].text and "force" in r.content[0].text
    r = await client.call_tool("start_job", {"kind": "run_stage", "run_id": 103, "stage": "nope"})
    assert r.is_error is True and "Unknown stage" in r.content[0].text
    h = await call(client, "start_job", kind="run_stage", run_id=102, stage="summary", force=True)
    assert (await poll(client, h["job_id"]))["status"] == "completed"


@pytest.mark.anyio
async def test_unknown_or_expired_job_is_a_tool_error_twice(client, app):
    for _ in range(2):                                                  # Review Focus 2: never a crash
        r = await client.call_tool("job_status", {"job_id": "00000000-0000-0000-0000-000000000000"})
        assert r.is_error is True and "start_job" in r.content[0].text and "list_runs" in r.content[0].text
    h = await call(client, "start_job", kind="run_stage", run_id=105, stage="render", force=True)
    await poll(client, h["job_id"])
    from datetime import datetime, timedelta, timezone
    app.store.job_update(h["job_id"], ttl_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
    r = await client.call_tool("job_status", {"job_id": h["job_id"]})
    assert r.is_error is True and "expired" in r.content[0].text


@pytest.mark.anyio
async def test_stage_that_raises_after_partial_writes_fails_cleanly(fast_settings, synthetic_db):
    """Review Focus 4: the job is failed, fail_stage was called exactly once, run_status shows it."""
    from mcp import Client

    from geoecon_pipeline_mcp.server import create_server
    from geoecon_pipeline_mcp.store import SyntheticStore
    from geoecon_pipeline_mcp.synth import make_fake_stages

    class Spy(SyntheticStore):
        fail_calls: list[tuple] = []

        def fail_stage(self, run_id, stage, error):
            self.fail_calls.append((run_id, stage, error))
            super().fail_stage(run_id, stage, error)

    store = Spy(synthetic_db)

    def factory(client):
        fns = dict(make_fake_stages(store, client, unit_sleep=0.0))

        def boom(run, report, cancel):
            store.execute("UPDATE articles SET gate_geoeconomic=1, gate_score=0.9 WHERE run_id=? AND article_id IN "
                          "(SELECT article_id FROM articles WHERE run_id=? LIMIT 7)", (run["run_id"], run["run_id"]))
            report(1, 16, "gate batch 1/16")
            raise RuntimeError("synthetic backend returned HTTP 503 for every batch")

        fns["gate"] = boom
        return fns

    app = create_server(fast_settings, store=store, stage_factory=factory)
    try:
        async with Client(app.mcp, raise_exceptions=True) as client:
            h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate")
            job = await poll(client, h["job_id"])
            assert job["status"] == "failed"
            assert job["error"] == "RuntimeError: synthetic backend returned HTTP 503 for every batch"
            assert Spy.fail_calls == [(103, "gate", "RuntimeError: synthetic backend returned HTTP 503 for every batch")]
            d = await call(client, "run_status", run_id=103)
            gate = next(s for s in d["stages"] if s["stage_name"] == "gate")
            assert gate["status"] == "failed" and "HTTP 503" in gate["error"]
            assert d["status"] == "failed" and d["next_stage"] == "gate"
            e = await call(client, "failed_stage_error", run_id=103)
            assert "HTTP 503" in e["error"]
    finally:
        app.close()


@pytest.mark.anyio
async def test_reconcile_abandoned_closes_the_stale_run(client, app):
    h = await call(client, "start_job", kind="reconcile_abandoned")
    assert h["run_id"] is None and h["stages_planned"] == []
    job = await poll(client, h["job_id"])
    assert job["status"] == "completed" and job["result"]["reconciled"] == [104]
    d = app.store.run_detail(104)
    assert d["status"] == "failed" and "abandoned" in d["notes"]
    gate = next(s for s in d["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "failed" and "abandoned" in gate["error"]
    hv = await call(client, "pipeline_health")
    assert hv["stale_running"] == 0


@pytest.mark.anyio
async def test_resume_run_on_a_complete_run_is_refused(client):
    r = await client.call_tool("start_job", {"kind": "resume_run", "run_id": 101})
    assert r.is_error is True and "nothing to resume" in r.content[0].text
    r = await client.call_tool("start_job", {"kind": "run_stage", "run_id": 103})
    assert r.is_error is True and "'gate'" in r.content[0].text


@pytest.mark.anyio
async def test_a_run_the_cronjob_is_executing_right_now_is_refused(client, app):
    """Run 104 is `running`; nine hours old it is abandoned and may be resumed, one hour old it is
    live and must not be touched (point at reconcile_abandoned / wait)."""
    from datetime import timedelta
    from geoecon_pipeline_mcp.store import iso
    fresh = app.store.now() - timedelta(hours=1)
    app.store.execute("UPDATE runs SET started_at=? WHERE run_id=104", (iso(fresh),))
    for kind, extra in (("resume_run", {}), ("run_stage", {"stage": "gate"})):
        r = await client.call_tool("start_job", {"kind": kind, "run_id": 104, **extra})
        assert r.is_error is True, kind
        assert "running" in r.content[0].text and "reconcile_abandoned" in r.content[0].text
    r = await client.call_tool("preview_resume", {"run_id": 104})
    assert r.is_error is False and "still running" in r.structured_content["message"]
    app.store.execute("UPDATE runs SET started_at=? WHERE run_id=104", (iso(app.store.now() - timedelta(hours=9)),))
    h = await call(client, "start_job", kind="run_stage", run_id=104, stage="gate")       # abandoned: allowed
    assert (await poll(client, h["job_id"]))["status"] == "completed"


@pytest.mark.anyio
async def test_run_is_marked_running_while_a_job_works_on_it(fast_settings, synthetic_db):
    import threading
    from mcp import Client
    from geoecon_pipeline_mcp.server import create_server
    from geoecon_pipeline_mcp.store import SyntheticStore

    release = threading.Event()
    store = SyntheticStore(synthetic_db)

    def factory(client):
        def gate(run, report, cancel):
            release.wait(10)
            return {"gate_kept": 1}
        return {"gate": gate}

    app = create_server(fast_settings, store=store, stage_factory=factory)
    try:
        async with Client(app.mcp, raise_exceptions=True) as client:
            h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate")
            for _ in range(100):
                d = await call(client, "run_status", run_id=103)
                if d["status"] == "running":
                    break
                import anyio; await anyio.sleep(0.01)
            assert d["status"] == "running" and d["active_job_id"] == h["job_id"]
            release.set()
            assert (await poll(client, h["job_id"]))["status"] == "completed"
            assert (await call(client, "run_status", run_id=103))["status"] == "partial"
    finally:
        app.close()


@pytest.mark.anyio
async def test_cancelled_job_leaves_the_run_failed_not_running(client, app):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate", budget_usd=0.001)
    await poll(client, h["job_id"], until=("input_required",))
    assert app.store.run_detail(103)["status"] == "running"
    await call(client, "cancel_job", job_id=h["job_id"])
    assert (await poll(client, h["job_id"]))["status"] == "cancelled"
    assert app.store.run_detail(103)["status"] == "failed"


def test_stage_set_close_is_called_when_the_job_ends(store):
    """Operator mode opens a database connection per job for the stage functions; the runner must
    close it whether the job completes or fails."""
    from geoecon_pipeline_mcp import cost
    from geoecon_pipeline_mcp.jobs import JobRegistry
    from geoecon_pipeline_mcp.synth import FakeLLM
    from .conftest import ROOT

    closed = []

    class StageSet(dict):
        def close(self):
            closed.append(True)

    def factory(client):
        return StageSet(render=lambda run, report, cancel: {"ok": 1},
                        gate=lambda run, report, cancel: (_ for _ in ()).throw(RuntimeError("boom")))

    reg = JobRegistry(store, factory, pricing=cost.Pricing.load(ROOT / "pricing.yaml"), stage_models={}, llm_factory=FakeLLM)
    import time
    for stages_, expected in ((["render"], "completed"), (["gate"], "failed")):
        job = reg.start("run_stage", 105 if stages_ == ["render"] else 103, stages_, 1.0, "t", {})
        for _ in range(300):
            if reg.get(job["job_id"])["status"] in ("completed", "failed"):
                break
            time.sleep(0.01)
        assert reg.get(job["job_id"])["status"] == expected
    assert closed == [True, True]
    reg.shutdown()
