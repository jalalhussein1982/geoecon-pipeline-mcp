"""Cooperative cancel: between stages and between LLM batches."""

from __future__ import annotations

import threading

import anyio
import pytest

from geoecon_pipeline_mcp import cost
from geoecon_pipeline_mcp.jobs import JobRegistry
from geoecon_pipeline_mcp.synth import FakeLLM

from .conftest import ROOT, call, poll


def _registry(store, factory, **kw):
    return JobRegistry(store, factory, pricing=cost.Pricing.load(ROOT / "pricing.yaml"),
                       stage_models={"gate": "deepseek-v3.2"}, llm_factory=FakeLLM, **kw)


def _wait(reg, job_id, timeout=5.0):
    import time
    t0 = time.time()
    while time.time() - t0 < timeout:
        row = reg.get(job_id)
        if row["status"] in ("completed", "failed", "cancelled"):
            return row
        time.sleep(0.01)
    raise AssertionError(f"job did not finish: {reg.get(job_id)}")


def test_cancel_between_two_planned_stages_never_starts_the_second(store):
    started, first_running = [], threading.Event()
    release = threading.Event()

    def factory(client):
        def first(run, report, cancel):
            started.append("first"); first_running.set()
            release.wait(5)
            return {"first": 1}

        def second(run, report, cancel):
            started.append("second")
            return {"second": 1}

        return {"extraction": first, "gate": second}

    reg = _registry(store, factory)
    job = reg.start("resume_run", 104, ["extraction", "gate"], 2.0, "test", {})
    assert first_running.wait(5)
    reg.cancel(job["job_id"])
    release.set()
    row = _wait(reg, job["job_id"])
    assert row["status"] == "cancelled" and row["error"] == "cancelled via MCP"
    assert started == ["first"]
    d = store.run_detail(104)
    ext = next(s for s in d["stages"] if s["stage_name"] == "extraction")
    assert ext["status"] == "completed"                         # the running stage finished and was recorded
    assert next(s for s in d["stages"] if s["stage_name"] == "gate")["status"] == "running"  # untouched (fixture state)
    reg.shutdown()


def test_cancel_between_llm_batches_marks_the_stage_failed(store):
    calls = []

    def factory(client):
        def gate(run, report, cancel):
            for i in range(50):
                client.complete("deepseek-v3.2", [{"role": "user", "content": "x"}])
                calls.append(i)
                if i == 3:
                    reg.cancel(job["job_id"])          # cancel mid-stage; the next LLM call must raise
            return {"never": True}

        return {"gate": gate}

    reg = _registry(store, factory)
    job = reg.start("run_stage", 103, ["gate"], 2.0, "test", {})
    row = _wait(reg, job["job_id"])
    assert row["status"] == "cancelled"
    assert len(calls) == 4                                       # the running call finished, the next one did not start
    gate = next(s for s in store.run_detail(103)["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "failed" and gate["error"] == "cancelled via MCP"
    reg.shutdown()


def test_cancel_is_idempotent_on_terminal_jobs(store):
    reg = _registry(store, lambda c: {"render": lambda run, report, cancel: {"ok": 1}})
    job = reg.start("run_stage", 105, ["render"], 2.0, "test", {})
    row = _wait(reg, job["job_id"])
    assert row["status"] == "completed"
    assert reg.cancel(job["job_id"])["status"] == "completed"
    reg.shutdown()


@pytest.mark.anyio
async def test_cancel_job_tool(client, app):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate")
    c = await call(client, "cancel_job", job_id=h["job_id"])
    assert c["status"] in ("working", "cancelled")
    job = await poll(client, h["job_id"])
    assert job["status"] in ("cancelled", "completed")           # a very fast stage may finish first
    again = await call(client, "cancel_job", job_id=h["job_id"])
    assert again["status"] == job["status"] and "nothing to cancel" in again["message"]
    r = await client.call_tool("cancel_job", {"job_id": "nope"})
    assert r.is_error is True
