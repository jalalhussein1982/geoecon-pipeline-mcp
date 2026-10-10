"""The cost gate: estimate before an LLM stage, enforcement between calls, answer_job."""

from __future__ import annotations

import threading

import pytest

from geoecon_pipeline_mcp import cost
from geoecon_pipeline_mcp.synth import FakeLLM

from .conftest import ROOT, call, poll

PRICING = cost.Pricing.load(ROOT / "pricing.yaml")


# ── counting client ─────────────────────────────────────────────────────────────────


def test_counting_client_records_tokens_and_calls_back_when_exceeded():
    usage = cost.empty_usage()
    budget = cost.Budget(limit_usd=0.001)
    asked = []
    c = cost.CountingLLMClient(FakeLLM(3000, 600), PRICING, budget, usage, on_exceeded=lambda spent: asked.append(spent))
    c.set_stage("gate")
    c.complete("deepseek-v3.2", [])
    assert c.calls == 1 and usage["stages"]["gate"]["deepseek-v3.2"]["prompt_tokens"] == 3000
    assert budget.spent_usd == pytest.approx(PRICING.usd("deepseek-v3.2", 3000, 600))
    assert asked == []                                       # the first call always runs; the ledger fills after it
    c.complete("deepseek-v3.2", [])
    assert asked == [pytest.approx(budget.spent_usd - PRICING.usd("deepseek-v3.2", 3000, 600))]
    assert c.calls == 2 and usage["usd_total"] == pytest.approx(budget.spent_usd)
    assert c.prompt_tokens == 3000                            # unknown attributes pass through to the inner client


def test_counting_client_raises_job_cancelled_when_flag_is_set():
    from geoecon_pipeline_mcp.jobs import JobCancelled
    cancel = threading.Event(); cancel.set()
    c = cost.CountingLLMClient(FakeLLM(), PRICING, cost.Budget(10), cost.empty_usage(), cancel=cancel)
    with pytest.raises(JobCancelled):
        c.complete("deepseek-v3.2", [])


# ── estimate above budget -> input_required ─────────────────────────────────────────


@pytest.mark.anyio
async def test_estimate_above_budget_pauses_with_the_form_schema(client, app):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate", budget_usd=0.01)
    job = await poll(client, h["job_id"], until=("input_required", "completed", "failed"))
    assert job["status"] == "input_required"
    req = job["input_request"]
    assert req["key"] == "budget" and "gate" in req["message"] and "0.0100" in req["message"]
    assert req["requested_schema"]["required"] == ["approve"]
    assert set(req["requested_schema"]["properties"]) == {"approve", "new_budget_usd"}
    assert job["budget_usd"] == 0.01 and job["stage"] == "gate"
    gate = next(s for s in app.store.run_detail(103)["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "failed"                        # nothing started yet; the fixture's failure stands
    await call(client, "answer_job", job_id=h["job_id"], approve=False)
    await poll(client, h["job_id"])


@pytest.mark.anyio
async def test_approve_resumes_and_completes_with_usage(client, app):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate", budget_usd=0.01)
    await poll(client, h["job_id"], until=("input_required",))
    a = await call(client, "answer_job", job_id=h["job_id"], approve=True, new_budget_usd=1.0)
    assert a["accepted"] is True and a["budget_usd"] == 1.0 and a["status"] in ("working", "completed")
    job = await poll(client, h["job_id"])
    assert job["status"] == "completed" and job["budget_usd"] == 1.0
    assert job["usage"]["stages"]["gate"]["deepseek-v3.2"]["calls"] >= 15 and job["usage"]["usd_total"] > 0
    assert job["input_request"] is None
    assert next(s for s in app.store.run_detail(103)["stages"] if s["stage_name"] == "gate")["status"] == "completed"


@pytest.mark.anyio
async def test_approve_without_a_number_takes_the_estimate_as_ceiling(client):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate", budget_usd=0.001)
    await poll(client, h["job_id"], until=("input_required",))
    a = await call(client, "answer_job", job_id=h["job_id"], approve=True)
    assert a["budget_usd"] > 0.001
    assert (await poll(client, h["job_id"]))["status"] == "completed"


@pytest.mark.anyio
async def test_abort_fails_the_stage_cleanly(client, app):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate", budget_usd=0.01)
    await poll(client, h["job_id"], until=("input_required",))
    a = await call(client, "answer_job", job_id=h["job_id"], approve=False)
    assert a["status"] == "failed"
    job = await poll(client, h["job_id"])
    assert job["status"] == "failed" and job["error"] == "aborted at budget gate"
    gate = next(s for s in app.store.run_detail(103)["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "failed" and gate["error"] == "aborted at budget gate"
    e = await call(client, "failed_stage_error", run_id=103)
    assert e["error"] == "aborted at budget gate"


@pytest.mark.anyio
async def test_mid_run_overspend_pauses_again(synthetic_db):
    """The estimate fits the budget; the real token counts do not. The counting client pauses
    the job at the next call boundary and asks again."""
    from mcp import Client

    from geoecon_pipeline_mcp.server import Settings, create_server

    settings = Settings(mode="synthetic", synthetic_db=synthetic_db, fake_unit_sleep=0.0,
                        fake_prompt_tokens=300_000, fake_completion_tokens=60_000)     # 100x the estimate
    app = create_server(settings)
    try:
        async with Client(app.mcp, raise_exceptions=True) as client:
            est = (await call(client, "preview_resume", run_id=103))["estimates"][0]
            budget = round(est["usd"] * 2, 4)                                         # estimate passes...
            h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate", budget_usd=budget)
            job = await poll(client, h["job_id"], until=("input_required", "completed", "failed"))
            assert job["status"] == "input_required"                                   # ...the ledger does not
            assert "spent" in job["input_request"]["message"] and "ceiling" in job["input_request"]["message"]
            assert job["usage"]["usd_total"] > budget
            gate = next(s for s in app.store.run_detail(103)["stages"] if s["stage_name"] == "gate")
            assert gate["status"] == "running"                                         # mid-stage pause
            await call(client, "answer_job", job_id=h["job_id"], approve=True, new_budget_usd=100.0)
            job = await poll(client, h["job_id"], timeout=30)
            assert job["status"] == "completed" and job["usage"]["usd_total"] > budget
    finally:
        app.close()


@pytest.mark.anyio
async def test_answer_job_on_a_job_not_waiting_is_a_tool_error(client):
    h = await call(client, "start_job", kind="run_stage", run_id=105, stage="render", force=True)
    job = await poll(client, h["job_id"])
    assert job["status"] == "completed"
    r = await client.call_tool("answer_job", {"job_id": h["job_id"], "approve": True})
    assert r.is_error is True and "completed" in r.content[0].text
    r = await client.call_tool("answer_job", {"job_id": "missing", "approve": True})
    assert r.is_error is True and "start_job" in r.content[0].text


@pytest.mark.anyio
async def test_answer_after_cancel_is_refused_and_nothing_resumes(client, app):
    """Review Focus 3."""
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate", budget_usd=0.01)
    await poll(client, h["job_id"], until=("input_required",))
    c = await call(client, "cancel_job", job_id=h["job_id"])
    job = await poll(client, h["job_id"])
    assert job["status"] == "cancelled" and c["status"] in ("cancelled", "input_required")
    r = await client.call_tool("answer_job", {"job_id": h["job_id"], "approve": True, "new_budget_usd": 5.0})
    assert r.is_error is True and "job is cancelled" in r.content[0].text
    gate = next(s for s in app.store.run_detail(103)["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "failed" and "NullGateError" in gate["error"]      # untouched: the stage never started
    assert (await call(client, "job_status", job_id=h["job_id"]))["status"] == "cancelled"
