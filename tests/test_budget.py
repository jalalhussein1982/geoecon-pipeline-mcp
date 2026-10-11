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


# ── fan-out: the upstream gate/synth stages run LLM calls in a ThreadPoolExecutor ───────


class _PromptCountingStore:
    """Wrap a SyntheticStore so the test can count how often a job was parked in input_required."""

    def __init__(self, inner):
        self._inner = inner
        self.prompts = 0

    def job_update(self, job_id, **fields):
        if fields.get("status") == "input_required":
            self.prompts += 1
        return self._inner.job_update(job_id, **fields)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _fanout_registry(store, n_calls=10, workers=4):
    """A stage that mirrors gate.run: LLM calls fan out over a `with ThreadPoolExecutor` block
    and the main thread collects `fut.result()`; the first raise leaves the block via
    shutdown(wait=True), so queued batches still call complete()."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def factory(client):
        def gate(run, report, cancel):
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = [ex.submit(client.complete, "deepseek-v3.2", [{"role": "user", "content": "x"}]) for _ in range(n_calls)]
                for fut in as_completed(futs):
                    fut.result()
            return {"gate_total": n_calls}
        return {"gate": gate}

    from geoecon_pipeline_mcp.jobs import JobRegistry
    return JobRegistry(store, factory, pricing=PRICING, stage_models={"gate": "deepseek-v3.2"},
                       llm_factory=lambda: FakeLLM(300_000, 60_000, latency_s=0.01))


def _wait_status(reg, job_id, statuses, timeout=5.0):
    import time
    t0 = time.time()
    while time.time() - t0 < timeout:
        row = reg.get(job_id)
        if row["status"] in statuses:
            return row
        time.sleep(0.01)
    raise AssertionError(f"job did not reach {statuses}: {reg.get(job_id)}")


def test_budget_abort_under_fanout_prompts_exactly_once(store):
    spy = _PromptCountingStore(store)
    reg = _fanout_registry(spy)
    job = reg.start("run_stage", 103, ["gate"], 0.2, "test", {})          # estimate 0.024 fits; real calls do not
    _wait_status(reg, job["job_id"], ("input_required",))
    reg.answer(job["job_id"], False, None)
    row = _wait_status(reg, job["job_id"], ("failed", "completed", "cancelled"))
    assert row["status"] == "failed" and row["error"] == "aborted at budget gate"
    assert spy.prompts == 1, f"the operator was asked {spy.prompts} times"
    gate = next(s for s in store.run_detail(103)["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "failed" and gate["error"] == "aborted at budget gate"
    reg.shutdown()


def test_budget_approve_under_fanout_asks_once_per_overspend_not_per_worker(store):
    spy = _PromptCountingStore(store)
    reg = _fanout_registry(spy)
    job = reg.start("run_stage", 103, ["gate"], 0.2, "test", {})
    _wait_status(reg, job["job_id"], ("input_required",))
    reg.answer(job["job_id"], True, 100.0)
    row = _wait_status(reg, job["job_id"], ("failed", "completed", "cancelled"))
    assert row["status"] == "completed" and row["result"]["gate"]["gate_total"] == 10
    assert spy.prompts == 1
    reg.shutdown()


# ── handshake races (finding 7) ───────────────────────────────────────────────────────


def _runner(store, budget=1.0):
    from geoecon_pipeline_mcp.jobs import JobRegistry, JobRunner
    reg = JobRegistry(store, lambda c: {}, pricing=PRICING, stage_models={}, llm_factory=FakeLLM)
    job = {"job_id": "11111111-1111-1111-1111-111111111111", "kind": "run_stage", "run_id": 103, "stages_planned": ["gate"],
           "budget_usd": budget, "status": "working"}
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    store.job_insert({**job, "created_at": now, "updated_at": now})
    return reg, JobRunner(reg, job)


def test_cancel_that_lands_before_the_wait_is_not_lost(store):
    """request_cancel sets cancel+decision; _ask_budget then clears decision. Without a re-check
    the worker would block forever on a wake-up that already happened."""
    from geoecon_pipeline_mcp.jobs import JobCancelled
    reg, runner = _runner(store)
    runner.request_cancel()
    outcome = []

    def go():
        try:
            runner._ask_budget("m", 1.0)
        except BaseException as e:  # noqa: BLE001
            outcome.append(type(e).__name__)

    t = threading.Thread(target=go, daemon=True); t.start(); t.join(2.0)
    assert not t.is_alive(), "worker blocked on a lost wake-up"
    assert outcome == ["JobCancelled"]


def test_a_second_answer_is_refused(store):
    from geoecon_pipeline_mcp.jobs import NotWaiting
    reg, runner = _runner(store)
    reg._runners[runner.job_id] = runner
    gate_open = threading.Event()
    result = []

    def go():
        try:
            runner._ask_budget("m", 0.5); result.append("resumed")
        except BaseException as e:  # noqa: BLE001
            result.append(type(e).__name__)

    t = threading.Thread(target=go, daemon=True); t.start()
    deadline = threading.Event()
    for _ in range(200):
        if reg.get(runner.job_id)["status"] == "input_required":
            break
        threading.Event().wait(0.01)
    assert runner.deliver_answer(True, 2.0) is True
    assert runner.deliver_answer(False, None) is False            # slot already consumed
    t.join(2.0)
    assert result == ["resumed"] and runner.budget.limit_usd == 2.0


# ── runtime approval needs a number (finding 5) ───────────────────────────────────────


@pytest.mark.anyio
async def test_runtime_overspend_approval_without_a_number_is_refused(synthetic_db):
    from mcp import Client
    from geoecon_pipeline_mcp.server import Settings, create_server

    settings = Settings(mode="synthetic", synthetic_db=synthetic_db, fake_unit_sleep=0.0,
                        fake_prompt_tokens=300_000, fake_completion_tokens=60_000)
    app = create_server(settings)
    try:
        async with Client(app.mcp, raise_exceptions=True) as client:
            est = (await call(client, "preview_resume", run_id=103))["estimates"][0]
            h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate", budget_usd=round(est["usd"] * 2, 4))
            job = await poll(client, h["job_id"], until=("input_required", "completed", "failed"))
            assert job["status"] == "input_required" and job["input_request"]["requires_new_budget_usd"] is True
            r = await client.call_tool("answer_job", {"job_id": h["job_id"], "approve": True})
            assert r.is_error is True and "new_budget_usd" in r.content[0].text
            assert (await call(client, "job_status", job_id=h["job_id"]))["status"] == "input_required"   # still waiting
            await call(client, "answer_job", job_id=h["job_id"], approve=True, new_budget_usd=50.0)
            assert (await poll(client, h["job_id"], timeout=30))["status"] == "completed"
    finally:
        app.close()


# ── estimate against the remaining budget, models from the pipeline config (finding 10) ──


def test_second_llm_stage_is_gated_against_what_is_left(store):
    """Budget 0.025: gate's estimate (0.0235) fits and the fake gate spends exactly that, leaving
    0.0015; cluster's estimate (~0.0033 for 60 kept articles in 3 countries) must now prompt even
    though it is far below the total ceiling."""
    from geoecon_pipeline_mcp.jobs import JobRegistry

    def factory(client):
        def gate(run, report, cancel):
            for _ in range(16):
                client.complete("deepseek-v3.2", [])
            store.execute("UPDATE articles SET gate_geoeconomic=1, gate_score=0.8, "
                          "gate_country=CASE article_id % 3 WHEN 0 THEN 'SA' WHEN 1 THEN 'BR' ELSE 'EG' END, "
                          "country_fips=CASE article_id % 3 WHEN 0 THEN 'SA' WHEN 1 THEN 'BR' ELSE 'EG' END "
                          "WHERE run_id=103 AND article_id IN (SELECT article_id FROM articles WHERE run_id=103 LIMIT 60)")
            return {"gate_total": 228, "gate_kept": 60}

        def cluster(run, report, cancel):
            return {"clusters": 1}
        return {"gate": gate, "cluster": cluster}

    spy = _PromptCountingStore(store)
    reg = JobRegistry(spy, factory, pricing=PRICING, stage_models={"gate": "deepseek-v3.2", "cluster": "deepseek-v3.2"},
                      llm_factory=lambda: FakeLLM(3000, 600))
    job = reg.start("resume_run", 103, ["gate", "cluster"], 0.025, "test", {})
    row = _wait_status(reg, job["job_id"], ("input_required", "completed", "failed"))
    assert row["status"] == "input_required" and row["stage"] == "cluster"
    assert "remaining" in row["input_request"]["message"]
    reg.answer(job["job_id"], True, 1.0)
    assert _wait_status(reg, job["job_id"], ("completed", "failed"))["status"] == "completed"
    reg.shutdown()


def test_stage_models_come_from_the_pipeline_config():
    from geoecon.config import Config
    from geoecon_pipeline_mcp.cost import stage_models_from_config
    cfg = Config.from_env({"DB_DSN": "x", "MAETCENTRUM_KEY": "k", "SMTP_HOST": "h", "SMTP_USER": "u", "SMTP_PASSWORD": "p",
                           "FROM_ADDR": "f", "RECIPIENTS": "a@b", "GATE_MODEL": "gemma4", "DEDUP_MODEL": "kimi",
                           "SYNTHESIS_MODEL": "deepseek"})
    assert stage_models_from_config(cfg) == {"gate": "gemma4", "cluster": "kimi", "synthesis": "deepseek", "summary": "deepseek"}


# ── usage snapshot (finding 9) ────────────────────────────────────────────────────────


def test_on_usage_receives_a_snapshot_taken_under_the_lock():
    seen = []
    c = cost.CountingLLMClient(FakeLLM(), PRICING, cost.Budget(10), cost.empty_usage(), on_usage=lambda snap: seen.append(snap))
    c.set_stage("gate")
    c.complete("deepseek-v3.2", [])
    assert len(seen) == 1 and seen[0] is not c.usage and seen[0] == c.usage
    seen[0]["stages"]["gate"]["deepseek-v3.2"]["calls"] = 99
    assert c.usage["stages"]["gate"]["deepseek-v3.2"]["calls"] == 1              # a deep copy, not a view
