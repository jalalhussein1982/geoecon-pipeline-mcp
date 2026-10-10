"""Health verdict (watchdog + stale runs) and the cost side (pricing, estimate, report)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from mcp import Client

from geoecon_pipeline_mcp import cost, health
from geoecon_pipeline_mcp.synth import ANCHOR_NOW

from .conftest import ROOT

PRICING = ROOT / "pricing.yaml"


@pytest.fixture
def server(synthetic_db):
    from geoecon_pipeline_mcp.server import Settings, create_server

    app = create_server(Settings(mode="synthetic", synthetic_db=synthetic_db))
    yield app
    app.close()


@pytest.fixture
async def client(server):
    async with Client(server.mcp, raise_exceptions=True) as c:
        yield c


# ── health ───────────────────────────────────────────────────────────────────────────


def test_verdict_reports_the_stale_run_104_and_the_unsent_morning(store):
    v = health.verdict(store, ANCHOR_NOW)
    assert v["ok"] is False
    assert v["stale_running"] == 1 and v["stale_run_ids"] == [104]
    assert v["latest_run_id"] == 105
    assert "no newsletter" in v["subject"]                # 105 completed but sent nothing
    assert "104" in v["note"] and "reconcile_abandoned" in v["note"]
    assert v["day_start"] == datetime(2026, 10, 10, 22, 0, tzinfo=timezone.utc)   # Prague day in UTC


def test_verdict_before_the_smoke_rerun_names_the_hung_run(store):
    # 06:00Z: run 104 has been running for 7 h, run 105 does not exist yet in the window logic's eyes
    store.execute("DELETE FROM stage_runs WHERE run_id=105"); store.execute("DELETE FROM runs WHERE run_id=105")
    v = health.verdict(store, ANCHOR_NOW - timedelta(hours=2))
    assert v["ok"] is False and v["latest_run_id"] == 104
    assert "latest: running" in v["subject"]
    assert "no failed stage recorded" in v["body"]
    assert v["stale_run_ids"] == [104]


def test_verdict_with_no_run_today(store):
    v = health.verdict(store, ANCHOR_NOW + timedelta(days=3))
    assert v["ok"] is False and v["subject"] == "no pipeline run today"
    assert v["stale_run_ids"] == [104]


def test_verdict_ok_when_sent_and_nothing_stale(store):
    store.execute("UPDATE runs SET status='failed', finished_at=started_at WHERE run_id=104")
    v = health.verdict(store, datetime(2026, 10, 9, 8, 0, tzinfo=timezone.utc))     # the morning after run 102
    assert v["ok"] is True and v["latest_run_id"] == 102 and v["subject"] is None


@pytest.mark.anyio
async def test_pipeline_health_tool_uses_the_fixture_clock(client: Client):
    r = await client.call_tool("pipeline_health", {})
    assert r.is_error is False
    v = r.structured_content
    assert v["ok"] is False and v["stale_running"] == 1 and 104 in v["stale_run_ids"]
    assert v["body"] and v["subject"]


# ── pricing ──────────────────────────────────────────────────────────────────────────


def test_pricing_loads_and_resolves_aliases():
    p = cost.Pricing.load(PRICING)
    assert p.as_of == "2026-10-11"
    assert p.resolve("deepseek") == "deepseek-v3.2" and p.resolve("mystery-model") == "default"
    assert p.usd("deepseek-v3.2", 1_000_000, 0) == pytest.approx(0.27)
    assert p.usd("deepseek-v3.2", 0, 1_000_000) == pytest.approx(1.10)


@pytest.mark.parametrize("text, msg", [
    ("models:\n  default: {input_per_m: 1, output_per_m: 1}\n", "as_of"),
    ("as_of: x\nmodels:\n  foo: {input_per_m: 1, output_per_m: 1}\n", "default"),
    ("as_of: x\nmodels:\n  default: {input_per_m: 1}\n", "output_per_m"),
    ("as_of: x\nmodels:\n  default: {input_per_m: 1, output_per_m: 1}\naliases: {a: nope}\n", "unknown model"),
])
def test_pricing_validation(tmp_path, text, msg):
    f = tmp_path / "p.yaml"; f.write_text(text)
    with pytest.raises(cost.PricingError, match=msg):
        cost.Pricing.load(f)


# ── estimate ─────────────────────────────────────────────────────────────────────────


def test_gate_estimate_for_run_103_counts_batches_of_fifteen(store):
    p = cost.Pricing.load(PRICING)
    run = store.run_detail(103)
    fetchable = store.stage_inputs(103)["fetchable"]
    e = cost.estimate("gate", run, store, p, {"gate": "deepseek-v3.2"})
    batches = -(-fetchable // 15)
    assert e["tokens_in"] == batches * 3000 and e["tokens_out"] == batches * 600
    assert e["usd"] == pytest.approx(p.usd("deepseek-v3.2", e["tokens_in"], e["tokens_out"]))
    assert "re-judges" in e["basis"]


def test_downstream_estimates_project_when_gate_has_not_run(store):
    p = cost.Pricing.load(PRICING)
    run = store.run_detail(103)
    syn = cost.estimate("synthesis", run, store, p, {"synthesis": "deepseek-v3.2"})
    assert syn["tokens_in"] > 0 and "projected" in syn["basis"]
    run102 = store.run_detail(102)
    syn102 = cost.estimate("synthesis", run102, store, p, {"synthesis": "deepseek-v3.2"})
    assert "projected" not in syn102["basis"]
    assert syn102["tokens_in"] == store.stage_inputs(102)["clusters"] * 7500
    assert cost.estimate("extraction", run, store, p, {})["usd"] == 0.0


# ── report ───────────────────────────────────────────────────────────────────────────


def test_cost_report_103_is_partial_with_zero_spend(store):
    p = cost.Pricing.load(PRICING)
    r = cost.cost_report(store, store.run_detail(103), p)
    assert r["partial"] is True and r["estimated_usd"] == 0.0
    assert r["stages_without_usage"] == ["discovery", "discovery_registry", "extraction"]
    assert r["jobs_counted"] == []


def test_cost_report_sums_job_usage(store):
    p = cost.Pricing.load(PRICING)
    usage = cost.empty_usage()
    cost.add_usage(usage, "gate", "deepseek-v3.2", 30000, 6000, p)
    cost.add_usage(usage, "gate", "deepseek-v3.2", 30000, 6000, p)
    cost.add_usage(usage, "synthesis", "kimi-k2.6", 7500, 1200, p)
    now = datetime.now(timezone.utc)
    store.job_insert({"job_id": "j1", "kind": "resume_run", "run_id": 103, "stages_planned": ["gate", "cluster"],
                      "status": "completed", "usage": usage, "result": {"gate": {}, "cluster": {}},
                      "budget_usd": 2.0, "created_at": now, "updated_at": now, "finished_at": now, "trace": {}})
    store.complete_stage(103, "gate", {}); store.complete_stage(103, "cluster", {})
    r = cost.cost_report(store, store.run_detail(103), p)
    assert [s["stage"] for s in r["stages"]] == ["gate", "synthesis"]
    gate = r["stages"][0]["models"][0]
    assert gate["calls"] == 2 and gate["prompt_tokens"] == 60000
    assert r["estimated_usd"] == pytest.approx(p.usd("deepseek-v3.2", 60000, 12000) + p.usd("kimi-k2.6", 7500, 1200))
    assert r["partial"] is True and r["stages_without_usage"] == ["discovery", "discovery_registry", "extraction"]
    assert r["jobs_counted"] == ["j1"]


@pytest.mark.anyio
async def test_cost_report_tool(client: Client):
    r = await client.call_tool("cost_report", {"run_id": 103})
    assert r.is_error is False
    c = r.structured_content
    assert c["partial"] is True and c["estimated_usd"] == 0 and c["pricing_as_of"] == "2026-10-11"
