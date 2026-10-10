"""Resume planning: the right stages, the original window, the send guard."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from geoecon_pipeline_mcp import stages

from .conftest import call, poll


def test_run_103_resumes_at_gate_with_everything_after(store):
    plan = stages.plan_resume(store.run_detail(103))
    assert plan.remaining == ["gate", "cluster", "synthesis", "summary", "render"]        # Review Focus 1
    assert plan.send_withheld is True
    assert plan.skipped_completed == ["discovery", "discovery_registry", "extraction"]
    assert plan.window_end - plan.window_start == plan.window_end - plan.window_start      # original window kept
    assert plan.window_end == datetime(2026, 10, 9, 23, 0, tzinfo=timezone.utc)


def test_run_101_has_nothing_to_resume(store):
    plan = stages.plan_resume(store.run_detail(101))
    assert plan.remaining == [] and plan.nothing_to_resume


def test_send_included_only_when_allowed(store):
    assert stages.plan_resume(store.run_detail(103), allow_send=True).remaining[-1] == "send"
    plan = stages.plan_resume(store.run_detail(104))                                       # gate still `running`
    assert plan.remaining[0] == "gate" and "send" not in plan.remaining


def test_available_names_restrict_the_plan(store):
    plan = stages.plan_resume(store.run_detail(103), available=["gate", "cluster", "render"])
    assert plan.remaining == ["gate", "cluster", "render"]


def _fake_modules(calls):
    def mod(name):
        def run(cfg, repo, rid, *args, **kw):
            calls.append((name, args, kw))
            return {name: 1}
        return SimpleNamespace(run=run)

    class Send:
        @staticmethod
        def send_pending(cfg, repo, newsletters, subject_for):
            calls.append(("send_pending", [subject_for(n["bucket"]) for n in newsletters], {}))
            return {"all_sent": True, "results": [[n["bucket"], "sent"] for n in newsletters]}

    render = SimpleNamespace(run=lambda cfg, repo, rid, date_str, *, stats=None: (calls.append(("render", (date_str,), {"stats": stats}))
                                                                                  or {"record_count": 5, "newsletters": 2}))
    return SimpleNamespace(discovery=mod("discovery"), discovery_registry=mod("discovery_registry"),
                           discovery_search=mod("discovery_search"), extract=mod("extraction"), gate=mod("gate"),
                           cluster=mod("cluster"), synth=mod("synthesis"), summary=mod("summary"), render=render,
                           send=Send, regions=SimpleNamespace(BUCKET_DISPLAY={"latam": "Latin America"}))


def test_pipeline_wrappers_pass_the_original_window(store):
    calls = []
    run = store.run_detail(103)
    repo = SimpleNamespace(newsletters_for_run=lambda rid: [{"bucket": "latam", "newsletter_id": 1, "email_html": "<p>", "send_status": "pending"}],
                           mark_emitted=lambda rid: calls.append(("mark_emitted", (rid,), {})))
    cfg = SimpleNamespace(dry_run=False)
    fns = stages.make_pipeline_stage_fns(cfg, repo, run, client="CLIENT", modules=_fake_modules(calls),
                                         names=["discovery", "discovery_registry", "extraction", "gate", "cluster",
                                                "synthesis", "summary", "render", "send"])
    assert "discovery_search" not in fns                                 # not in this pipeline's default stages
    report, cancel = lambda *a: None, None
    fns["discovery"](run, report, cancel); fns["discovery_registry"](run, report, cancel)
    assert calls[0] == ("discovery", (), {"now": run["window_end"]})
    assert calls[1] == ("discovery_registry", (), {"now": run["window_end"]})
    fns["gate"](run, report, cancel)
    assert calls[2] == ("gate", ("CLIENT",), {})
    out = fns["render"](run, report, cancel)
    assert calls[3] == ("render", ("2026-10-09",), {"stats": None}) and out == {"record_count": 5, "newsletters": 2}
    out = fns["send"](run, report, cancel)
    assert out["send_status"] == "sent"
    assert calls[4] == ("send_pending", ["Geoeconomic Brief — Latin America — 2026-10-09"], {})
    assert calls[5] == ("mark_emitted", (103,), {})


def test_send_wrapper_honours_dry_run_and_empty(store):
    calls = []
    run = store.run_detail(103)
    repo = SimpleNamespace(newsletters_for_run=lambda rid: [])
    fns = stages.make_pipeline_stage_fns(SimpleNamespace(dry_run=True), repo, run, None, modules=_fake_modules(calls), names=["send"])
    assert fns["send"](run, None, None) == {"send_status": "skipped"}
    fns = stages.make_pipeline_stage_fns(SimpleNamespace(dry_run=False), repo, run, None, modules=_fake_modules(calls), names=["send"])
    assert fns["send"](run, None, None) == {"send_status": "skipped_empty"}


def test_default_stage_names_come_from_the_real_orchestrator():
    from geoecon.config import Config
    cfg = Config.from_env({"DB_DSN": "x", "MAETCENTRUM_KEY": "k", "SMTP_HOST": "h", "SMTP_USER": "u",
                           "SMTP_PASSWORD": "p", "FROM_ADDR": "f", "RECIPIENTS": "a@b"})
    names = stages.default_stage_names(cfg, client=None)
    assert names == ["discovery", "discovery_registry", "extraction", "gate", "cluster", "synthesis", "summary", "render", "send"]
    fns = stages.make_pipeline_stage_fns(cfg, repo=None, run={"run_id": 1, "window_end": datetime(2026, 10, 9, tzinfo=timezone.utc)},
                                         client=None)
    assert list(fns) == names                                            # real modules import; nothing executed


# ── tools ──────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_preview_resume_103(client):
    p = await call(client, "preview_resume", run_id=103)
    assert p["remaining"] == ["gate", "cluster", "synthesis", "summary", "render"]
    assert p["send_withheld"] is True and p["skipped_completed"] == ["discovery", "discovery_registry", "extraction"]
    assert p["window_end"].startswith("2026-10-09T23:00:00")
    assert [e["stage"] for e in p["estimates"]] == ["gate", "cluster", "synthesis", "summary"]
    assert p["estimated_usd"] > 0 and p["default_budget_usd"] == 2.0 and p["would_require_approval"] is False
    assert "gate" in p["message"] and "2026-10-09" in p["message"]


@pytest.mark.anyio
async def test_preview_resume_of_a_complete_run_says_so(client):
    p = await call(client, "preview_resume", run_id=101)
    assert p["remaining"] == [] and "nothing to resume" in p["message"] and p["estimated_usd"] == 0


@pytest.mark.anyio
async def test_resume_run_103_runs_every_remaining_stage_and_stops_before_send(client, app):
    h = await call(client, "start_job", kind="resume_run", run_id=103)
    assert h["stages_planned"] == ["gate", "cluster", "synthesis", "summary", "render"]
    assert "send withheld" in h["note"]
    job = await poll(client, h["job_id"], timeout=30)
    assert job["status"] == "completed", job
    assert job["stages_completed"] == ["gate", "cluster", "synthesis", "summary", "render"]
    assert job["usage"]["usd_total"] > 0 and set(job["usage"]["stages"]) == {"gate", "cluster", "synthesis", "summary"}
    d = await call(client, "run_status", run_id=103)
    assert d["status"] == "partial" and d["next_stage"] == "send"
    assert "send withheld" in d["notes"]
    assert {s["status"] for s in d["stages"]} == {"completed"}
    assert d["metrics"]["records_total"] > 0 and d["metrics"]["newsletters"] >= 1
    cr = await call(client, "cost_report", run_id=103)
    assert cr["partial"] is True and cr["stages_without_usage"] == ["discovery", "discovery_registry", "extraction"]
    assert cr["estimated_usd"] == pytest.approx(job["usage"]["usd_total"], rel=1e-6)
    held = await call(client, "list_held_records", run_id=103)
    assert isinstance(held["result"], list)
