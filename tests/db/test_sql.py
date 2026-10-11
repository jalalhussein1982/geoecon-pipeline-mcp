"""PostgresStore reads against rows inserted with the pipeline's own Repo."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from geoecon_pipeline_mcp import health
from geoecon_pipeline_mcp.stages import plan_resume

pytestmark = pytest.mark.db


@pytest.fixture
def failed_run(repo):
    """A run that failed in gate after discovery/registry/extraction: 20 articles, 3 from the
    registry (two sources), 4 gate rejections of which 2 unparsed, nothing clustered."""
    now = datetime.now(timezone.utc)
    rid = repo.create_run(now - timedelta(hours=30), now - timedelta(hours=6), now - timedelta(hours=6), "deepseek-v3.2", "task_a_cov")
    for name in ("discovery", "discovery_registry", "extraction"):
        repo.start_stage(rid, name); repo.complete_stage(rid, name, {name: True})
    ids = []
    for i in range(20):
        reg = {0: "sa_pif", 1: "sa_pif", 2: "ae_mof"}.get(i)
        aid = repo.insert_article(rid, source_url=f"https://x.example/{rid}/{i}", url_hash=f"h{rid}-{i}", domain="x.example",
                                  title=f"title {i}", discovery_source="registry" if reg else "gdelt", registry_id=reg,
                                  source_tier=0 if reg else None)
        repo.update_extraction(aid, "body " * 100 if i % 5 else None, "extracted" if i % 5 else "failed")
        ids.append(aid)
    for aid, (geo, reason) in zip(ids[:6], [(True, "keep"), (True, "keep"), (False, "sports"), (False, "weather"),
                                           (False, "gate_unparsed_dropped"), (False, "gate_unparsed_dropped")]):
        repo.upsert_gate(aid, geoeconomic=geo, score=0.9 if geo else 0.1, country="Saudi Arabia" if geo else None,
                         reason=reason, country_fips="SA" if geo else None)
    repo.start_stage(rid, "gate")
    repo.fail_stage(rid, "gate", RuntimeError("NullGateError: gate kept 0 of 20 (db fixture)"))
    repo.set_run_status(rid, "failed")
    repo.merge_run_metrics(rid, {"extracted": 16, "failed": 4})
    return rid


@pytest.fixture
def completed_run(repo):
    now = datetime.now(timezone.utc)
    rid = repo.create_run(now - timedelta(hours=26), now - timedelta(hours=2), now - timedelta(hours=2), "deepseek-v3.2", "task_a_cov")
    for name in ("discovery", "discovery_registry", "extraction", "gate", "cluster", "synthesis", "summary", "render", "send"):
        repo.start_stage(rid, name); repo.complete_stage(rid, name, {})
    a1 = repo.insert_article(rid, source_url=f"https://y.example/{rid}/1", url_hash=f"c{rid}-1", title="t1")
    a2 = repo.insert_article(rid, source_url=f"https://y.example/{rid}/2", url_hash=f"c{rid}-2", title="t2")
    repo.update_extraction(a1, "body", "extracted"); repo.update_extraction(a2, "body", "extracted")
    repo.upsert_gate(a1, geoeconomic=True, score=0.8, country="Brazil", reason="keep", country_fips="BR")
    repo.upsert_gate(a2, geoeconomic=True, score=0.7, country="Brazil", reason="keep", country_fips="BR")
    c1 = repo.insert_cluster(rid, f"fp-{rid}-1", "LatAm", ["pt"], 1, country_fips="BR")
    c2 = repo.insert_cluster(rid, f"fp-{rid}-2", "LatAm", ["pt"], 1, country_fips="BR")
    repo.add_membership(c1, a1); repo.add_membership(c2, a2)
    repo.upsert_synthesis_record(rid, c1, {"headline_en": "ok record"}, "ok")
    repo.upsert_synthesis_record(rid, c2, {"headline_en": "held record"}, "empty_figures",
                                 quality_detail={"reasons": ["no figures"], "action": "hold"})
    nid = repo.insert_newsletter(rid, "en_only", "/data/archive/x.html", "<html>brief</html>", 1, bucket="latam")
    repo.update_newsletter_send(nid, "sent")
    repo.set_run_status(rid, "completed")
    return rid


def test_list_runs_and_run_detail(pg_store, failed_run, completed_run):
    cards = {c["run_id"]: c for c in pg_store.list_runs(None, None, 50)}
    assert cards[failed_run]["status"] == "failed" and cards[failed_run]["last_failed_stage"] == "gate"
    assert cards[failed_run]["stages_done"] == 3 and cards[failed_run]["stages_total"] == 9
    assert cards[completed_run]["stages_done"] == cards[completed_run]["stages_total"] == 9
    assert [c["run_id"] for c in pg_store.list_runs("failed", None, 50) if c["run_id"] == failed_run] == [failed_run]
    d = pg_store.run_detail(failed_run)
    assert d["next_stage"] == "gate" and d["completed_stages"] == ["discovery", "discovery_registry", "extraction"]
    gate = next(s for s in d["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "failed" and "NullGateError" in gate["error"] and gate["finished_at"].tzinfo is not None
    assert d["metrics"] == {"extracted": 16, "failed": 4}
    assert pg_store.run_detail(completed_run)["next_stage"] is None
    assert pg_store.run_detail(99_999_999) is None
    assert pg_store.latest_run_id() >= completed_run


def test_held_gate_sources_and_inputs(pg_store, failed_run, completed_run):
    held = pg_store.held_records(completed_run, None)
    assert len(held) == 1 and held[0]["flag"] == "empty_figures" and held[0]["reasons"] == ["no figures"]
    assert held[0]["action"] == "hold" and held[0]["article_count"] == 1 and held[0]["headline"] == "held record"
    assert pg_store.held_records(completed_run, "zero_entities") == []
    page, cursor = pg_store.gate_rejections(failed_run, 3, None)
    assert len(page) == 3 and cursor is not None
    page2, cursor2 = pg_store.gate_rejections(failed_run, 3, cursor)
    assert len(page2) == 1 and cursor2 is None and page2[0]["unparsed"] is True
    assert pg_store.gate_drop_counts(failed_run) == (4, 2)
    assert pg_store.source_counts(failed_run) == {"sa_pif": 2, "ae_mof": 1}
    inputs = pg_store.stage_inputs(failed_run)
    assert inputs["articles"] == 20 and inputs["fetchable"] == 16 and inputs["kept"] == 2 and inputs["gated"] == 6
    assert inputs["clusters"] == 0 and inputs["countries"] == 1
    assert pg_store.stage_inputs(completed_run)["clusters"] == 2
    assert pg_store.funnel(completed_run)["records_held"] == 1


def test_newsletters_watchdog_and_health(pg_store, completed_run):
    nls = pg_store.newsletters(completed_run)
    assert nls[0]["bucket"] == "latam" and nls[0]["send_status"] == "sent" and nls[0]["sent_at"] is not None
    now = datetime.now(timezone.utc)
    latest = pg_store.latest_run_for_window(now - timedelta(days=2), now + timedelta(hours=1))
    assert latest["run_id"] >= completed_run
    v = health.verdict(pg_store, now)
    assert set(v) >= {"ok", "subject", "stale_running", "stale_run_ids", "latest_run_id"}


def test_stage_bookkeeping_goes_through_the_repo(pg_store, failed_run):
    plan = plan_resume(pg_store.run_detail(failed_run))
    assert plan.remaining == ["gate", "cluster", "synthesis", "summary", "render"] and plan.send_withheld
    pg_store.start_stage(failed_run, "gate")
    assert next(s for s in pg_store.run_detail(failed_run)["stages"] if s["stage_name"] == "gate")["status"] == "running"
    pg_store.complete_stage(failed_run, "gate", {"gate_kept": 2})
    d = pg_store.run_detail(failed_run)
    gate = next(s for s in d["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "completed" and gate["checkpoint"] == {"gate_kept": 2} and d["next_stage"] == "cluster"
    pg_store.append_run_note(failed_run, "resumed via MCP")
    pg_store.append_run_note(failed_run, "send withheld")
    assert pg_store.run_detail(failed_run)["notes"] == "resumed via MCP | send withheld"
    pg_store.set_run_status(failed_run, "partial")
    assert pg_store.run_detail(failed_run)["status"] == "partial"
    assert pg_store.failed_stages(failed_run) == []


def test_stale_runs_and_mark_run_failed(pg_store, repo):
    now = datetime.now(timezone.utc)
    rid = repo.create_run(now - timedelta(hours=33), now - timedelta(hours=9), now - timedelta(hours=9), "m", "p")
    pg_store.conn.execute("UPDATE runs SET started_at = %s WHERE run_id = %s", (now - timedelta(hours=9), rid))
    repo.start_stage(rid, "gate")
    stale = pg_store.stale_running_runs(now - timedelta(hours=6))
    assert rid in [r["run_id"] for r in stale]
    pg_store.mark_run_failed(rid, "abandoned (db test)")
    d = pg_store.run_detail(rid)
    assert d["status"] == "failed" and "abandoned" in d["notes"]
    assert next(s for s in d["stages"] if s["stage_name"] == "gate")["status"] == "failed"
