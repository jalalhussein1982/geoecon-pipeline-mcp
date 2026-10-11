"""The synthetic history is the evidence every other test reads. Pin its shape."""

from __future__ import annotations

from datetime import timedelta

from geoecon_pipeline_mcp.store import SyntheticStore, next_stage_of
from geoecon_pipeline_mcp.synth import ANCHOR_NOW, build_synthetic, canonical_dump, fetching_sources, load_registry

from .conftest import FIXTURE_DB


def test_list_runs_returns_five_cards_newest_first(store):
    cards = store.list_runs(None, None, 50)
    assert [c["run_id"] for c in cards] == [105, 104, 103, 102, 101]
    by_id = {c["run_id"]: c for c in cards}
    assert by_id[101]["status"] == "completed" and by_id[101]["stages_done"] == by_id[101]["stages_total"] == 8
    assert by_id[102]["status"] == "completed" and by_id[102]["stages_done"] == 9
    assert by_id[103]["status"] == "failed" and by_id[103]["last_failed_stage"] == "gate"
    assert by_id[103]["stages_done"] == 3 and by_id[103]["stages_total"] == 9
    assert by_id[104]["status"] == "running"
    assert by_id[105]["status"] == "completed"


def test_list_runs_filters_by_status_and_since(store):
    assert [c["run_id"] for c in store.list_runs("failed", None, 50)] == [103]
    since = ANCHOR_NOW - timedelta(hours=12)
    assert [c["run_id"] for c in store.list_runs(None, since, 50)] == [105, 104]
    assert len(store.list_runs(None, None, 2)) == 2


def test_run_103_failed_in_gate_and_resumes_at_gate(store):
    d = store.run_detail(103)
    assert d["status"] == "failed"
    gate = next(s for s in d["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "failed"
    assert "NullGateError" in gate["error"] and "0 of 228" in gate["error"]
    assert d["next_stage"] == "gate"                       # Review Focus 1: failed stage is re-run, not skipped
    assert d["completed_stages"] == ["discovery", "discovery_registry", "extraction"]
    assert d["window_end"] - d["window_start"] == timedelta(hours=24)


def test_next_stage_logic_handles_running_and_legacy_rows():
    rows = [{"stage_name": "discovery", "status": "completed"}, {"stage_name": "extraction", "status": "running"}]
    assert next_stage_of(rows) == "discovery_registry"
    rows = [{"stage_name": n, "status": "completed"} for n in
            ("discovery", "discovery_registry", "extraction", "gate", "cluster", "synthesis", "summary", "render", "send")]
    assert next_stage_of(rows) is None


def test_run_104_is_running_and_stale(store):
    d = store.run_detail(104)
    assert d["status"] == "running" and d["next_stage"] == "gate"
    assert ANCHOR_NOW - d["started_at"] >= timedelta(hours=8, minutes=59)
    stale = store.stale_running_runs(ANCHOR_NOW - timedelta(hours=6))
    assert [r["run_id"] for r in stale] == [104]


def test_run_105_completed_without_sending(store):
    d = store.run_detail(105)
    assert d["status"] == "completed" and d["metrics"]["send_status"] == "skipped_empty"
    assert store.newsletters(105) == []
    assert store.stage_inputs(105)["kept"] == 0


def test_held_records_102_has_three_with_flags(store):
    held = store.held_records(102, None)
    assert sorted(h["flag"] for h in held) == ["empty_figures", "invalid_json", "zero_entities"]
    assert all(h["reasons"] and h["action"] for h in held)
    assert [h["flag"] for h in store.held_records(102, "invalid_json")] == ["invalid_json"]
    assert len(store.held_records(101, None)) == 1
    assert store.held_records(103, None) == []


def test_gate_rejections_paginate_and_count_unparsed(store):
    page1, cursor = store.gate_rejections(102, 5, None)
    assert len(page1) == 5 and cursor is not None
    page2, _ = store.gate_rejections(102, 5, cursor)
    assert page2 and page2[0]["article_id"] > page1[-1]["article_id"]
    total, unparsed = store.gate_drop_counts(102)
    assert total > 100 and 0 < unparsed < total
    total103, unparsed103 = store.gate_drop_counts(103)
    assert total103 == unparsed103 > 0                      # every verdict unparsable in the failed run


def test_source_counts_mirror_the_registry(store):
    sources = load_registry()
    assert len(sources) == 66
    fetching = {s["id"] for s in fetching_sources(sources)}
    counts = store.source_counts(102)
    assert set(counts) <= fetching
    live = {s["id"] for s in sources if s.get("status") == "live"}
    zero_live = live - set(counts)
    assert zero_live == {"ae_mof", "co_cb_banrep"}
    assert store.source_counts(101) == {}                  # older run: GDELT only, no registry stage


def test_newsletters_sent_for_completed_runs(store):
    nls = store.newsletters(102)
    assert [n["bucket"] for n in nls] == ["latam", "middle_east", "north_africa"]
    assert all(n["send_status"] == "sent" and n["email_html"].startswith("<!doctype html>") for n in nls)


def test_latest_run_for_window_matches_the_watchdog_query(store):
    day_start = ANCHOR_NOW.replace(hour=22, minute=0) - timedelta(days=1)   # Prague day = [22:00Z, 22:00Z)
    r = store.latest_run_for_window(day_start, day_start + timedelta(days=1))
    assert r["run_id"] == 105
    assert store.failed_stages(103)[0]["stage_name"] == "gate"


def test_build_is_deterministic(tmp_path):
    a = build_synthetic(tmp_path / "a.db")
    b = build_synthetic(tmp_path / "b.db")
    assert canonical_dump(a) == canonical_dump(b)
    a.close(); b.close()


def test_committed_fixture_matches_a_fresh_build(tmp_path):
    assert FIXTURE_DB.exists(), "run `make fixtures`"
    fresh = build_synthetic(tmp_path / "fresh.db")
    committed = SyntheticStore(FIXTURE_DB)
    try:
        assert canonical_dump(committed) == canonical_dump(fresh)
    finally:
        fresh.close(); committed.close()


def test_now_is_the_anchor_so_tests_do_not_depend_on_wall_clock(store):
    assert store.now() == ANCHOR_NOW
