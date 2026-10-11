"""Inspection tools over the synthetic history: the numbers an operator would read."""

from __future__ import annotations

import pytest

from mcp import Client


@pytest.fixture
def server(synthetic_db):
    from geoecon_pipeline_mcp.server import Settings, create_server

    app = create_server(Settings(mode="synthetic", synthetic_db=synthetic_db))
    yield app.mcp
    app.close()


@pytest.fixture
async def client(server):
    async with Client(server, raise_exceptions=True) as c:
        yield c


async def call(client: Client, name: str, **args):
    r = await client.call_tool(name, args)
    assert r.is_error is False, r.content[0].text if r.content else r
    return r.structured_content


@pytest.mark.anyio
async def test_list_runs_cards_newest_first(client: Client):
    runs = (await call(client, "list_runs"))["result"]
    assert [r["run_id"] for r in runs] == [105, 104, 103, 102, 101]
    r103 = next(r for r in runs if r["run_id"] == 103)
    assert r103["status"] == "failed" and r103["last_failed_stage"] == "gate"
    assert r103["stages_done"] == 3 and r103["stages_total"] == 9
    failed = (await call(client, "list_runs", status="failed"))["result"]
    assert [r["run_id"] for r in failed] == [103]


@pytest.mark.anyio
async def test_run_status_103_names_gate_as_next_stage(client: Client):
    d = await call(client, "run_status", run_id=103)
    assert d["next_stage"] == "gate"
    gate = next(s for s in d["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "failed" and "NullGateError" in gate["error"]
    assert [s["stage_name"] for s in d["stages"]] == ["discovery", "discovery_registry", "extraction", "gate"]
    assert d["active_job_id"] is None
    assert d["metrics"]["extracted"] > 0


@pytest.mark.anyio
async def test_failed_stage_error_returns_the_stored_text(client: Client):
    e = await call(client, "failed_stage_error", run_id=103)
    assert e["stage_name"] == "gate" and "NullGateError" in e["error"] and "0 of 228" in e["error"]
    assert e["duration_s"] == 2900.0
    r = await client.call_tool("failed_stage_error", {"run_id": 102})
    assert r.is_error is True and "no failed stage" in r.content[0].text


@pytest.mark.anyio
async def test_list_held_records_102(client: Client):
    held = (await call(client, "list_held_records", run_id=102))["result"]
    assert len(held) == 3
    assert sorted(h["flag"] for h in held) == ["empty_figures", "invalid_json", "zero_entities"]
    assert all(h["reasons"] for h in held)
    only = (await call(client, "list_held_records", run_id=102, flag="zero_entities"))["result"]
    assert len(only) == 1 and only[0]["flag"] == "zero_entities"
    r = await client.call_tool("list_held_records", {"run_id": 102, "flag": "bogus"})
    assert r.is_error is True


@pytest.mark.anyio
async def test_list_gate_rejections_paginates(client: Client):
    p1 = await call(client, "list_gate_rejections", run_id=102, limit=5)
    assert len(p1["items"]) == 5 and p1["next_cursor"]
    p2 = await call(client, "list_gate_rejections", run_id=102, limit=5, cursor=p1["next_cursor"])
    assert p2["items"][0]["article_id"] > p1["items"][-1]["article_id"]
    assert p1["total_dropped"] == p2["total_dropped"] > 100
    assert 0 < p1["unparsed_dropped"] < p1["total_dropped"]
    p103 = await call(client, "list_gate_rejections", run_id=103, limit=3)
    assert p103["unparsed_dropped"] == p103["total_dropped"] and all(i["unparsed"] for i in p103["items"])


@pytest.mark.anyio
async def test_source_health_marks_zero_row_live_sources_silent(client: Client):
    rows = (await call(client, "source_health", run_id=102))["result"]
    assert len(rows) == 66
    by_id = {r["registry_id"]: r for r in rows}
    assert by_id["ae_mof"]["state"] == "silent" and by_id["ae_mof"]["articles"] == 0
    assert by_id["co_cb_banrep"]["state"] == "silent"
    assert by_id["sa_pif"]["state"] == "healthy" and by_id["sa_pif"]["articles"] > 0
    assert by_id["sa_cb_sama"]["state"] == "held"                      # needs_review never fetches
    assert by_id["wire_ap"]["state"] == "retired"
    assert sum(1 for r in rows if r["state"] == "silent") == 2


@pytest.mark.anyio
async def test_source_health_without_registry_stage_is_all_unknown(client: Client):
    rows = (await call(client, "source_health", run_id=101))["result"]      # Review Focus 5
    assert len(rows) == 66
    assert {r["state"] for r in rows} == {"unknown"}
    assert all(r["articles"] is None for r in rows)


@pytest.mark.anyio
async def test_source_health_defaults_to_the_latest_run(client: Client):
    rows = (await call(client, "source_health"))["result"]
    assert len(rows) == 66 and {r["state"] for r in rows} != {"unknown"}


@pytest.mark.anyio
async def test_run_resource_reads_for_unknown_run_fail_cleanly(client: Client):
    r = await client.call_tool("list_held_records", {"run_id": 4242})
    assert r.is_error is True and "list_runs" in r.content[0].text
