"""Prompts name the tools they depend on; trace context from _meta lands on the job."""

from __future__ import annotations

import pytest

from .conftest import call, poll


@pytest.mark.anyio
async def test_prompts_are_listed_and_mention_the_tools(client):
    names = {p.name for p in (await client.list_prompts()).prompts}
    assert names == {"write_incident_report", "morning_check"}
    p = await client.get_prompt("write_incident_report", {"run_id": "103"})
    text = p.messages[0].content.text
    for tool in ("run_status(103)", "failed_stage_error(103)", "list_held_records(103)", "pipeline_health()", "cost_report(103)"):
        assert tool in text
    assert "geoecon://incidents/template" in text and "P0" in text
    m = await client.get_prompt("morning_check", {})
    text = m.messages[0].content.text
    for tool in ("pipeline_health", "list_runs", "run_status", "source_health", "list_held_records", "preview_resume",
                 "reconcile_abandoned", "write_incident_report"):
        assert tool in text


@pytest.mark.anyio
async def test_incident_prompt_for_unknown_run_fails_cleanly(client):
    with pytest.raises(Exception):
        await client.get_prompt("write_incident_report", {"run_id": "999"})


@pytest.mark.anyio
async def test_trace_context_from_meta_is_stored_on_the_job(client):
    meta = {"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "tracestate": "congo=t61rcWkgMzE",
            "baggage": "operator=ops@example", "unrelated": "dropped"}
    r = await client.call_tool("start_job", {"kind": "run_stage", "run_id": 105, "stage": "render", "force": True}, meta=meta)
    assert r.is_error is False
    job = await poll(client, r.structured_content["job_id"])
    assert job["trace"]["traceparent"] == meta["traceparent"]
    assert job["trace"]["tracestate"] == meta["tracestate"] and job["trace"]["baggage"] == meta["baggage"]
    assert "unrelated" not in job["trace"]
    plain = await call(client, "start_job", kind="run_stage", run_id=105, stage="render", force=True)
    job = await poll(client, plain["job_id"])
    assert job["trace"] == {}
