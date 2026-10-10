"""The send stage never runs unless ALLOW_SEND=1 and the caller holds pipeline:send."""

from __future__ import annotations

import pytest

from mcp import Client

from geoecon_pipeline_mcp.server import Settings, create_server

from .conftest import call, poll


async def _with(settings):
    app = create_server(settings)
    return app, Client(app.mcp, raise_exceptions=True)


@pytest.mark.anyio
async def test_send_dropped_by_default_and_run_status_says_so(synthetic_db):
    app, c = await _with(Settings(mode="synthetic", synthetic_db=synthetic_db, fake_unit_sleep=0.0))
    try:
        async with c as client:
            p = await call(client, "preview_resume", run_id=103)
            assert "send" not in p["remaining"] and p["send_withheld"] is True
            h = await call(client, "start_job", kind="resume_run", run_id=103)
            assert "send" not in h["stages_planned"] and "send withheld" in h["note"]
            job = await poll(client, h["job_id"], timeout=30)
            assert job["status"] == "completed"
            d = await call(client, "run_status", run_id=103)
            assert d["status"] == "partial" and d["next_stage"] == "send" and "send withheld" in d["notes"]
            assert all(n["send_status"] == "pending" for n in app.store.newsletters(103))
            r = await client.call_tool("start_job", {"kind": "run_stage", "run_id": 103, "stage": "send"})
            assert r.is_error is True and "ALLOW_SEND" in r.content[0].text
    finally:
        app.close()


@pytest.mark.anyio
async def test_allow_send_without_the_scope_is_refused(synthetic_db):
    app, c = await _with(Settings(mode="synthetic", synthetic_db=synthetic_db, fake_unit_sleep=0.0, allow_send=True,
                                  stdio_scopes=("pipeline:read", "pipeline:operate")))
    try:
        async with c as client:
            p = await call(client, "preview_resume", run_id=103)
            assert p["send_withheld"] is True and "pipeline:send" in p["message"]
            r = await client.call_tool("start_job", {"kind": "run_stage", "run_id": 103, "stage": "send", "force": True})
            assert r.is_error is True and "pipeline:send" in r.content[0].text
            h = await call(client, "start_job", kind="resume_run", run_id=103)
            assert "send" not in h["stages_planned"]
            await poll(client, h["job_id"], timeout=30)
    finally:
        app.close()


@pytest.mark.anyio
async def test_both_present_includes_send(synthetic_db):
    app, c = await _with(Settings(mode="synthetic", synthetic_db=synthetic_db, fake_unit_sleep=0.0, allow_send=True,
                                  stdio_scopes=("pipeline:read", "pipeline:operate", "pipeline:send")))
    try:
        async with c as client:
            p = await call(client, "preview_resume", run_id=103)
            assert p["remaining"][-1] == "send" and p["send_withheld"] is False
            h = await call(client, "start_job", kind="resume_run", run_id=103)
            assert h["stages_planned"][-1] == "send" and h["note"] is None
            job = await poll(client, h["job_id"], timeout=30)
            assert job["status"] == "completed" and job["result"]["send"]["send_status"] == "sent"
            d = await call(client, "run_status", run_id=103)
            assert d["status"] == "completed" and d["next_stage"] is None
            assert all(n["send_status"] == "sent" for n in app.store.newsletters(103))
    finally:
        app.close()


@pytest.mark.anyio
async def test_operate_scope_is_required_for_writes(synthetic_db):
    app, c = await _with(Settings(mode="synthetic", synthetic_db=synthetic_db, stdio_scopes=("pipeline:read",)))
    try:
        async with c as client:
            assert (await call(client, "list_runs"))["result"]
            for name, args in (("start_job", {"kind": "reconcile_abandoned"}), ("cancel_job", {"job_id": "x"}),
                               ("answer_job", {"job_id": "x", "approve": True})):
                r = await client.call_tool(name, args)
                assert r.is_error is True and "pipeline:operate" in r.content[0].text, name
    finally:
        app.close()
