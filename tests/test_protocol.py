"""Protocol-level facts: tool order, output schemas, annotations, error policy."""

from __future__ import annotations

import pytest

from mcp import Client

TOOL_ORDER = [
    "list_runs", "run_status", "failed_stage_error", "list_held_records", "list_gate_rejections",
    "source_health", "pipeline_health", "cost_report",
    "start_job", "job_status", "answer_job", "cancel_job", "preview_resume",
]
READ_ONLY = {"list_runs", "run_status", "failed_stage_error", "list_held_records", "list_gate_rejections",
             "source_health", "pipeline_health", "cost_report", "job_status", "preview_resume"}


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


@pytest.mark.anyio
async def test_tools_are_listed_in_spec_order(client: Client):
    names = [t.name for t in (await client.list_tools()).tools]
    assert names == TOOL_ORDER


@pytest.mark.anyio
async def test_every_tool_has_an_output_schema_and_a_title(client: Client):
    for t in (await client.list_tools()).tools:
        assert t.output_schema, t.name
        assert t.title, t.name
        assert t.description and len(t.description) > 40, t.name


@pytest.mark.anyio
async def test_annotations_follow_the_spec(client: Client):
    tools = {t.name: t for t in (await client.list_tools()).tools}
    for name in READ_ONLY:
        assert tools[name].annotations.read_only_hint is True, name
    sj = tools["start_job"].annotations
    assert sj.read_only_hint is False and sj.destructive_hint is False
    assert sj.idempotent_hint is False and sj.open_world_hint is True
    assert tools["answer_job"].annotations.idempotent_hint is True
    assert tools["cancel_job"].annotations.idempotent_hint is True


@pytest.mark.anyio
async def test_protocol_version_is_2026_07_28(client: Client):
    assert client.protocol_version == "2026-07-28"


@pytest.mark.anyio
async def test_unknown_run_is_a_tool_error_naming_list_runs(client: Client):
    r = await client.call_tool("run_status", {"run_id": 999})
    assert r.is_error is True
    assert "list_runs" in r.content[0].text and "999" in r.content[0].text


@pytest.mark.anyio
async def test_input_validation_rejects_out_of_range_limit(client: Client):
    r = await client.call_tool("list_runs", {"limit": 500})
    assert r.is_error is True


@pytest.mark.anyio
async def test_server_instructions_point_at_the_entry_tools(client: Client):
    assert client.instructions and "list_runs" in client.instructions and "preview_resume" in client.instructions
