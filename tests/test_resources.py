"""Resources: URIs, mime types, cache hints, not-found behaviour."""

from __future__ import annotations

import json

import pytest

from .conftest import call, poll


@pytest.mark.anyio
async def test_resource_templates_are_listed(client):
    t = {x.uri_template for x in (await client.list_resource_templates()).resource_templates}
    assert t == {"run://{run_id}", "run://{run_id}/held", "run://{run_id}/newsletter/{bucket}",
                 "run://{run_id}/archive/{bucket}", "job://{job_id}"}
    r = {str(x.uri) for x in (await client.list_resources()).resources}
    assert r == {"geoecon://sources", "geoecon://pricing", "geoecon://incidents/template"}


@pytest.mark.anyio
async def test_run_resource_is_the_run_detail(client):
    res = await client.read_resource("run://103")
    doc = json.loads(res.contents[0].text)
    assert doc["run_id"] == 103 and doc["next_stage"] == "gate"
    assert res.contents[0].mime_type == "application/json"
    assert res.ttl_ms == 0 and res.cache_scope == "private"


@pytest.mark.anyio
async def test_run_resource_reflects_a_completed_stage(client):
    h = await call(client, "start_job", kind="run_stage", run_id=103, stage="gate")
    await poll(client, h["job_id"])
    doc = json.loads((await client.read_resource("run://103")).contents[0].text)
    gate = next(s for s in doc["stages"] if s["stage_name"] == "gate")
    assert gate["status"] == "completed" and doc["next_stage"] == "cluster"
    job = json.loads((await client.read_resource(f"job://{h['job_id']}")).contents[0].text)
    assert job["status"] == "completed" and job["stages_completed"] == ["gate"]


@pytest.mark.anyio
async def test_held_and_newsletter_resources(client):
    held = json.loads((await client.read_resource("run://102/held")).contents[0].text)
    assert len(held) == 3 and {h["flag"] for h in held} == {"empty_figures", "zero_entities", "invalid_json"}
    nl = await client.read_resource("run://102/newsletter/latam")
    assert nl.contents[0].mime_type == "text/html" and "<h1>" in nl.contents[0].text
    assert nl.ttl_ms == 86_400_000 and nl.cache_scope == "private"


@pytest.mark.anyio
async def test_archive_explains_where_it_lives_when_unreadable(client):
    with pytest.raises(Exception) as ei:
        await client.read_resource("run://102/archive/latam")
    assert "geoecon-archive" in str(ei.value) and "/data/archive/" in str(ei.value)


@pytest.mark.anyio
async def test_public_cache_scope_on_pricing_and_template(client):
    p = await client.read_resource("geoecon://pricing")
    assert p.ttl_ms == 3_600_000 and p.cache_scope == "public" and "as_of" in p.contents[0].text
    t = await client.read_resource("geoecon://incidents/template")
    assert t.cache_scope == "public" and "## 3. Fixes in priority order" in t.contents[0].text
    s = await client.read_resource("geoecon://sources")
    assert s.cache_scope == "private" and s.ttl_ms == 3_600_000
    assert len(json.loads(s.contents[0].text)) == 66


@pytest.mark.anyio
async def test_not_found_resources(client):
    for uri in ("run://999", "run://102/newsletter/mars", "job://nope", "run://abc"):
        with pytest.raises(Exception):
            await client.read_resource(uri)
