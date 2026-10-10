"""Shared fixtures: a fresh synthetic store per test, built into tmp_path so tests never
touch the committed fixture, and an anyio backend pinned to asyncio."""

from __future__ import annotations

from pathlib import Path

import pytest

from geoecon_pipeline_mcp.synth import ANCHOR_NOW, build_synthetic

ROOT = Path(__file__).resolve().parent.parent
FIXTURE_DB = ROOT / "fixtures" / "synthetic.db"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def synthetic_db(tmp_path) -> Path:
    path = tmp_path / "synthetic.db"
    build_synthetic(path).close()
    return path


@pytest.fixture
def store(synthetic_db):
    from geoecon_pipeline_mcp.store import SyntheticStore

    s = SyntheticStore(synthetic_db)
    yield s
    s.close()


@pytest.fixture
def anchor():
    return ANCHOR_NOW


# ── server + client fixtures with fast fake stages ──────────────────────────────────


@pytest.fixture
def fast_settings(synthetic_db):
    from geoecon_pipeline_mcp.server import Settings

    return Settings(mode="synthetic", synthetic_db=synthetic_db, fake_unit_sleep=0.002)


@pytest.fixture
def app(fast_settings):
    from geoecon_pipeline_mcp.server import create_server

    a = create_server(fast_settings)
    yield a
    a.close()


@pytest.fixture
async def client(app):
    from mcp import Client

    async with Client(app.mcp, raise_exceptions=True) as c:
        yield c


async def call(client, name: str, **args):
    """Call a tool and return structured content; fail loudly on a ToolError."""
    r = await client.call_tool(name, args)
    assert r.is_error is False, r.content[0].text if r.content else r
    return r.structured_content


async def poll(client, job_id: str, until=("completed", "failed", "cancelled"), timeout: float = 15.0):
    """Poll job_status until the status is in `until`; return the last Job."""
    import anyio

    deadline = anyio.current_time() + timeout
    while True:
        job = await call(client, "job_status", job_id=job_id)
        if job["status"] in until:
            return job
        if anyio.current_time() > deadline:
            raise AssertionError(f"job {job_id} still {job['status']} after {timeout}s: {job}")
        await anyio.sleep(0.03)
