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
