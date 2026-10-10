"""Streamable HTTP smoke: the server over a real socket, stateless, with and without OAuth."""

from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest
import uvicorn

from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared._httpx_utils import create_mcp_http_client

from geoecon_pipeline_mcp.auth import mint_hs256
from geoecon_pipeline_mcp.server import Settings, create_server

from .conftest import call, poll
from .test_protocol import TOOL_ORDER

ISS, SECRET = "https://issuer.test", "http-test-secret-that-is-long-enough-for-hmac-sha256"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Uvicorn:
    def __init__(self, asgi_app, port: int):
        self.server = uvicorn.Server(uvicorn.Config(asgi_app, host="127.0.0.1", port=port, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.port = port

    def __enter__(self):
        self.thread.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    return self
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("uvicorn did not start")

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(5)


@pytest.fixture
def http_app(synthetic_db):
    app = create_server(Settings(mode="synthetic", synthetic_db=synthetic_db, fake_unit_sleep=0.0))
    port = _free_port()
    with _Uvicorn(app.mcp.streamable_http_app(stateless_http=True), port):
        yield app, f"http://127.0.0.1:{port}/mcp"
    app.close()


@pytest.fixture
def auth_app(synthetic_db):
    port = _free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    app = create_server(Settings(mode="synthetic", synthetic_db=synthetic_db, fake_unit_sleep=0.0,
                                 auth_issuer=ISS, auth_resource_url=url, auth_hs256_secret=SECRET))
    with _Uvicorn(app.mcp.streamable_http_app(stateless_http=True), port):
        yield app, url
    app.close()


def _bearer(url: str, scopes: list[str], subject: str = "operator@example"):
    tok = mint_hs256(SECRET, issuer=ISS, audience=url, subject=subject, scopes=scopes)
    return streamable_http_client(url, http_client=create_mcp_http_client(headers={"Authorization": f"Bearer {tok}"}))


@pytest.mark.anyio
async def test_stateless_http_serves_the_same_tools(http_app):
    _, url = http_app
    async with Client(url, raise_exceptions=True) as client:
        assert [t.name for t in (await client.list_tools()).tools] == TOOL_ORDER
        runs = (await call(client, "list_runs", limit=2))["result"]
        assert [r["run_id"] for r in runs] == [105, 104]
        h = await call(client, "start_job", kind="run_stage", run_id=105, stage="render", force=True)
        job = await poll(client, h["job_id"])
        assert job["status"] == "completed" and job["principal"] == "stdio"


@pytest.mark.anyio
async def test_unauthenticated_requests_are_refused_with_401(auth_app):
    _, url = auth_app
    async with httpx.AsyncClient() as http:
        r = await http.post(url, json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
                            headers={"Accept": "application/json, text/event-stream", "Content-Type": "application/json"})
        assert r.status_code == 401 and "Bearer" in r.headers.get("www-authenticate", "")
        meta = await http.get(url.replace("/mcp", "/.well-known/oauth-protected-resource/mcp"))
        assert meta.status_code == 200
        body = meta.json()
        assert body["resource"].rstrip("/") == url and body["authorization_servers"] == [ISS]
    with pytest.raises(Exception):
        async with Client(url, raise_exceptions=True) as client:
            await client.list_tools()


@pytest.mark.anyio
async def test_bearer_scopes_drive_authorization(auth_app):
    _, url = auth_app
    async with Client(_bearer(url, ["pipeline:read"]), raise_exceptions=True) as client:
        assert (await call(client, "list_runs", limit=1))["result"]
        r = await client.call_tool("start_job", {"kind": "reconcile_abandoned"})
        assert r.is_error is True and "pipeline:operate" in r.content[0].text
    async with Client(_bearer(url, ["pipeline:read", "pipeline:operate"], subject="ops@example"), raise_exceptions=True) as client:
        h = await call(client, "start_job", kind="run_stage", run_id=105, stage="render", force=True)
        job = await poll(client, h["job_id"])
        assert job["status"] == "completed" and job["principal"] == "ops@example"


@pytest.mark.anyio
async def test_token_for_another_resource_is_refused(auth_app):
    _, url = auth_app
    tok = mint_hs256(SECRET, issuer=ISS, audience="http://other.example/mcp", subject="s", scopes=["pipeline:read"])
    transport = streamable_http_client(url, http_client=create_mcp_http_client(headers={"Authorization": f"Bearer {tok}"}))
    with pytest.raises(Exception):
        async with Client(transport, raise_exceptions=True) as client:
            await client.list_tools()
