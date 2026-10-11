"""JWT verifier: the token facts the resource server checks."""

from __future__ import annotations

import time

import jwt
import pytest

from geoecon_pipeline_mcp.auth import JwtVerifier, mint_hs256, scopes_from

ISS, AUD, SECRET = "https://issuer.test", "https://pipeline.test/mcp", "unit-test-secret-not-for-production-at-least-32-bytes"


@pytest.fixture
def verifier():
    return JwtVerifier(issuer=ISS, audience=AUD, hs256_secret=SECRET)


@pytest.mark.anyio
async def test_valid_token_yields_scopes_subject_and_resource(verifier):
    tok = mint_hs256(SECRET, issuer=ISS, audience=AUD, subject="operator@example", scopes=["pipeline:read", "pipeline:operate"],
                     azp="inspector")
    at = await verifier.verify_token(tok)
    assert at is not None
    assert at.scopes == ["pipeline:read", "pipeline:operate"] and at.subject == "operator@example"
    assert at.client_id == "inspector" and at.resource == AUD and at.expires_at > time.time()
    assert at.claims["iss"] == ISS and "exp" not in at.claims


@pytest.mark.anyio
async def test_wrong_audience_issuer_or_expiry_is_rejected(verifier):
    assert await verifier.verify_token(mint_hs256(SECRET, issuer=ISS, audience="https://other/mcp", subject="s", scopes=[])) is None
    assert await verifier.verify_token(mint_hs256(SECRET, issuer="https://evil", audience=AUD, subject="s", scopes=[])) is None
    assert await verifier.verify_token(mint_hs256(SECRET, issuer=ISS, audience=AUD, subject="s", scopes=[], ttl_s=-120)) is None
    assert await verifier.verify_token(mint_hs256("another-secret-that-is-also-at-least-32-bytes-long", issuer=ISS, audience=AUD, subject="s", scopes=[])) is None
    assert await verifier.verify_token("not-a-jwt") is None


@pytest.mark.anyio
async def test_audience_list_and_scp_list_forms(verifier):
    now = int(time.time())
    tok = jwt.encode({"iss": ISS, "aud": [AUD, "https://also"], "sub": "s", "scp": ["pipeline:read"], "exp": now + 60}, SECRET, algorithm="HS256")
    at = await verifier.verify_token(tok)
    assert at.scopes == ["pipeline:read"] and at.resource == AUD


def test_scopes_from_variants():
    assert scopes_from({"scope": "a b  c"}) == ["a", "b", "c"]
    assert scopes_from({"scp": ["a"]}) == ["a"]
    assert scopes_from({"scope": ["x", "y"]}) == ["x", "y"]
    assert scopes_from({}) == []


def test_verifier_needs_a_key():
    with pytest.raises(ValueError):
        JwtVerifier(issuer=ISS, audience=AUD)


def test_server_builds_auth_settings_when_configured(synthetic_db):
    from geoecon_pipeline_mcp.server import Settings, create_server

    app = create_server(Settings(mode="synthetic", synthetic_db=synthetic_db, auth_issuer=ISS, auth_resource_url=AUD,
                                 auth_hs256_secret=SECRET))
    try:
        assert app.mcp.settings.auth.required_scopes == ["pipeline:read"]
        assert str(app.mcp.settings.auth.resource_server_url) == AUD
        assert app.mcp.settings.auth.validate_token_resource is True
    finally:
        app.close()


# ── fail closed (finding 4) ─────────────────────────────────────────────────────────────


def test_partial_auth_configuration_is_a_startup_error(synthetic_db):
    from geoecon_pipeline_mcp.server import Settings, create_server

    for partial in ({"auth_issuer": ISS}, {"auth_resource_url": AUD}, {"auth_hs256_secret": SECRET},
                    {"auth_issuer": ISS, "auth_resource_url": AUD}):             # no key source
        with pytest.raises(ValueError, match="AUTH_"):
            create_server(Settings(mode="synthetic", synthetic_db=synthetic_db, **partial))


@pytest.mark.anyio
async def test_no_token_never_inherits_stdio_scopes_when_auth_is_configured(synthetic_db):
    from mcp import Client
    from geoecon_pipeline_mcp.server import Settings, create_server

    app = create_server(Settings(mode="synthetic", synthetic_db=synthetic_db, auth_issuer=ISS, auth_resource_url=AUD,
                                 auth_hs256_secret=SECRET, stdio_scopes=("pipeline:read", "pipeline:operate")))
    try:
        async with Client(app.mcp, raise_exceptions=True) as client:        # in-process: no bearer middleware ran
            r = await client.call_tool("list_runs", {})
            assert r.is_error is True and "pipeline:read" in r.content[0].text
            r = await client.call_tool("start_job", {"kind": "reconcile_abandoned"})
            assert r.is_error is True and "pipeline:operate" in r.content[0].text
    finally:
        app.close()


def test_http_on_a_non_loopback_host_without_auth_is_refused(monkeypatch):
    from geoecon_pipeline_mcp import server as S

    monkeypatch.setenv("GEOECON_MODE", "synthetic")
    for var in ("AUTH_ISSUER", "AUTH_RESOURCE_URL", "AUTH_JWKS_URL", "AUTH_HS256_SECRET", "AUTH_PUBLIC_KEY_PEM"):
        monkeypatch.delenv(var, raising=False)
    created = []
    monkeypatch.setattr(S, "create_server", lambda *a, **k: created.append(1) or (_ for _ in ()).throw(RuntimeError("stop")))
    with pytest.raises(SystemExit) as ei:
        S.main(["--http", "--host", "0.0.0.0"])
    assert ei.value.code != 0 and created == []                                 # refused before anything was opened
    with pytest.raises(RuntimeError, match="stop"):
        S.main(["--http", "--host", "0.0.0.0", "--insecure-no-auth"])          # explicit opt-out reaches startup
    with pytest.raises(RuntimeError, match="stop"):
        S.main(["--http", "--host", "127.0.0.1"])                               # loopback dev is allowed
    with pytest.raises(RuntimeError, match="stop"):
        S.main(["--stdio"])


# ── JWKS: off the event loop, unknown kids negative-cached (finding 8) ──────────────────


@pytest.mark.anyio
async def test_unknown_kid_is_negative_cached(monkeypatch):
    from jwt import PyJWKClientError

    class FakeJWKS:
        fetches = 0

        def get_signing_key_from_jwt(self, token):
            self.fetches += 1
            raise PyJWKClientError("Unable to find a signing key that matches: kid")

    v = JwtVerifier(issuer=ISS, audience=AUD, jwks_url="https://issuer.test/jwks")
    v.jwks = FakeJWKS()
    tok = jwt.encode({"iss": ISS, "aud": AUD, "sub": "s", "exp": int(time.time()) + 60}, "k" * 32, algorithm="HS256",
                     headers={"kid": "rotated-away"})
    assert await v.verify_token(tok) is None
    assert await v.verify_token(tok) is None
    assert await v.verify_token(tok) is None
    assert v.jwks.fetches == 1                                                 # the next two never hit the network
    v._bad_kids["rotated-away"] = 0.0                                          # cache entry expired
    assert await v.verify_token(tok) is None
    assert v.jwks.fetches == 2


@pytest.mark.anyio
async def test_decode_runs_in_a_worker_thread(monkeypatch, verifier):
    import threading
    seen = []
    original = verifier.decode

    def spy(token):
        seen.append(threading.current_thread() is threading.main_thread())
        return original(token)

    monkeypatch.setattr(verifier, "decode", spy)
    tok = mint_hs256(SECRET, issuer=ISS, audience=AUD, subject="s", scopes=["pipeline:read"])
    assert (await verifier.verify_token(tok)).scopes == ["pipeline:read"]
    assert seen == [False]                                                     # PyJWT/JWKS I/O never blocks the loop
