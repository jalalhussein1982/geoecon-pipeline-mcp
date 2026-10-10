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
