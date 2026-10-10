"""OAuth 2.1 resource-server piece: a JWT `TokenVerifier`.

Same shape as hostel-ops-mcp's verifier, copied rather than shared so each repo stands alone.
The authorization server is external (the cluster's IdP); this server only verifies the
bearer tokens it is handed:

* signature — HS256 shared secret (tests, small deployments), an RSA/EC public key PEM, or a
  JWKS endpoint (`PyJWKClient`, cached);
* `iss` must equal the configured issuer;
* `aud` must contain the resource server URL (RFC 8707 resource indicator); the SDK's bearer
  middleware re-checks `AccessToken.resource` against `AuthSettings.resource_server_url`;
* `exp` is enforced by PyJWT and again by the middleware;
* scopes come from `scope` (space-separated string, RFC 8693/9068) or `scp` (list).

Scopes this server uses: pipeline:read, pipeline:operate, pipeline:send.
"""

from __future__ import annotations

import logging
from typing import Any

import jwt
from jwt import PyJWKClient

from mcp.server.auth.provider import AccessToken, TokenVerifier

log = logging.getLogger("geoecon_pipeline_mcp.auth")

ALL_SCOPES = ("pipeline:read", "pipeline:operate", "pipeline:send")


class JwtVerifier(TokenVerifier):
    def __init__(self, *, issuer: str, audience: str, hs256_secret: str | None = None,
                 public_key_pem: str | None = None, jwks_url: str | None = None, leeway_s: int = 30):
        if not (hs256_secret or public_key_pem or jwks_url):
            raise ValueError("JwtVerifier needs one of hs256_secret, public_key_pem, jwks_url")
        self.issuer = issuer
        self.audience = audience
        self.hs256_secret = hs256_secret
        self.public_key_pem = public_key_pem
        self.jwks = PyJWKClient(jwks_url, cache_keys=True) if jwks_url else None
        self.leeway_s = leeway_s

    def _key_and_algs(self, token: str) -> tuple[Any, list[str]]:
        if self.jwks is not None:
            return self.jwks.get_signing_key_from_jwt(token).key, ["RS256", "RS384", "RS512", "ES256", "ES384", "PS256"]
        if self.public_key_pem:
            return self.public_key_pem, ["RS256", "RS384", "RS512", "ES256", "ES384", "PS256"]
        return self.hs256_secret, ["HS256"]

    def decode(self, token: str) -> dict[str, Any] | None:
        try:
            key, algs = self._key_and_algs(token)
            return jwt.decode(token, key, algorithms=algs, audience=self.audience, issuer=self.issuer,
                              leeway=self.leeway_s, options={"require": ["exp", "iss", "aud"]})
        except jwt.PyJWTError as e:
            log.info("token rejected: %s", type(e).__name__)
            return None

    async def verify_token(self, token: str) -> AccessToken | None:
        claims = self.decode(token)
        if claims is None:
            return None
        scopes = scopes_from(claims)
        aud = claims.get("aud")
        resource = self.audience if (aud == self.audience or (isinstance(aud, list) and self.audience in aud)) else None
        return AccessToken(
            token=token,
            client_id=str(claims.get("azp") or claims.get("client_id") or claims.get("sub") or "unknown"),
            scopes=scopes,
            expires_at=int(claims["exp"]) if "exp" in claims else None,
            resource=resource,
            subject=str(claims["sub"]) if claims.get("sub") else None,
            claims={k: v for k, v in claims.items() if k not in ("exp", "iat", "nbf")},
        )


def scopes_from(claims: dict[str, Any]) -> list[str]:
    scope = claims.get("scope")
    if isinstance(scope, str):
        return [s for s in scope.split() if s]
    scp = claims.get("scp") or (scope if isinstance(scope, list) else None)
    if isinstance(scp, list):
        return [str(s) for s in scp]
    return []


def mint_hs256(secret: str, *, issuer: str, audience: str, subject: str, scopes: list[str], ttl_s: int = 3600,
               **extra: Any) -> str:
    """Test/dev helper: a signed HS256 token the verifier accepts. Not for production issuance."""
    import time

    now = int(time.time())
    payload = {"iss": issuer, "aud": audience, "sub": subject, "scope": " ".join(scopes), "iat": now,
               "exp": now + ttl_s, **extra}
    return jwt.encode(payload, secret, algorithm="HS256")
