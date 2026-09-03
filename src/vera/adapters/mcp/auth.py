"""OAuth 2.1 Resource Server token verification.

Validates a bearer JWT's signature, issuer, audience, expiry, and required scopes.
An audience bound to this resource server (RFC 8707 / RFC 9728) prevents a token
minted for another service from being replayed here. A failed check returns None, so
the MCP SDK responds 401 with the protected-resource metadata pointer.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any, cast
from uuid import UUID

import jwt
from mcp.server.auth.provider import AccessToken, TokenVerifier

from vera.adapters.identity.oidc import OidcTokenVerifier
from vera.domain.identity.models import AuthenticatedPrincipal


def _scopes(claims: dict[str, Any]) -> list[str]:
    for name in ("scope", "scp"):
        raw = claims.get(name)
        if isinstance(raw, str):
            return raw.split()
    value = claims.get("scopes")
    if isinstance(value, list):
        return [str(item) for item in cast("list[Any]", value)]
    return []


def issue_mcp_jwt(
    *,
    principal_id: UUID,
    secret: str,
    algorithm: str,
    issuer: str,
    audience: str,
    scopes: list[str],
    ttl_seconds: int,
    now: int | None = None,
) -> str:
    """Issue a short-lived MCP bearer token for an authenticated principal."""
    issued_at = int(time.time()) if now is None else now
    return jwt.encode(
        {
            "sub": str(principal_id),
            "iss": issuer,
            "aud": audience,
            "scope": " ".join(dict.fromkeys(scopes)),
            "iat": issued_at,
            "exp": issued_at + ttl_seconds,
        },
        secret,
        algorithm=algorithm,
    )


class JwtTokenVerifier(TokenVerifier):
    def __init__(
        self,
        *,
        secret: str,
        algorithm: str,
        issuer: str,
        audience: str,
        required_scopes: list[str],
        principal_exists: Callable[[UUID], Awaitable[bool]],
    ) -> None:
        self._secret = secret
        self._algorithm = algorithm
        self._issuer = issuer
        self._audience = audience
        self._required_scopes = set(required_scopes)
        self._principal_exists = principal_exists

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            claims: dict[str, Any] = jwt.decode(
                token,
                self._secret,
                algorithms=[self._algorithm],
                audience=self._audience,
                issuer=self._issuer,
                options={"require": ["exp", "iss", "aud", "sub"]},
            )
        except jwt.PyJWTError:
            return None

        scopes = _scopes(claims)
        if not self._required_scopes.issubset(scopes):
            return None
        subject = claims.get("sub")
        if not subject:
            return None
        try:
            principal_id = UUID(str(subject))
        except ValueError:
            return None
        if not await self._principal_exists(principal_id):
            return None
        return AccessToken(
            token=token,
            client_id=str(principal_id),
            scopes=scopes,
            subject=str(principal_id),
            resource=self._audience,
            claims=claims,
        )


class OidcMcpTokenVerifier(TokenVerifier):
    """Verify an external IdP access token and map its subject to a VERA principal."""

    def __init__(
        self,
        *,
        verifier: OidcTokenVerifier,
        authenticate_claims: Callable[[dict[str, Any]], Awaitable[AuthenticatedPrincipal | None]],
        audience: str,
        required_scopes: list[str],
    ) -> None:
        self._verifier = verifier
        self._authenticate_claims = authenticate_claims
        self._audience = audience
        self._required_scopes = set(required_scopes)

    async def verify_token(self, token: str) -> AccessToken | None:
        claims = await self._verifier.verify_async(token)
        if claims is None:
            return None
        scopes = _scopes(claims)
        if not self._required_scopes.issubset(scopes):
            return None
        principal = await self._authenticate_claims(claims)
        if principal is None:
            return None
        return AccessToken(
            token=token,
            client_id=str(claims.get("client_id") or claims.get("azp") or principal.id),
            scopes=scopes,
            subject=str(principal.id),
            resource=self._audience,
            claims=claims,
        )


class CompositeTokenVerifier(TokenVerifier):
    """Accept the first valid token from the configured bearer and OAuth verifiers."""

    def __init__(self, *verifiers: TokenVerifier) -> None:
        self._verifiers = verifiers

    async def verify_token(self, token: str) -> AccessToken | None:
        for verifier in self._verifiers:
            access_token = await verifier.verify_token(token)
            if access_token is not None:
                return access_token
        return None
