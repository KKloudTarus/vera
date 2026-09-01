"""OAuth 2.1 Resource Server token verification.

Validates a bearer JWT's signature, issuer, audience, expiry, and required scopes.
An audience bound to this resource server (RFC 8707 / RFC 9728) prevents a token
minted for another service from being replayed here. A failed check returns None, so
the MCP SDK responds 401 with the protected-resource metadata pointer.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast
from uuid import UUID

import jwt
from mcp.server.auth.provider import AccessToken, TokenVerifier


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

    def _scopes(self, claims: dict[str, Any]) -> list[str]:
        raw = claims.get("scope")
        if isinstance(raw, str):
            return raw.split()
        value = claims.get("scopes")
        if isinstance(value, list):
            return [str(item) for item in cast("list[Any]", value)]
        return []

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

        scopes = self._scopes(claims)
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
