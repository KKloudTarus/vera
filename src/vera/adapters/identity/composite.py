"""Dispatch a bearer credential to the right authenticator by its shape.

A VERA API key is ``<prefix>.<secret>`` with a ``vera_`` prefix; anything else is
treated as an OIDC bearer token. When OIDC is not configured, only the API-key path is
tried, so local development needs no identity provider.
"""

from __future__ import annotations

from vera.domain.identity.models import AuthenticatedPrincipal
from vera.domain.ports.identity import Authenticator
from vera.shared.security import split_api_key

_API_KEY_NAMESPACE = "vera_"


class CompositeAuthenticator:
    def __init__(self, *, api_key: Authenticator, oidc: Authenticator | None = None) -> None:
        self._api_key = api_key
        self._oidc = oidc

    async def authenticate(self, credential: str) -> AuthenticatedPrincipal | None:
        parts = split_api_key(credential)
        looks_like_api_key = parts is not None and parts[0].startswith(_API_KEY_NAMESPACE)
        if looks_like_api_key:
            return await self._api_key.authenticate(credential)
        if self._oidc is not None:
            return await self._oidc.authenticate(credential)
        return None
