"""OIDC login: verify an identity-provider JWT and resolve a principal.

The verifier validates signature, issuer, audience, and expiry. On first login the
authenticator provisions a principal (just-in-time), linking it by a stable
``oidc_<hash(iss|sub)>`` credential so later logins resolve the same principal even if
the email changes. Only an explicitly verified email may link a pre-provisioned user.
A provisioned principal has only its personal scope until an admin
grants a membership, so JIT provisioning never widens access on its own.

The signature is checked against a configured key or the issuer's cached JWKS.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import jwt
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories.identity import SqlAlchemyIdentityRepository
from vera.domain.identity.models import (
    AuthenticatedPrincipal,
    CredentialKind,
    PrincipalKind,
)
from vera.shared.ids import uuid7
from vera.shared.security import hash_secret


class OidcTokenVerifier:
    def __init__(
        self,
        *,
        algorithms: list[str],
        issuer: str,
        audience: str,
        signing_key: str | None = None,
        jwks_url: str | None = None,
    ) -> None:
        if not signing_key and not jwks_url:
            raise ValueError("OIDC verifier needs a signing_key or a jwks_url")
        self._signing_key = signing_key
        self._algorithms = algorithms
        self._issuer = issuer
        self._audience = audience
        # Production fetches the issuer's JWKS and caches the keys; a static signing key
        # is the simple path for local/dev or a symmetric secret.
        self._jwks: Any = jwt.PyJWKClient(jwks_url, timeout=5) if jwks_url else None
        self._jwks_lock = threading.Lock()
        self._jwks_async_lock = asyncio.Lock()
        self._jwks_retry_after = 0.0
        self._known_keys: dict[str, tuple[Any, float]] = {}

    def _remember_keys(self, signing_keys: list[Any], now: float) -> None:
        for signing_key in signing_keys:
            kid = getattr(signing_key, "key_id", None)
            key = getattr(signing_key, "key", None)
            if isinstance(kid, str) and kid and key is not None:
                self._known_keys[kid] = (key, now + 300)

    def _key(self, token: str) -> Any:
        if self._jwks is not None:
            kid = jwt.get_unverified_header(token).get("kid")
            if not isinstance(kid, str) or not kid:
                raise jwt.PyJWKClientError("JWKS token is missing kid")
            with self._jwks_lock:
                now = time.monotonic()
                known = self._known_keys.get(kid)
                if known is not None and known[1] > now:
                    return known[0]
                if now < self._jwks_retry_after:
                    raise jwt.PyJWKClientError("JWKS refresh is temporarily throttled")
                try:
                    self._remember_keys(self._jwks.get_signing_keys(), now)
                    known = self._known_keys.get(kid)
                    if known is None:
                        self._remember_keys(self._jwks.get_signing_keys(refresh=True), now)
                        known = self._known_keys.get(kid)
                    if known is None:
                        raise jwt.PyJWKClientError("JWKS has no matching signing key")
                except (
                    jwt.PyJWKClientError,
                    jwt.PyJWTError,
                    OSError,
                    TypeError,
                    ValueError,
                ) as exc:
                    self._jwks_retry_after = now + 30
                    if isinstance(exc, jwt.PyJWKClientError):
                        raise
                    raise jwt.PyJWKClientError("JWKS lookup failed") from exc
                return known[0]
        return self._signing_key

    def verify(self, token: str) -> dict[str, Any] | None:
        try:
            return jwt.decode(
                token,
                self._key(token),
                algorithms=self._algorithms,
                audience=self._audience,
                issuer=self._issuer,
                options={"require": ["exp", "iss", "aud", "sub"]},
            )
        except (jwt.PyJWTError, jwt.PyJWKClientError):
            return None

    async def verify_async(self, token: str) -> dict[str, Any] | None:
        """Run JWT/JWKS verification off the event loop."""
        if self._jwks is None:
            return self.verify(token)
        try:
            kid = jwt.get_unverified_header(token).get("kid")
        except jwt.PyJWTError:
            return None
        if not isinstance(kid, str) or not kid:
            return None
        known = self._known_keys.get(kid)
        if known is not None and known[1] > time.monotonic():
            return self.verify(token)
        if time.monotonic() < self._jwks_retry_after:
            return None
        async with self._jwks_async_lock:
            known = self._known_keys.get(kid)
            if known is not None and known[1] > time.monotonic():
                return self.verify(token)
            if time.monotonic() < self._jwks_retry_after:
                return None
            return await asyncio.to_thread(self.verify, token)


class OidcAuthenticator:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        verifier: OidcTokenVerifier,
    ) -> None:
        self._session_factory = session_factory
        self._verifier = verifier

    async def authenticate(self, credential: str) -> AuthenticatedPrincipal | None:
        claims = await self._verifier.verify_async(credential)
        if claims is None:
            return None
        return await self.authenticate_claims(claims)

    async def authenticate_claims(self, claims: dict[str, Any]) -> AuthenticatedPrincipal | None:
        """Resolve verified OIDC claims without verifying the same token twice."""
        subject = claims.get("sub")
        if not subject:
            return None
        issuer = str(claims.get("iss", ""))
        email = claims.get("email") if claims.get("email_verified") is True else None
        email_str = str(email) if email else None
        key_prefix = "oidc_" + hash_secret(f"{issuer}|{subject}")[:32]

        for attempt in range(2):
            async with self._session_factory() as session:
                try:
                    repo = SqlAlchemyIdentityRepository(session)
                    link = await repo.get_credential_by_prefix(key_prefix)
                    principal = None
                    if link is not None and link.principal_id is not None:
                        principal = await repo.get_principal(link.principal_id)
                    if principal is None:
                        if email_str is not None:
                            principal = await repo.get_principal_by_email(email_str)
                        if principal is None:
                            new_id = uuid7()
                            principal = await repo.create_principal(
                                principal_id=new_id,
                                kind=PrincipalKind.USER,
                                display_name=email_str or str(subject),
                                email=email_str,
                                personal_group_id=f"u:{new_id}",
                            )
                        if link is None:
                            # This no-secret credential makes later logins stable.
                            await repo.create_credential(
                                principal_id=principal.id,
                                service_account_id=None,
                                kind=CredentialKind.OAUTH,
                                key_prefix=key_prefix,
                                hashed_secret="oidc-link",  # noqa: S106  link row, no secret
                                expires_at=None,
                            )
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    if attempt == 0:
                        continue
                    return None

                return AuthenticatedPrincipal(
                    id=principal.id,
                    kind=principal.kind,
                    display_name=principal.display_name,
                    personal_group_id=principal.personal_group_id,
                )

        return None
