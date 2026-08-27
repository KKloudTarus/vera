"""OIDC login: verify an identity-provider JWT and resolve a principal.

The verifier validates signature, issuer, audience, and expiry. On first login the
authenticator provisions a principal (just-in-time), linking it by a stable
``oidc_<hash(iss|sub)>`` credential so later logins resolve the same principal even if
the email changes. A provisioned principal has only its personal scope until an admin
grants a membership, so JIT provisioning never widens access on its own.

The signature is checked against a configured key. Production should fetch the
issuer's JWKS; that upgrade lives behind this same interface.
"""

from __future__ import annotations

from typing import Any

import jwt
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
        signing_key: str,
        algorithms: list[str],
        issuer: str,
        audience: str,
    ) -> None:
        self._signing_key = signing_key
        self._algorithms = algorithms
        self._issuer = issuer
        self._audience = audience

    def verify(self, token: str) -> dict[str, Any] | None:
        try:
            return jwt.decode(
                token,
                self._signing_key,
                algorithms=self._algorithms,
                audience=self._audience,
                issuer=self._issuer,
            )
        except jwt.PyJWTError:
            return None


class OidcAuthenticator:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        verifier: OidcTokenVerifier,
    ) -> None:
        self._session_factory = session_factory
        self._verifier = verifier

    async def authenticate(self, credential: str) -> AuthenticatedPrincipal | None:
        claims = self._verifier.verify(credential)
        if claims is None:
            return None
        subject = claims.get("sub")
        if not subject:
            return None
        issuer = str(claims.get("iss", ""))
        email = claims.get("email")
        email_str = str(email) if email else None
        key_prefix = "oidc_" + hash_secret(f"{issuer}|{subject}")[:32]

        async with self._session_factory() as session:
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
                    # A link credential carries no secret; it maps the IdP subject to
                    # the principal so later logins are stable.
                    await repo.create_credential(
                        principal_id=principal.id,
                        service_account_id=None,
                        kind=CredentialKind.OAUTH,
                        key_prefix=key_prefix,
                        hashed_secret="oidc-link",  # noqa: S106  link row, no secret
                        expires_at=None,
                    )
            await session.commit()

        return AuthenticatedPrincipal(
            id=principal.id,
            kind=principal.kind,
            display_name=principal.display_name,
            personal_group_id=principal.personal_group_id,
        )
