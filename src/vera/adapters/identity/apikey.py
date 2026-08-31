"""API-key authentication.

Splits ``<prefix>.<secret>``, looks the credential up by its clear prefix, then
verifies the secret half with a constant-time compare. A revoked or expired credential
never authenticates. On success it records last-used and resolves the owning principal,
so a service-account key (a principal of kind ``service_account``) resolves uniformly.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories.identity import SqlAlchemyIdentityRepository
from vera.domain.identity.models import (
    AuthenticatedPrincipal,
    CredentialKind,
    PrincipalKind,
)
from vera.shared.security import split_api_key, verify_secret
from vera.shared.time import utc_now


class ApiKeyAuthenticator:
    _TOUCH_INTERVAL_S = 60.0
    _TOUCH_CACHE_SIZE = 4096

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._last_touched: OrderedDict[UUID, float] = OrderedDict()

    def _should_touch(self, credential_id: UUID) -> bool:
        now = time.monotonic()
        previous = self._last_touched.get(credential_id)
        if previous is not None and now - previous < self._TOUCH_INTERVAL_S:
            self._last_touched.move_to_end(credential_id)
            return False
        self._last_touched[credential_id] = now
        self._last_touched.move_to_end(credential_id)
        while len(self._last_touched) > self._TOUCH_CACHE_SIZE:
            self._last_touched.popitem(last=False)
        return True

    async def authenticate(self, credential: str) -> AuthenticatedPrincipal | None:
        parts = split_api_key(credential)
        if parts is None:
            return None
        key_prefix, secret = parts

        async with self._session_factory() as session:
            repo = SqlAlchemyIdentityRepository(session)
            resolved = await repo.get_credential_principal_by_prefix(key_prefix)
            if resolved is None:
                return None
            record, principal = resolved
            if record.kind is not CredentialKind.API_KEY:
                return None
            if record.revoked_at is not None:
                return None
            if record.expires_at is not None and record.expires_at <= utc_now():
                return None
            if not verify_secret(secret, record.hashed_secret):
                return None
            if self._should_touch(record.id):
                await repo.touch_credential(record.id)
                await session.commit()

        via = principal.id if principal.kind is PrincipalKind.SERVICE_ACCOUNT else None
        return AuthenticatedPrincipal(
            id=principal.id,
            kind=principal.kind,
            display_name=principal.display_name,
            personal_group_id=principal.personal_group_id,
            via_service_account_id=via,
        )
