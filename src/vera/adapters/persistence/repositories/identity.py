"""Identity repository: principals, service accounts, memberships, credentials.

Maps ORM rows to domain records at its boundary. These tables carry no row-level
security (they are organization-level, not group-scoped), so the repository reads and
writes without a tenant context.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import CursorResult, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.identity import (
    CredentialRow,
    MembershipRow,
    PrincipalRow,
    ServiceAccountRow,
)
from vera.domain.identity.models import (
    Credential,
    CredentialKind,
    Membership,
    Principal,
    PrincipalKind,
    Role,
    ServiceAccount,
)
from vera.shared.time import utc_now


def _to_principal(row: PrincipalRow) -> Principal:
    return Principal(
        id=row.id,
        kind=PrincipalKind(row.kind),
        display_name=row.display_name,
        email=row.email,
        personal_group_id=row.personal_group_id,
    )


def _to_membership(row: MembershipRow) -> Membership:
    return Membership(
        id=row.id,
        principal_id=row.principal_id,
        workspace_id=row.workspace_id,
        project_id=row.project_id,
        role=Role(row.role),
    )


def _to_credential(row: CredentialRow) -> Credential:
    return Credential(
        id=row.id,
        principal_id=row.principal_id,
        service_account_id=row.service_account_id,
        kind=CredentialKind(row.kind),
        key_prefix=row.key_prefix,
        hashed_secret=row.hashed_secret,
        expires_at=row.expires_at,
        revoked_at=row.revoked_at,
    )


class SqlAlchemyIdentityRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_principal(
        self,
        *,
        principal_id: UUID,
        kind: PrincipalKind,
        display_name: str,
        email: str | None,
        personal_group_id: str,
    ) -> Principal:
        row = PrincipalRow(
            id=principal_id,
            kind=kind.value,
            display_name=display_name,
            email=email,
            personal_group_id=personal_group_id,
        )
        self._session.add(row)
        await self._session.flush()
        return _to_principal(row)

    async def get_principal(self, principal_id: UUID) -> Principal | None:
        row = await self._session.get(PrincipalRow, principal_id)
        return _to_principal(row) if row is not None else None

    async def get_principal_by_email(self, email: str) -> Principal | None:
        row = (
            await self._session.execute(select(PrincipalRow).where(PrincipalRow.email == email))
        ).scalar_one_or_none()
        return _to_principal(row) if row is not None else None

    async def create_service_account(
        self,
        *,
        service_account_id: UUID,
        owner_principal_id: UUID,
        workspace_id: UUID,
        name: str,
        description: str | None,
    ) -> ServiceAccount:
        row = ServiceAccountRow(
            id=service_account_id,
            owner_principal_id=owner_principal_id,
            workspace_id=workspace_id,
            name=name,
            description=description,
        )
        self._session.add(row)
        await self._session.flush()
        return ServiceAccount(
            id=row.id,
            owner_principal_id=row.owner_principal_id,
            workspace_id=row.workspace_id,
            name=row.name,
            description=row.description,
        )

    async def add_membership(
        self,
        *,
        principal_id: UUID,
        workspace_id: UUID,
        project_id: UUID | None,
        role: Role,
    ) -> Membership:
        row = MembershipRow(
            principal_id=principal_id,
            workspace_id=workspace_id,
            project_id=project_id,
            role=role.value,
        )
        self._session.add(row)
        await self._session.flush()
        return _to_membership(row)

    async def list_memberships(self, principal_id: UUID) -> list[Membership]:
        rows = (
            await self._session.execute(
                select(MembershipRow).where(MembershipRow.principal_id == principal_id)
            )
        ).scalars()
        return [_to_membership(row) for row in rows]

    async def find_membership(self, *, principal_id: UUID, workspace_id: UUID) -> Membership | None:
        row = (
            await self._session.execute(
                select(MembershipRow).where(
                    MembershipRow.principal_id == principal_id,
                    MembershipRow.workspace_id == workspace_id,
                    MembershipRow.project_id.is_(None),
                )
            )
        ).scalar_one_or_none()
        return _to_membership(row) if row is not None else None

    async def create_credential(
        self,
        *,
        principal_id: UUID | None,
        service_account_id: UUID | None,
        kind: CredentialKind,
        key_prefix: str,
        hashed_secret: str,
        expires_at: datetime | None,
    ) -> Credential:
        row = CredentialRow(
            principal_id=principal_id,
            service_account_id=service_account_id,
            kind=kind.value,
            key_prefix=key_prefix,
            hashed_secret=hashed_secret,
            expires_at=expires_at,
        )
        self._session.add(row)
        await self._session.flush()
        return _to_credential(row)

    async def get_credential_by_prefix(self, key_prefix: str) -> Credential | None:
        row = (
            await self._session.execute(
                select(CredentialRow).where(CredentialRow.key_prefix == key_prefix)
            )
        ).scalar_one_or_none()
        return _to_credential(row) if row is not None else None

    async def get_credential(self, credential_id: UUID) -> Credential | None:
        row = (
            await self._session.execute(
                select(CredentialRow).where(CredentialRow.id == credential_id)
            )
        ).scalar_one_or_none()
        return _to_credential(row) if row is not None else None

    async def touch_credential(self, credential_id: UUID) -> None:
        await self._session.execute(
            update(CredentialRow)
            .where(CredentialRow.id == credential_id)
            .values(last_used_at=utc_now())
        )

    async def revoke_credential(self, credential_id: UUID) -> bool:
        result = await self._session.execute(
            update(CredentialRow)
            .where(CredentialRow.id == credential_id, CredentialRow.revoked_at.is_(None))
            .values(revoked_at=utc_now())
        )
        return cast("CursorResult[Any]", result).rowcount > 0
