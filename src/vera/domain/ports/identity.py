"""Identity ports: authentication, membership storage, and scope resolution.

A client never chooses its group_ids. The server resolves them from the authenticated
principal's memberships, which is what keeps tenants isolated at the API surface.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from vera.domain.identity.models import (
    AuthenticatedPrincipal,
    Credential,
    CredentialKind,
    Membership,
    Principal,
    PrincipalKind,
    Role,
    ServiceAccount,
)


@dataclass(frozen=True, slots=True)
class ResolvedScope:
    group_ids: tuple[str, ...]
    personal_group_id: str
    primary_workspace_id: UUID | None


class ScopeResolver(Protocol):
    async def resolve(self, principal_id: UUID) -> ResolvedScope | None:
        """The scopes a principal may read, or None if the principal is unknown."""
        ...


class Authenticator(Protocol):
    async def authenticate(self, credential: str) -> AuthenticatedPrincipal | None:
        """Resolve a bearer credential to a principal, or None if it does not verify."""
        ...


class IdentityRepository(Protocol):
    """Storage for principals, service accounts, memberships, and credentials.

    Attached to a Unit of Work; these methods read and write, never commit.
    """

    async def create_principal(
        self,
        *,
        principal_id: UUID,
        kind: PrincipalKind,
        display_name: str,
        email: str | None,
        personal_group_id: str,
    ) -> Principal: ...

    async def get_principal(self, principal_id: UUID) -> Principal | None: ...

    async def get_principal_by_email(self, email: str) -> Principal | None: ...

    async def create_service_account(
        self,
        *,
        service_account_id: UUID,
        owner_principal_id: UUID,
        workspace_id: UUID,
        name: str,
        description: str | None,
    ) -> ServiceAccount: ...

    async def add_membership(
        self,
        *,
        principal_id: UUID,
        workspace_id: UUID,
        project_id: UUID | None,
        role: Role,
    ) -> Membership: ...

    async def list_memberships(self, principal_id: UUID) -> list[Membership]: ...

    async def find_membership(self, *, principal_id: UUID, workspace_id: UUID) -> Membership | None:
        """The principal's workspace-wide membership, if any (project rows ignored)."""
        ...

    async def create_credential(
        self,
        *,
        principal_id: UUID | None,
        service_account_id: UUID | None,
        kind: CredentialKind,
        key_prefix: str,
        hashed_secret: str,
        expires_at: datetime | None,
    ) -> Credential: ...

    async def get_credential_by_prefix(self, key_prefix: str) -> Credential | None: ...

    async def touch_credential(self, credential_id: UUID) -> None:
        """Record that a credential was just used (best-effort last_used_at)."""
        ...

    async def revoke_credential(self, credential_id: UUID) -> bool: ...
