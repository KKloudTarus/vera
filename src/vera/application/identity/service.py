"""IdentityService: the write side of tenancy and access control.

VERA assigns every scope its opaque group_id (``o:`` org, ``w:`` workspace, ``p:``
project, ``u:`` personal), so a client never chooses one. Membership roles are totally
ordered, so authorization is one comparison against the actor's workspace role.

A service account is modelled as a principal of kind ``service_account`` that shares
its id with its ``service_accounts`` catalog row. It therefore holds memberships and a
personal scope like any principal, which keeps authentication and scope resolution
uniform across humans and machines.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from vera.domain.identity.models import (
    AuthenticatedPrincipal,
    CredentialKind,
    Membership,
    Organization,
    Principal,
    PrincipalKind,
    Project,
    Role,
    ServiceAccount,
    Workspace,
    role_at_least,
)
from vera.domain.ports.unit_of_work import UnitOfWork
from vera.shared.errors import DomainError, Err, Forbidden, NotFound, Ok, Result
from vera.shared.ids import uuid7
from vera.shared.security import generate_api_key


@dataclass(frozen=True, slots=True)
class IssuedApiKey:
    credential_id: UUID
    principal_id: UUID
    api_key: str  # plaintext, returned once and never stored


class IdentityService:
    """Runs inside a Unit of Work; the caller commits."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def create_organization(self, *, name: str, slug: str) -> Organization:
        org_id = uuid7()
        return await self._uow.tenancy.create_organization(
            org_id=org_id, slug=slug, name=name, group_id=f"o:{org_id}"
        )

    async def create_workspace(
        self, *, actor: AuthenticatedPrincipal, org_id: UUID, name: str, slug: str
    ) -> Workspace:
        # Any authenticated principal may open a workspace and becomes its owner.
        workspace_id = uuid7()
        workspace = await self._uow.tenancy.create_workspace(
            workspace_id=workspace_id,
            org_id=org_id,
            slug=slug,
            name=name,
            group_id=f"w:{workspace_id}",
        )
        await self._uow.identity.add_membership(
            principal_id=actor.id,
            workspace_id=workspace_id,
            project_id=None,
            role=Role.OWNER,
        )
        return workspace

    async def create_project(
        self, *, actor: AuthenticatedPrincipal, workspace_id: UUID, name: str, slug: str
    ) -> Result[Project, DomainError]:
        guard = await self._require_role(actor, workspace_id, Role.ADMIN)
        if isinstance(guard, Err):
            return guard
        project_id = uuid7()
        project = await self._uow.tenancy.create_project(
            project_id=project_id,
            workspace_id=workspace_id,
            slug=slug,
            name=name,
            group_id=f"p:{project_id}",
        )
        return Ok(project)

    async def register(
        self, *, display_name: str, email: str | None = None
    ) -> tuple[Principal, IssuedApiKey]:
        """Self-service signup: create a user principal and its first API key.

        The new principal has only its personal scope until an admin adds it to a
        workspace, so this bootstrap path grants no access to shared memory.
        """
        principal = await self.create_principal(display_name=display_name, email=email)
        return principal, await self._issue_api_key(principal.id)

    async def create_principal(
        self,
        *,
        display_name: str,
        email: str | None,
        kind: PrincipalKind = PrincipalKind.USER,
    ) -> Principal:
        principal_id = uuid7()
        return await self._uow.identity.create_principal(
            principal_id=principal_id,
            kind=kind,
            display_name=display_name,
            email=email,
            personal_group_id=f"u:{principal_id}",
        )

    async def add_member(
        self,
        *,
        actor: AuthenticatedPrincipal,
        workspace_id: UUID,
        principal_id: UUID,
        role: Role,
        project_id: UUID | None = None,
    ) -> Result[Membership, DomainError]:
        guard = await self._require_role(actor, workspace_id, Role.ADMIN)
        if isinstance(guard, Err):
            return guard
        member = await self._uow.identity.get_principal(principal_id)
        if member is None:
            return Err(NotFound(f"principal {principal_id} does not exist"))
        membership = await self._uow.identity.add_membership(
            principal_id=principal_id,
            workspace_id=workspace_id,
            project_id=project_id,
            role=role,
        )
        return Ok(membership)

    async def create_service_account(
        self,
        *,
        actor: AuthenticatedPrincipal,
        workspace_id: UUID,
        name: str,
        description: str | None = None,
    ) -> Result[tuple[ServiceAccount, IssuedApiKey], DomainError]:
        guard = await self._require_role(actor, workspace_id, Role.ADMIN)
        if isinstance(guard, Err):
            return guard
        # The service account is a principal (so it can hold a membership and a personal
        # scope) whose id is shared with its catalog row, linking the two.
        account_id = uuid7()
        await self._uow.identity.create_principal(
            principal_id=account_id,
            kind=PrincipalKind.SERVICE_ACCOUNT,
            display_name=name,
            email=None,
            personal_group_id=f"u:{account_id}",
        )
        account = await self._uow.identity.create_service_account(
            service_account_id=account_id,
            owner_principal_id=actor.id,
            workspace_id=workspace_id,
            name=name,
            description=description,
        )
        await self._uow.identity.add_membership(
            principal_id=account_id,
            workspace_id=workspace_id,
            project_id=None,
            role=Role.MEMBER,
        )
        issued = await self._issue_api_key(account_id)
        return Ok((account, issued))

    async def issue_api_key(
        self, *, actor: AuthenticatedPrincipal, principal_id: UUID
    ) -> Result[IssuedApiKey, DomainError]:
        # A principal issues its own key. Sharing management of another principal's keys
        # is an admin flow deferred to a later phase.
        if actor.id != principal_id:
            return Err(Forbidden("a principal can only issue an API key for itself"))
        target = await self._uow.identity.get_principal(principal_id)
        if target is None:
            return Err(NotFound(f"principal {principal_id} does not exist"))
        return Ok(await self._issue_api_key(principal_id))

    async def revoke_credential(self, *, credential_id: UUID) -> Result[None, DomainError]:
        revoked = await self._uow.identity.revoke_credential(credential_id)
        if not revoked:
            return Err(NotFound(f"credential {credential_id} not found or already revoked"))
        return Ok(None)

    async def _issue_api_key(self, principal_id: UUID) -> IssuedApiKey:
        generated = generate_api_key()
        credential = await self._uow.identity.create_credential(
            principal_id=principal_id,
            service_account_id=None,
            kind=CredentialKind.API_KEY,
            key_prefix=generated.key_prefix,
            hashed_secret=generated.hashed_secret,
            expires_at=None,
        )
        return IssuedApiKey(
            credential_id=credential.id,
            principal_id=principal_id,
            api_key=generated.full_key,
        )

    async def _require_role(
        self, actor: AuthenticatedPrincipal, workspace_id: UUID, minimum: Role
    ) -> Result[Membership, DomainError]:
        membership = await self._uow.identity.find_membership(
            principal_id=actor.id, workspace_id=workspace_id
        )
        if membership is None or not role_at_least(membership.role, minimum):
            return Err(
                Forbidden(
                    f"principal {actor.id} needs role {minimum.value} on workspace {workspace_id}"
                )
            )
        return Ok(membership)
