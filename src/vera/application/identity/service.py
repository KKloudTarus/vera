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
from datetime import datetime
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
from vera.shared.errors import Conflict, DomainError, Err, Forbidden, NotFound, Ok, Result
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.security import generate_api_key, hash_secret, split_api_key


@dataclass(frozen=True, slots=True)
class IssuedApiKey:
    credential_id: UUID
    principal_id: UUID
    api_key: str  # plaintext, returned once and never stored


@dataclass(frozen=True, slots=True)
class BootstrapAdmin:
    """The outcome of ensuring the init admin exists. ``created`` is True only on the run
    that first minted it, so an operator can tell a fresh seed from a repeat.
    """

    principal_id: UUID
    workspace_id: UUID
    org_id: UUID
    created: bool


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

    async def provision_user(
        self,
        *,
        actor: AuthenticatedPrincipal,
        workspace_id: UUID,
        display_name: str,
        email: str | None = None,
        role: Role = Role.MEMBER,
    ) -> Result[tuple[Principal, IssuedApiKey], DomainError]:
        """Create a user principal, add it to the actor's workspace, and issue its first
        key, all in one admin-gated step. This is the closed-signup replacement for
        register: an admin hands out access instead of anyone minting their own.
        """
        guard = await self._require_role(actor, workspace_id, Role.ADMIN)
        if isinstance(guard, Err):
            return guard
        principal = await self.create_principal(display_name=display_name, email=email)
        await self._uow.identity.add_membership(
            principal_id=principal.id,
            workspace_id=workspace_id,
            project_id=None,
            role=role,
        )
        issued = await self._issue_api_key(principal.id)
        return Ok((principal, issued))

    async def ensure_admin(
        self,
        *,
        admin_api_key: str,
        admin_email: str,
        admin_display_name: str,
        org_slug: str,
        org_name: str,
        workspace_slug: str,
        workspace_name: str,
    ) -> Result[BootstrapAdmin, DomainError]:
        """Idempotently seed an initial admin that owns a root workspace and holds the
        given API key. Keyed by email (principal), slug (org and workspace), and the key's
        clear prefix (credential), so a repeat run changes nothing. Not authorization
        gated: it runs as an operator seed, out of band from the request path.
        """
        parts = split_api_key(admin_api_key)
        if parts is None:
            return Err(Conflict("bootstrap admin_api_key is malformed (want <prefix>.<secret>)"))
        key_prefix, secret = parts

        principal = await self._uow.identity.get_principal_by_email(admin_email)
        created = principal is None
        if principal is None:
            principal = await self.create_principal(
                display_name=admin_display_name, email=admin_email
            )

        org_id = deterministic_id("bootstrap-org", org_slug)
        if await self._uow.tenancy.get_organization(org_id) is None:
            await self._uow.tenancy.create_organization(
                org_id=org_id, slug=org_slug, name=org_name, group_id=f"o:{org_id}"
            )

        workspace_id = deterministic_id("bootstrap-workspace", workspace_slug)
        if await self._uow.tenancy.get_workspace(workspace_id) is None:
            await self._uow.tenancy.create_workspace(
                workspace_id=workspace_id,
                org_id=org_id,
                slug=workspace_slug,
                name=workspace_name,
                group_id=f"w:{workspace_id}",
            )

        membership = await self._uow.identity.find_membership(
            principal_id=principal.id, workspace_id=workspace_id
        )
        if membership is None:
            await self._uow.identity.add_membership(
                principal_id=principal.id,
                workspace_id=workspace_id,
                project_id=None,
                role=Role.OWNER,
            )

        existing = await self._uow.identity.get_credential_by_prefix(key_prefix)
        if existing is None:
            await self._uow.identity.create_credential(
                principal_id=principal.id,
                service_account_id=None,
                kind=CredentialKind.API_KEY,
                key_prefix=key_prefix,
                hashed_secret=hash_secret(secret),
                expires_at=None,
            )
        elif existing.principal_id != principal.id:
            return Err(Conflict(f"api key prefix {key_prefix} belongs to a different principal"))

        return Ok(
            BootstrapAdmin(
                principal_id=principal.id,
                workspace_id=workspace_id,
                org_id=org_id,
                created=created,
            )
        )

    async def issue_api_key(
        self,
        *,
        actor: AuthenticatedPrincipal,
        principal_id: UUID,
        expires_at: datetime | None = None,
    ) -> Result[IssuedApiKey, DomainError]:
        """Issue a key for the actor itself, or, for a workspace admin, for another
        principal that is a member of a workspace the actor administers.
        """
        guard = await self._may_manage(actor, principal_id)
        if isinstance(guard, Err):
            return guard
        target = await self._uow.identity.get_principal(principal_id)
        if target is None:
            return Err(NotFound(f"principal {principal_id} does not exist"))
        return Ok(await self._issue_api_key(principal_id, expires_at=expires_at))

    async def rotate_api_key(
        self,
        *,
        actor: AuthenticatedPrincipal,
        credential_id: UUID,
        expires_at: datetime | None = None,
    ) -> Result[IssuedApiKey, DomainError]:
        """Revoke a credential and issue a fresh one for the same principal, so a key can
        be replaced without a window where the principal has none.
        """
        credential = await self._uow.identity.get_credential(credential_id)
        if credential is None or credential.principal_id is None:
            return Err(NotFound(f"credential {credential_id} not found"))
        guard = await self._may_manage(actor, credential.principal_id)
        if isinstance(guard, Err):
            return guard
        revoked = await self._uow.identity.revoke_credential(credential_id)
        if not revoked:
            return Err(Conflict(f"credential {credential_id} was already revoked"))
        return Ok(await self._issue_api_key(credential.principal_id, expires_at=expires_at))

    async def revoke_credential(
        self, *, actor: AuthenticatedPrincipal, credential_id: UUID
    ) -> Result[None, DomainError]:
        credential = await self._uow.identity.get_credential(credential_id)
        if credential is None or credential.principal_id is None:
            return Err(NotFound(f"credential {credential_id} not found"))
        guard = await self._may_manage(actor, credential.principal_id)
        if isinstance(guard, Err):
            return guard
        revoked = await self._uow.identity.revoke_credential(credential_id)
        if not revoked:
            return Err(Conflict(f"credential {credential_id} was already revoked"))
        return Ok(None)

    async def _issue_api_key(
        self, principal_id: UUID, *, expires_at: datetime | None = None
    ) -> IssuedApiKey:
        generated = generate_api_key()
        credential = await self._uow.identity.create_credential(
            principal_id=principal_id,
            service_account_id=None,
            kind=CredentialKind.API_KEY,
            key_prefix=generated.key_prefix,
            hashed_secret=generated.hashed_secret,
            expires_at=expires_at,
        )
        return IssuedApiKey(
            credential_id=credential.id,
            principal_id=principal_id,
            api_key=generated.full_key,
        )

    async def _may_manage(
        self, actor: AuthenticatedPrincipal, principal_id: UUID
    ) -> Result[None, DomainError]:
        """The actor may manage the target's credentials if it is the target, or an admin
        on a workspace the target belongs to.
        """
        if actor.id == principal_id:
            return Ok(None)
        for membership in await self._uow.identity.list_memberships(principal_id):
            actor_membership = await self._uow.identity.find_membership(
                principal_id=actor.id, workspace_id=membership.workspace_id
            )
            if actor_membership is not None and role_at_least(actor_membership.role, Role.ADMIN):
                return Ok(None)
        return Err(Forbidden(f"principal {actor.id} may not manage credentials for {principal_id}"))

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
