"""ScopeResolutionService: the application-facing resolver of readable scopes.

Turns an authenticated principal into the group_ids it may read. Reads and proposals
run only against these, so a principal in one workspace cannot reach another's memory.
It depends on the ``ScopeResolver`` port, so the storage detail stays in an adapter.
"""

from __future__ import annotations

from uuid import UUID

from vera.domain.identity.models import Role, role_at_least
from vera.domain.identity.scopes import ScopeKind, scope_kind
from vera.domain.ports.identity import ResolvedScope, ScopeResolver


class ScopeResolutionService:
    def __init__(self, resolver: ScopeResolver) -> None:
        self._resolver = resolver

    async def resolve(self, principal_id: UUID) -> ResolvedScope | None:
        return await self._resolver.resolve(principal_id)

    async def allowed_group_ids(self, principal_id: UUID) -> tuple[str, ...]:
        scope = await self._resolver.resolve(principal_id)
        return scope.group_ids if scope is not None else ()

    async def can_read(self, principal_id: UUID, group_id: str) -> bool:
        return group_id in await self.allowed_group_ids(principal_id)

    async def role_for(self, principal_id: UUID, group_id: str) -> Role | None:
        return await self._resolver.role_for(principal_id, group_id)

    async def can_administer(self, principal_id: UUID, group_id: str) -> bool:
        """Whether the principal may perform destructive or governance actions on the group:
        it requires ADMIN or higher, never mere read access.
        """
        role = await self._resolver.role_for(principal_id, group_id)
        return role is not None and role_at_least(role, Role.ADMIN)

    async def shared_group_ids(self, principal_id: UUID) -> tuple[str, ...]:
        """Only the org/workspace/project scopes, excluding the personal scope."""
        allowed = await self.allowed_group_ids(principal_id)
        return tuple(g for g in allowed if scope_kind(g) is not ScopeKind.PERSONAL)
