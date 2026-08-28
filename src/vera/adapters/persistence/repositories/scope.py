"""Scope resolver: a principal's readable group_ids from its memberships.

A workspace membership grants the org, the workspace, and every project in it. A
project membership grants the org, the workspace, and that project. Everyone gets
their own personal scope.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.identity import MembershipRow, PrincipalRow
from vera.adapters.persistence.models.tenancy import (
    OrganizationRow,
    ProjectRow,
    WorkspaceRow,
)
from vera.domain.identity.models import Role
from vera.domain.ports.identity import ResolvedScope

# Highest role first, so the first match is the principal's effective role for a group.
_ROLE_ORDER = (Role.OWNER, Role.ADMIN, Role.MEMBER, Role.VIEWER)


class SqlAlchemyScopeResolver:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def resolve(self, principal_id: UUID) -> ResolvedScope | None:
        async with self._session_factory() as session:
            personal = await session.scalar(
                select(PrincipalRow.personal_group_id).where(PrincipalRow.id == principal_id)
            )
            if personal is None:
                return None

            membership_rows = (
                await session.execute(
                    select(
                        MembershipRow.workspace_id,
                        MembershipRow.project_id,
                        WorkspaceRow.group_id,
                        OrganizationRow.group_id,
                        ProjectRow.group_id,
                    )
                    .join(WorkspaceRow, WorkspaceRow.id == MembershipRow.workspace_id)
                    .join(OrganizationRow, OrganizationRow.id == WorkspaceRow.org_id)
                    .outerjoin(ProjectRow, ProjectRow.id == MembershipRow.project_id)
                    .where(MembershipRow.principal_id == principal_id)
                )
            ).all()

            groups: set[str] = {personal}
            workspace_wide: list[UUID] = []
            primary_workspace_id: UUID | None = None
            for workspace_id, project_id, ws_group, org_group, proj_group in membership_rows:
                primary_workspace_id = primary_workspace_id or workspace_id
                groups.add(org_group)
                groups.add(ws_group)
                if proj_group is not None:
                    groups.add(proj_group)
                if project_id is None:
                    workspace_wide.append(workspace_id)

            if workspace_wide:
                project_groups = (
                    (
                        await session.execute(
                            select(ProjectRow.group_id).where(
                                ProjectRow.workspace_id.in_(workspace_wide)
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                groups.update(project_groups)

        return ResolvedScope(
            group_ids=tuple(sorted(groups)),
            personal_group_id=personal,
            primary_workspace_id=primary_workspace_id,
        )

    async def role_for(self, principal_id: UUID, group_id: str) -> Role | None:
        async with self._session_factory() as session:
            personal = await session.scalar(
                select(PrincipalRow.personal_group_id).where(PrincipalRow.id == principal_id)
            )
            if personal is not None and personal == group_id:
                return Role.OWNER  # a principal owns its own personal scope

            rows = (
                await session.execute(
                    select(
                        MembershipRow.role,
                        MembershipRow.workspace_id,
                        MembershipRow.project_id,
                        WorkspaceRow.group_id,
                        OrganizationRow.group_id,
                        ProjectRow.group_id,
                    )
                    .join(WorkspaceRow, WorkspaceRow.id == MembershipRow.workspace_id)
                    .join(OrganizationRow, OrganizationRow.id == WorkspaceRow.org_id)
                    .outerjoin(ProjectRow, ProjectRow.id == MembershipRow.project_id)
                    .where(MembershipRow.principal_id == principal_id)
                )
            ).all()

            granting_roles: set[str] = set()
            workspace_wide: list[UUID] = []
            for role, workspace_id, project_id, ws_group, org_group, proj_group in rows:
                grants = {org_group, ws_group}
                if proj_group is not None:
                    grants.add(proj_group)
                if group_id in grants:
                    granting_roles.add(role)
                if project_id is None:
                    workspace_wide.append(workspace_id)

            if not granting_roles and workspace_wide:
                # A workspace-wide membership also grants every project in that workspace.
                target_ws = await session.scalar(
                    select(ProjectRow.workspace_id).where(ProjectRow.group_id == group_id)
                )
                if target_ws in workspace_wide:
                    granting_roles.update(
                        role
                        for role, ws_id, project_id, *_ in rows
                        if project_id is None and ws_id == target_ws
                    )

        for candidate in _ROLE_ORDER:
            if candidate.value in granting_roles:
                return candidate
        return None
