"""Scope resolver: a principal's readable group_ids from its memberships.

A workspace membership grants the org, the workspace, and every project in it. A
project membership grants the org, the workspace, and that project. Everyone gets
their own personal scope.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select, text
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

_RESOLVE_SCOPE = """
WITH principal AS (
    SELECT personal_group_id
    FROM principals
    WHERE id = :principal_id
), membership_scope AS (
    SELECT m.workspace_id, scope.group_id
    FROM memberships m
    JOIN workspaces w ON w.id = m.workspace_id
    JOIN organizations o ON o.id = w.org_id
    CROSS JOIN LATERAL (VALUES (w.group_id), (o.group_id)) scope(group_id)
    WHERE m.principal_id = :principal_id
    UNION ALL
    SELECT m.workspace_id, p.group_id
    FROM memberships m
    JOIN projects p ON p.id = m.project_id
    WHERE m.principal_id = :principal_id
    UNION ALL
    SELECT m.workspace_id, p.group_id
    FROM memberships m
    JOIN projects p ON p.workspace_id = m.workspace_id
    WHERE m.principal_id = :principal_id AND m.project_id IS NULL
), all_scope AS (
    SELECT NULL::uuid AS workspace_id, personal_group_id AS group_id
    FROM principal
    UNION ALL
    SELECT workspace_id, group_id
    FROM membership_scope
)
SELECT (SELECT personal_group_id FROM principal) AS personal_group_id,
       (SELECT workspace_id FROM membership_scope ORDER BY workspace_id LIMIT 1)
           AS primary_workspace_id,
       array_agg(DISTINCT group_id ORDER BY group_id)
           FILTER (WHERE group_id IS NOT NULL) AS group_ids
FROM all_scope
"""


class SqlAlchemyScopeResolver:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def resolve(self, principal_id: UUID) -> ResolvedScope | None:
        async with self._session_factory() as session:
            row = (
                (await session.execute(text(_RESOLVE_SCOPE), {"principal_id": principal_id}))
                .mappings()
                .one()
            )
        personal = row["personal_group_id"]
        if personal is None:
            return None

        return ResolvedScope(
            group_ids=tuple(row["group_ids"] or ()),
            personal_group_id=personal,
            primary_workspace_id=row["primary_workspace_id"],
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
