"""Tenancy repository: organizations, workspaces, projects."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.tenancy import (
    OrganizationRow,
    ProjectRow,
    WorkspaceRow,
)
from vera.domain.identity.models import Organization, Project, Workspace


def _to_workspace(row: WorkspaceRow) -> Workspace:
    return Workspace(
        id=row.id, org_id=row.org_id, slug=row.slug, name=row.name, group_id=row.group_id
    )


def _to_project(row: ProjectRow) -> Project:
    return Project(
        id=row.id,
        workspace_id=row.workspace_id,
        slug=row.slug,
        name=row.name,
        group_id=row.group_id,
    )


class SqlAlchemyTenancyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_organization(
        self, *, slug: str, name: str, group_id: str, org_id: UUID | None = None
    ) -> Organization:
        row = OrganizationRow(slug=slug, name=name, group_id=group_id)
        if org_id is not None:
            row.id = org_id
        self._session.add(row)
        await self._session.flush()
        return Organization(id=row.id, slug=row.slug, name=row.name, group_id=row.group_id)

    async def create_workspace(
        self,
        *,
        org_id: UUID,
        slug: str,
        name: str,
        group_id: str,
        workspace_id: UUID | None = None,
    ) -> Workspace:
        row = WorkspaceRow(org_id=org_id, slug=slug, name=name, group_id=group_id)
        if workspace_id is not None:
            row.id = workspace_id
        self._session.add(row)
        await self._session.flush()
        return _to_workspace(row)

    async def create_project(
        self,
        *,
        workspace_id: UUID,
        slug: str,
        name: str,
        group_id: str,
        project_id: UUID | None = None,
    ) -> Project:
        row = ProjectRow(workspace_id=workspace_id, slug=slug, name=name, group_id=group_id)
        if project_id is not None:
            row.id = project_id
        self._session.add(row)
        await self._session.flush()
        return _to_project(row)

    async def get_project(self, project_id: UUID) -> Project | None:
        row = await self._session.get(ProjectRow, project_id)
        return _to_project(row) if row is not None else None

    async def get_workspace(self, workspace_id: UUID) -> Workspace | None:
        row = await self._session.get(WorkspaceRow, workspace_id)
        return _to_workspace(row) if row is not None else None

    async def get_organization(self, org_id: UUID) -> Organization | None:
        row = await self._session.get(OrganizationRow, org_id)
        if row is None:
            return None
        return Organization(id=row.id, slug=row.slug, name=row.name, group_id=row.group_id)
