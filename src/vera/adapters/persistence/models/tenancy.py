"""Tenancy tables: organizations, workspaces, projects.

Each scope carries the opaque Graphiti ``group_id`` for its shared memory. Clients
never choose it; VERA assigns it (for example ``o:{id}``, ``w:{id}``, ``p:{id}``).
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from vera.adapters.persistence.base import Base
from vera.adapters.persistence.models._mixins import UUIDPK, Timestamps


class OrganizationRow(Base, UUIDPK, Timestamps):
    __tablename__ = "organizations"

    slug: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    group_id: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)


class WorkspaceRow(Base, UUIDPK, Timestamps):
    __tablename__ = "workspaces"

    org_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    slug: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    group_id: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)

    __table_args__ = (UniqueConstraint("org_id", "slug", name="uq_workspace_slug"),)


class ProjectRow(Base, UUIDPK, Timestamps):
    __tablename__ = "projects"

    workspace_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False
    )
    slug: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    group_id: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)

    __table_args__ = (UniqueConstraint("workspace_id", "slug", name="uq_project_slug"),)
