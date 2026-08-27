"""Repository ports. Each is a collection of one aggregate, scoped to the current
Unit of Work. Repositories add and read; they never commit.
"""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from vera.domain.identity.models import Organization, Project, Workspace
from vera.domain.knowledge.models import CanonicalEntity
from vera.shared.types import JsonDict


class TenancyRepository(Protocol):
    async def create_organization(
        self, *, slug: str, name: str, group_id: str, org_id: UUID | None = None
    ) -> Organization: ...

    async def create_workspace(
        self,
        *,
        org_id: UUID,
        slug: str,
        name: str,
        group_id: str,
        workspace_id: UUID | None = None,
    ) -> Workspace: ...

    async def create_project(
        self,
        *,
        workspace_id: UUID,
        slug: str,
        name: str,
        group_id: str,
        project_id: UUID | None = None,
    ) -> Project: ...

    async def get_project(self, project_id: UUID) -> Project | None: ...

    async def get_workspace(self, workspace_id: UUID) -> Workspace | None: ...


class OutboxRepository(Protocol):
    async def add(
        self,
        *,
        group_id: str,
        source_id: str,
        dedup_uuid: UUID,
        payload: JsonDict,
        trace_context: JsonDict | None = None,
    ) -> None:
        """Insert an ingestion job in the current transaction (idempotent by dedup_uuid)."""
        ...


class CanonicalEntityRepository(Protocol):
    async def create(
        self,
        *,
        group_id: str,
        entity_type: str,
        canonical_name: str,
        aliases: list[str],
        embedding: list[float] | None = None,
    ) -> CanonicalEntity: ...

    async def resolve(self, *, group_id: str, name: str) -> CanonicalEntity | None:
        """Resolve a surface form to a canonical entity: exact-normalized, then fuzzy."""
        ...

    async def add_alias(self, *, entity_id: UUID, group_id: str, alias: str) -> None:
        """Attach a surface form to an existing canonical entity (idempotent)."""
        ...

    async def candidates_with_embeddings(
        self, *, group_id: str, entity_type: str
    ) -> list[tuple[CanonicalEntity, list[float]]]:
        """Entities in the group/type that carry a name embedding, for semantic linking."""
        ...

    async def without_embeddings(self, *, group_id: str) -> list[CanonicalEntity]:
        """Entities in the group that have no name embedding yet, for a backfill run."""
        ...

    async def set_embedding(self, *, entity_id: UUID, embedding: list[float]) -> None:
        """Store a canonical-name embedding on an existing entity."""
        ...
