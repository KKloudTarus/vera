"""Graph-map repository: links Graphiti node and edge uuids to VERA durable keys.

The map is a rebuildable index, so writes are idempotent by the (group_id, uuid)
unique constraints. It also gives retraction a target: which graph objects a
published episode produced.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.canonical import GraphEdgeMapRow, GraphNodeMapRow


class SqlAlchemyGraphMapRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record_node(
        self,
        *,
        group_id: str,
        node_uuid: UUID,
        canonical_entity_id: UUID | None,
        published_episode_id: UUID | None = None,
    ) -> None:
        stmt = (
            pg_insert(GraphNodeMapRow)
            .values(
                group_id=group_id,
                node_uuid=node_uuid,
                canonical_entity_id=canonical_entity_id,
                published_episode_id=published_episode_id,
            )
            .on_conflict_do_nothing(constraint="uq_node_map")
        )
        await self._session.execute(stmt)

    async def record_edge(
        self,
        *,
        group_id: str,
        edge_uuid: UUID,
        published_episode_id: UUID | None = None,
    ) -> None:
        stmt = (
            pg_insert(GraphEdgeMapRow)
            .values(
                group_id=group_id,
                edge_uuid=edge_uuid,
                published_episode_id=published_episode_id,
            )
            .on_conflict_do_nothing(constraint="uq_edge_map")
        )
        await self._session.execute(stmt)
