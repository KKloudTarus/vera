"""PostgreSQL source of truth for derived community fact lineage."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.community import CommunityFactLineageRow
from vera.domain.ports.community import (
    CommunityFact,
    CommunityLineageItem,
    CommunityLineagePage,
)

_ACTIVE_FACTS = text(
    "SELECT f.id, f.fact_key, s.canonical_name AS subject_name, f.predicate, "
    "COALESCE(o.canonical_name, f.object_scalar) AS object_name "
    "FROM facts f JOIN canonical_entities s "
    "ON s.id = f.subject_entity_id AND s.group_id = f.group_id "
    "LEFT JOIN canonical_entities o ON o.id = f.object_entity_id AND o.group_id = f.group_id "
    "WHERE f.group_id = :group AND f.lifecycle_state = 'active' ORDER BY f.id"
)
_PAGE = text(
    "WITH selected_run AS ("
    "  SELECT COALESCE(CAST(:run AS uuid), ("
    "    SELECT derivation_run_id FROM community_fact_lineage "
    "    WHERE community_id = CAST(:community AS uuid) "
    "      AND group_id = ANY(CAST(:groups AS text[])) "
    "    ORDER BY created_at DESC, derivation_run_id DESC LIMIT 1"
    "  )) AS id"
    ") "
    "SELECT l.community_id, l.derivation_run_id, l.fact_id, f.fact_key, "
    "s.canonical_name AS subject_name, f.predicate, "
    "COALESCE(o.canonical_name, f.object_scalar) AS object_name, l.created_at "
    "FROM community_fact_lineage l "
    "JOIN selected_run r ON r.id = l.derivation_run_id "
    "JOIN facts f ON f.id = l.fact_id AND f.group_id = l.group_id "
    "JOIN canonical_entities s ON s.id = f.subject_entity_id AND s.group_id = f.group_id "
    "LEFT JOIN canonical_entities o ON o.id = f.object_entity_id AND o.group_id = f.group_id "
    "WHERE l.community_id = CAST(:community AS uuid) "
    "AND l.group_id = ANY(CAST(:groups AS text[])) "
    "AND (CAST(:cursor AS uuid) IS NULL OR l.fact_id > CAST(:cursor AS uuid)) "
    "ORDER BY l.fact_id LIMIT :limit"
)


class SqlAlchemyCommunityLineageRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = session_factory

    async def active_facts(self, *, group_id: str) -> tuple[CommunityFact, ...]:
        async with self._sessions() as session:
            rows = (await session.execute(_ACTIVE_FACTS, {"group": group_id})).mappings().all()
        return tuple(
            CommunityFact(
                fact_id=row["id"],
                fact_key=str(row["fact_key"]),
                subject_name=str(row["subject_name"]),
                predicate=str(row["predicate"]),
                object_name=str(row["object_name"] or ""),
            )
            for row in rows
        )

    async def record(
        self,
        *,
        group_id: str,
        community_id: UUID,
        derivation_run_id: UUID,
        fact_ids: tuple[UUID, ...],
    ) -> None:
        if not fact_ids:
            return
        values = [
            {
                "group_id": group_id,
                "community_id": community_id,
                "derivation_run_id": derivation_run_id,
                "fact_id": fact_id,
            }
            for fact_id in fact_ids
        ]
        async with self._sessions() as session, session.begin():
            await session.execute(
                pg_insert(CommunityFactLineageRow).values(values).on_conflict_do_nothing()
            )

    async def page(
        self,
        *,
        group_ids: tuple[str, ...],
        community_id: UUID,
        derivation_run_id: UUID | None,
        cursor: UUID | None,
        limit: int,
    ) -> CommunityLineagePage:
        async with self._sessions() as session:
            rows = (
                (
                    await session.execute(
                        _PAGE,
                        {
                            "groups": list(group_ids),
                            "community": str(community_id),
                            "run": str(derivation_run_id) if derivation_run_id else None,
                            "cursor": str(cursor) if cursor else None,
                            "limit": limit + 1,
                        },
                    )
                )
                .mappings()
                .all()
            )
        page_rows = rows[:limit]
        items = tuple(
            CommunityLineageItem(
                community_id=row["community_id"],
                derivation_run_id=row["derivation_run_id"],
                fact_id=row["fact_id"],
                fact_key=str(row["fact_key"]),
                subject_name=str(row["subject_name"]),
                predicate=str(row["predicate"]),
                object_name=str(row["object_name"] or ""),
                created_at=row["created_at"],
            )
            for row in page_rows
        )
        next_cursor = str(page_rows[-1]["fact_id"]) if len(rows) > limit and page_rows else None
        return CommunityLineagePage(items=items, next_cursor=next_cursor)
