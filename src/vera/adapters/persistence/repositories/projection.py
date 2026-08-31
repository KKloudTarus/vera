"""The authoritative side of the graph projection: read the active fact set from Postgres.

Like the retrieval read model, this runs on a trusted connection and filters ``group_id``
explicitly rather than switching the RLS tenant, because a rebuild assembles one group's
facts with their subject/object names and supporting episode ids in a single query.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.domain.ports.projection import ProjectedFact

_ACTIVE_FACTS = text(
    """
    SELECT
        f.fact_key AS fact_key,
        cs.canonical_name AS subject_name,
        f.predicate AS predicate,
        COALESCE(co.canonical_name, f.object_scalar) AS object_name,
        f.authority AS authority,
        f.confidence AS confidence,
        f.valid_from AS valid_from,
        f.valid_to AS valid_to,
        sup.episodes AS episodes
    FROM facts f
    JOIN canonical_entities cs ON cs.id = f.subject_entity_id
    LEFT JOIN canonical_entities co ON co.id = f.object_entity_id
    LEFT JOIN LATERAL (
        SELECT array_agg(DISTINCT pe.id::text ORDER BY pe.id::text) AS episodes
        FROM assertions a
        JOIN published_episodes pe
          ON pe.group_id = a.group_id
         AND a.run_key IN ('episode:' || pe.source_id, 'backfill:' || pe.source_id)
        WHERE a.group_id = f.group_id
          AND a.fact_id = f.id
          AND a.state = 'active'
          AND a.polarity = 'supports'
    ) sup ON true
    WHERE f.group_id = :g AND f.lifecycle_state = 'active'
    """
)

_ACTIVE_KEYS = text("SELECT fact_key FROM facts WHERE group_id = :g AND lifecycle_state = 'active'")


class SqlAlchemyProjectionSource:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def active_facts(self, *, group_id: str) -> list[ProjectedFact]:
        async with self._session_factory() as session:
            rows = (await session.execute(_ACTIVE_FACTS, {"g": group_id})).mappings().all()
        facts: list[ProjectedFact] = []
        for row in rows:
            object_name = row["object_name"] or ""
            episodes: Any = row["episodes"] or []
            facts.append(
                ProjectedFact(
                    group_id=group_id,
                    fact_key=row["fact_key"],
                    subject_name=row["subject_name"],
                    predicate=row["predicate"],
                    object_name=object_name,
                    fact_text=f"{row['subject_name']} {row['predicate']} {object_name}".strip(),
                    authority=float(row["authority"]),
                    confidence=float(row["confidence"]),
                    valid_at=row["valid_from"],
                    invalid_at=row["valid_to"],
                    supporting_episode_ids=tuple(str(e) for e in episodes),
                )
            )
        return facts

    async def active_fact_keys(self, *, group_id: str) -> set[str]:
        async with self._session_factory() as session:
            rows = await session.scalars(_ACTIVE_KEYS, {"g": group_id})
        return {str(k) for k in rows}
