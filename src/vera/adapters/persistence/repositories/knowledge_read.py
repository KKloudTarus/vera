"""Read model for the generic knowledge contracts (Phase 6).

Cross-scope reads over the authoritative fact store, on a trusted connection with an explicit
``group_id = ANY(...)`` filter over the server-resolved scopes (never a client-chosen scope).
Returns plain dicts so the API and MCP surfaces can serialize them directly.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_FACT = text(
    "SELECT f.id::text AS fact_id, f.fact_key, f.group_id, cs.canonical_name AS subject, "
    "f.predicate, COALESCE(co.canonical_name, f.object_scalar) AS object, f.qualifiers, "
    "f.lifecycle_state, f.authority, f.confidence, f.valid_from, f.valid_to "
    "FROM facts f JOIN canonical_entities cs ON cs.id = f.subject_entity_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
    "WHERE f.group_id = ANY(CAST(:gids AS text[])) AND f.fact_key = :fk "
    "ORDER BY f.system_from DESC LIMIT 1"
)
_ASSERTIONS = text(
    "SELECT a.id::text AS assertion_id, a.polarity, a.verification_state, a.source_authority, "
    "a.extractor_confidence, a.knowledge_source_id::text AS source_id, "
    "a.artifact_version_id::text AS artifact_version_id, a.recorded_at "
    "FROM assertions a WHERE a.group_id = ANY(CAST(:gids AS text[])) "
    "AND a.fact_id = CAST(:fid AS uuid) AND a.state = 'active' "
    "ORDER BY a.recorded_at DESC"
)
_EVIDENCE = text(
    "SELECT e.id::text AS evidence_id, e.excerpt, e.citation_uri, e.chunk_id::text AS chunk_id, "
    "e.artifact_version_id::text AS artifact_version_id, e.confidentiality "
    "FROM evidence e WHERE e.group_id = ANY(CAST(:gids AS text[])) "
    "AND e.assertion_id = CAST(:aid AS uuid)"
)
_RELATIONS = text(
    "SELECT r.relation_type, r.to_fact_id::text AS to_fact_id "
    "FROM fact_relations r WHERE r.group_id = ANY(CAST(:gids AS text[])) "
    "AND r.from_fact_id = CAST(:fid AS uuid)"
)
_CHANGES = text(
    "SELECT event_type, occurred_at, actor, source_id, fact_id::text AS fact_id, reason "
    "FROM knowledge_events WHERE group_id = ANY(CAST(:gids AS text[])) "
    "ORDER BY occurred_at DESC LIMIT :lim"
)
_CONFLICTS = text(
    "SELECT f.fact_key, cs.canonical_name AS subject, f.predicate, "
    "COALESCE(co.canonical_name, f.object_scalar) AS object, f.slot_key, f.authority "
    "FROM facts f JOIN canonical_entities cs ON cs.id = f.subject_entity_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
    "WHERE f.group_id = ANY(CAST(:gids AS text[])) AND f.lifecycle_state = 'disputed' "
    "ORDER BY f.updated_at DESC LIMIT :lim"
)
_REVIEW = text(
    "SELECT f.fact_key, f.group_id, cs.canonical_name AS subject, f.predicate, "
    "COALESCE(co.canonical_name, f.object_scalar) AS object, f.authority, f.confidence, "
    "f.created_at "
    "FROM facts f JOIN canonical_entities cs ON cs.id = f.subject_entity_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
    "WHERE f.group_id = ANY(CAST(:gids AS text[])) AND f.lifecycle_state = 'proposed' "
    "ORDER BY f.created_at DESC LIMIT :lim"
)
_TIMELINE = text(
    "SELECT event_type, occurred_at, actor, reason FROM knowledge_events "
    "WHERE group_id = ANY(CAST(:gids AS text[])) AND fact_id IN ("
    "  SELECT id FROM facts WHERE group_id = ANY(CAST(:gids AS text[])) AND fact_key = :fk"
    ") ORDER BY occurred_at ASC LIMIT :lim"
)


class SqlAlchemyKnowledgeReadModel:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_fact(self, *, group_ids: list[str], fact_key: str) -> dict[str, Any] | None:
        async with self._session_factory() as session:
            row = (
                (await session.execute(_FACT, {"gids": group_ids, "fk": fact_key}))
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            fact = dict(row)
            relations = (
                (await session.execute(_RELATIONS, {"gids": group_ids, "fid": fact["fact_id"]}))
                .mappings()
                .all()
            )
            fact["relations"] = [dict(r) for r in relations]
            return fact

    async def explain_fact(self, *, group_ids: list[str], fact_key: str) -> dict[str, Any] | None:
        async with self._session_factory() as session:
            row = (
                (await session.execute(_FACT, {"gids": group_ids, "fk": fact_key}))
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            fact = dict(row)
            assertion_rows = (
                (await session.execute(_ASSERTIONS, {"gids": group_ids, "fid": fact["fact_id"]}))
                .mappings()
                .all()
            )
            assertions: list[dict[str, Any]] = []
            for a in assertion_rows:
                assertion = dict(a)
                ev = (
                    (
                        await session.execute(
                            _EVIDENCE, {"gids": group_ids, "aid": assertion["assertion_id"]}
                        )
                    )
                    .mappings()
                    .all()
                )
                assertion["evidence"] = [dict(e) for e in ev]
                assertions.append(assertion)
            fact["assertions"] = assertions
            return fact

    async def recent_changes(
        self, *, group_ids: list[str], limit: int = 50
    ) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                (await session.execute(_CHANGES, {"gids": group_ids, "lim": limit}))
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]

    async def conflicts(self, *, group_ids: list[str], limit: int = 50) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                (await session.execute(_CONFLICTS, {"gids": group_ids, "lim": limit}))
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]

    async def review_queue(self, *, group_ids: list[str], limit: int = 50) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                (await session.execute(_REVIEW, {"gids": group_ids, "lim": limit})).mappings().all()
            )
        return [dict(r) for r in rows]

    async def fact_timeline(
        self, *, group_ids: list[str], fact_key: str, limit: int = 200
    ) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                (
                    await session.execute(
                        _TIMELINE, {"gids": group_ids, "fk": fact_key, "lim": limit}
                    )
                )
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]
