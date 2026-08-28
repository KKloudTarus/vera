"""Snapshot and context-pack persistence (Phase 5).

Runs on a trusted connection with explicit ``group_id`` filters (like the retrieval read
model). Snapshot creation captures the active fact revisions, the source-revision
boundaries, and a SNAPSHOT_CREATED event in one transaction; pack save persists the pack and
a CONTEXT_PACK_CREATED event in one transaction, so the ledger never diverges from the row.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.domain.ports.snapshot import ContextPack, Snapshot
from vera.shared.types import JsonDict

_INSERT_SNAPSHOT = text(
    "INSERT INTO knowledge_snapshots "
    "(group_id, policy_version, as_of_valid_time, ontology_version_id, "
    "embedding_version, retrieval_index_version, graph_projection_checkpoint) "
    "VALUES (:g, :pv, COALESCE(:vt, now()), :ov, CAST(:ev AS jsonb), :iv, :gc) "
    "RETURNING id, created_at, frozen_at_system_time, as_of_valid_time"
)
_CAPTURE_ACTIVE = text(
    "INSERT INTO snapshot_facts (snapshot_id, fact_id, group_id) "
    "SELECT :sid, id, group_id FROM facts WHERE group_id = :g AND lifecycle_state = 'active'"
)
_CAPTURE_AS_OF = text(
    "INSERT INTO snapshot_facts (snapshot_id, fact_id, group_id) "
    "SELECT :sid, id, group_id FROM facts "
    "WHERE group_id = :g AND lifecycle_state NOT IN ('proposed', 'retracted') "
    "AND (valid_from IS NULL OR valid_from <= :vt) "
    "AND (valid_to IS NULL OR valid_to > :vt)"
)
_COUNT_FACTS = text("SELECT count(*) FROM snapshot_facts WHERE snapshot_id = :sid")
_SOURCE_BOUNDARIES = text(
    "SELECT coalesce(jsonb_object_agg(src, ver), '{}'::jsonb) FROM ("
    "  SELECT DISTINCT ON (a.knowledge_source_id) a.knowledge_source_id::text AS src, "
    "         a.artifact_version_id::text AS ver "
    "  FROM assertions a "
    "  WHERE a.group_id = :g AND a.state = 'active' "
    "    AND a.knowledge_source_id IS NOT NULL AND a.artifact_version_id IS NOT NULL "
    "  ORDER BY a.knowledge_source_id, a.recorded_at DESC"
    ") t"
)
_GRAPH_CHECKPOINT = text(
    "SELECT id FROM ingestion_jobs WHERE group_id = :g AND status = 'done' "
    "AND payload->>'job_kind' = 'project_facts' ORDER BY created_at DESC, id DESC LIMIT 1"
)
_SET_META = text(
    "UPDATE knowledge_snapshots SET fact_count = :n, source_boundaries = CAST(:sb AS jsonb) "
    "WHERE id = :sid"
)
_GET_SNAPSHOT = text(
    "SELECT id, group_id, created_at, frozen_at_system_time, as_of_valid_time, policy_version, "
    "fact_count, ontology_version_id, source_boundaries, embedding_version, "
    "retrieval_index_version, graph_projection_checkpoint "
    "FROM knowledge_snapshots WHERE id = :sid AND group_id = :g"
)
_SNAPSHOT_FACT_IDS = text(
    "SELECT fact_id::text FROM snapshot_facts WHERE snapshot_id = :sid AND group_id = :g"
)
_EVENT = text(
    "INSERT INTO knowledge_events (group_id, event_type, actor, next_state, policy_version) "
    "VALUES (:g, :et, :actor, CAST(:ns AS jsonb), :pv)"
)

_INSERT_PACK = text(
    "INSERT INTO context_packs (group_id, snapshot_id, query, hints, token_estimate, "
    "result_count, omitted, conflicts, freshness_warnings, results) "
    "VALUES (:g, :sid, :q, CAST(:hints AS jsonb), :tokens, :rc, :om, :cf, :fw, "
    "CAST(:res AS jsonb)) "
    "RETURNING id, created_at"
)
_GET_PACK = text(
    "SELECT id, group_id, snapshot_id, created_at, query, token_estimate, result_count, "
    "omitted, conflicts, freshness_warnings, results "
    "FROM context_packs WHERE id = :pid AND group_id = :g"
)


class SqlAlchemySnapshotRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def create(
        self,
        *,
        group_id: str,
        policy_version: str,
        as_of: datetime | None = None,
        ontology_version_id: str | None = None,
        embedding_version: JsonDict | None = None,
        retrieval_index_version: str = "fts-v1",
        actor: str | None = None,
    ) -> Snapshot:
        async with self._session_factory() as session, session.begin():
            graph_checkpoint = await session.scalar(_GRAPH_CHECKPOINT, {"g": group_id})
            row = (
                await session.execute(
                    _INSERT_SNAPSHOT,
                    {
                        "g": group_id,
                        "pv": policy_version,
                        "vt": as_of,
                        "ov": ontology_version_id,
                        "ev": json.dumps(embedding_version or {}),
                        "iv": retrieval_index_version,
                        "gc": graph_checkpoint,
                    },
                )
            ).one()
            snapshot_id = row.id
            capture = _CAPTURE_AS_OF if as_of is not None else _CAPTURE_ACTIVE
            await session.execute(
                capture,
                {"sid": snapshot_id, "g": group_id, "vt": row.as_of_valid_time},
            )
            count = await session.scalar(_COUNT_FACTS, {"sid": snapshot_id}) or 0
            boundaries: JsonDict = cast(
                "JsonDict", await session.scalar(_SOURCE_BOUNDARIES, {"g": group_id}) or {}
            )
            await session.execute(
                _SET_META, {"n": count, "sb": json.dumps(boundaries), "sid": snapshot_id}
            )
            await session.execute(
                _EVENT,
                {
                    "g": group_id,
                    "et": "SNAPSHOT_CREATED",
                    "actor": actor,
                    "ns": json.dumps({"snapshot_id": str(snapshot_id), "fact_count": count}),
                    "pv": policy_version,
                },
            )
            return Snapshot(
                id=str(snapshot_id),
                group_id=group_id,
                created_at=row.created_at,
                frozen_at_system_time=row.frozen_at_system_time,
                as_of_valid_time=row.as_of_valid_time,
                policy_version=policy_version,
                fact_count=count,
                ontology_version_id=ontology_version_id,
                source_boundaries=dict(boundaries),
                embedding_version=dict(embedding_version or {}),
                retrieval_index_version=retrieval_index_version,
                graph_projection_checkpoint=str(graph_checkpoint) if graph_checkpoint else None,
            )

    async def get(self, *, group_id: str, snapshot_id: str) -> Snapshot | None:
        async with self._session_factory() as session:
            row = (
                (await session.execute(_GET_SNAPSHOT, {"sid": snapshot_id, "g": group_id}))
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return Snapshot(
            id=str(row["id"]),
            group_id=row["group_id"],
            created_at=row["created_at"],
            frozen_at_system_time=row["frozen_at_system_time"],
            as_of_valid_time=row["as_of_valid_time"],
            policy_version=row["policy_version"],
            fact_count=row["fact_count"],
            ontology_version_id=str(row["ontology_version_id"])
            if row["ontology_version_id"]
            else None,
            source_boundaries=dict(row["source_boundaries"]),
            embedding_version=dict(row["embedding_version"]),
            retrieval_index_version=row["retrieval_index_version"],
            graph_projection_checkpoint=str(row["graph_projection_checkpoint"])
            if row["graph_projection_checkpoint"]
            else None,
        )

    async def fact_ids(self, *, group_id: str, snapshot_id: str) -> set[str]:
        async with self._session_factory() as session:
            rows = await session.scalars(_SNAPSHOT_FACT_IDS, {"sid": snapshot_id, "g": group_id})
        return set(rows)


class SqlAlchemyContextPackRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save(
        self,
        *,
        group_id: str,
        query: str,
        token_estimate: int,
        result_count: int,
        omitted: int,
        conflicts: int,
        freshness_warnings: int,
        results: list[JsonDict],
        snapshot_id: str | None = None,
        hints: JsonDict | None = None,
        actor: str | None = None,
    ) -> ContextPack:
        async with self._session_factory() as session, session.begin():
            row = (
                await session.execute(
                    _INSERT_PACK,
                    {
                        "g": group_id,
                        "sid": snapshot_id,
                        "q": query,
                        "hints": json.dumps(hints or {}),
                        "tokens": token_estimate,
                        "rc": result_count,
                        "om": omitted,
                        "cf": conflicts,
                        "fw": freshness_warnings,
                        "res": json.dumps(results),
                    },
                )
            ).one()
            await session.execute(
                _EVENT,
                {
                    "g": group_id,
                    "et": "CONTEXT_PACK_CREATED",
                    "actor": actor,
                    "ns": json.dumps({"pack_id": str(row.id), "snapshot_id": snapshot_id}),
                    "pv": None,
                },
            )
            return ContextPack(
                id=str(row.id),
                group_id=group_id,
                created_at=row.created_at,
                query=query,
                token_estimate=token_estimate,
                result_count=result_count,
                omitted=omitted,
                conflicts=conflicts,
                freshness_warnings=freshness_warnings,
                snapshot_id=snapshot_id,
                results=results,
            )

    async def get(self, *, group_id: str, pack_id: str) -> ContextPack | None:
        async with self._session_factory() as session:
            row = (
                (await session.execute(_GET_PACK, {"pid": pack_id, "g": group_id}))
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return ContextPack(
            id=str(row["id"]),
            group_id=row["group_id"],
            snapshot_id=str(row["snapshot_id"]) if row["snapshot_id"] else None,
            created_at=row["created_at"],
            query=row["query"],
            token_estimate=row["token_estimate"],
            result_count=row["result_count"],
            omitted=row["omitted"],
            conflicts=row["conflicts"],
            freshness_warnings=row["freshness_warnings"],
            results=list(row["results"]),
        )
