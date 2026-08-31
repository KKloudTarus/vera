"""Retraction and right-to-erasure for a published source.

Withdraws one published episode from memory end to end: its edges leave the graph, its
graph maps are cleared, and the episode is marked retracted (hidden from search and
skipped by a rebuild). Erasure goes further, deleting the episode row and its raw artifact
bytes from the object store for data-subject requests. Every retraction writes an audit
event.

Postgres is the source of truth, so it commits first. The graph and object-store cleanup
that follows is a separate step, so a crash between the commit and that cleanup would
otherwise leave the graph or (worse, for erasure) the raw bytes behind. To make the whole
operation durable, the same committing transaction also enqueues a ``retract_cleanup`` job
carrying the edge uuids and S3 keys, scheduled a short while ahead. The happy path still
cleans up in-process immediately and then retires that job; if the process dies first, the
worker runs the identical, idempotent cleanup once the job becomes visible. Erasure is
therefore guaranteed to complete, not merely attempted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories import SqlAlchemyKnowledgeEventLog
from vera.domain.knowledge.fabric import KnowledgeEvent, KnowledgeEventType
from vera.domain.ports.memory_engine import MemoryEngine
from vera.domain.ports.object_store import ObjectStore
from vera.domain.ports.projection import FactProjection
from vera.observability import get_logger
from vera.shared.errors import DomainError, Err, NotFound, Ok, Result
from vera.shared.ids import uuid7
from vera.shared.time import utc_now

log = get_logger(__name__)

# How far ahead the durable cleanup job is scheduled. The in-process cleanup normally runs
# and retires the job well within this window, so the worker only ever sees it after a crash.
_CLEANUP_SAFETY_DELAY_S = 30.0

_FIND = text("SELECT id FROM published_episodes WHERE group_id = :g AND source_id = :s")
_EDGES = text(
    "SELECT edge_uuid FROM graph_edge_map WHERE group_id = :g AND published_episode_id = :eid"
)
_S3_KEYS = text(
    "SELECT av.s3_key FROM candidate_claims cc "
    "JOIN artifact_versions av ON av.id = cc.artifact_version_id "
    "WHERE cc.group_id = :g AND cc.id = :cid"
)
_ARTIFACT_VERSION_IDS = text(
    "SELECT DISTINCT artifact_version_id FROM candidate_claims WHERE group_id = :g AND id = :cid"
)
_AFFECTED_ASSERTIONS_BY_RUN = text(
    "SELECT a.id, a.fact_id, a.artifact_id, f.fact_key FROM assertions a "
    "JOIN facts f ON f.id=a.fact_id AND f.group_id=a.group_id "
    # Both the live-ingest (episode:) and the migration backfill (backfill:) run keys, so a
    # backfilled group's assertions are withdrawn too. The projection matches both keys
    # (repositories/projection.py), so retraction must as well or the fact stays projected.
    "WHERE a.group_id=:g AND a.run_key IN (:run_key_episode, :run_key_backfill) "
    "AND a.state='active'"
)
_AFFECTED_ASSERTIONS_BY_VERSION = text(
    "SELECT a.id, a.fact_id, a.artifact_id, f.fact_key FROM assertions a "
    "JOIN facts f ON f.id=a.fact_id AND f.group_id=a.group_id "
    "WHERE a.group_id=:g AND a.artifact_version_id = ANY(CAST(:version_ids AS uuid[])) "
    "AND a.state='active'"
)
_WITHDRAW_ASSERTIONS = text(
    "UPDATE assertions SET state='withdrawn', withdrawn_at=:now, "
    "valid_to=COALESCE(valid_to, :now) WHERE group_id=:g "
    "AND id = ANY(CAST(:assertion_ids AS uuid[])) AND state='active'"
)
_RECOMPUTE_SUPPORTED_FACTS = text(
    "WITH support AS (SELECT fact_id, max(source_authority) AS authority, "
    "max(extractor_confidence) AS confidence FROM assertions WHERE group_id=:g "
    "AND fact_id = ANY(CAST(:fact_ids AS uuid[])) AND state='active' "
    "AND polarity='supports' GROUP BY fact_id) UPDATE facts f SET authority=s.authority, "
    "confidence=s.confidence, updated_at=:now FROM support s "
    "WHERE f.group_id=:g AND f.id=s.fact_id"
)
_RETRACT_UNSUPPORTED_FACTS = text(
    "UPDATE facts f SET lifecycle_state='retracted', valid_to=COALESCE(f.valid_to, :now), "
    "updated_at=:now WHERE f.group_id=:g AND f.id = ANY(CAST(:fact_ids AS uuid[])) "
    "AND NOT EXISTS (SELECT 1 FROM assertions a WHERE a.group_id=f.group_id "
    "AND a.fact_id=f.id AND a.state='active' AND a.polarity='supports') "
    "RETURNING f.id, f.fact_key"
)
_ERASE_RETRIEVAL_INPUTS = text(
    "SELECT erase_artifact_retrieval_inputs(:g, CAST(:version_ids AS uuid[]))"
)
_DELETE_EDGES = text(
    "DELETE FROM graph_edge_map WHERE group_id = :g AND published_episode_id = :eid"
)
_DELETE_NODES = text(
    "DELETE FROM graph_node_map WHERE group_id = :g AND published_episode_id = :eid"
)
_MARK = text(
    "UPDATE published_episodes SET retracted_at = :now, invalid_at = COALESCE(invalid_at, :now) "
    "WHERE id = :eid"
)
_DELETE_EPISODE = text("DELETE FROM published_episodes WHERE id = :eid")
_AUDIT = text(
    "INSERT INTO audit_events (actor, group_id, action, target, payload) "
    "VALUES (:actor, :g, :action, :target, '{}'::jsonb)"
)
_ENQUEUE_CLEANUP = text(
    "INSERT INTO ingestion_jobs (group_id, source_id, dedup_uuid, payload, next_visible_at) "
    "VALUES (:g, :s, :dedup, CAST(:payload AS jsonb), now() + (:delay * interval '1 second')) "
    "RETURNING id"
)
_MARK_JOB_DONE = text("UPDATE ingestion_jobs SET status = 'done', last_error = NULL WHERE id = :id")
_GROUP_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended(:g, 0))")
_CANCEL_SOURCE_JOBS = text(
    "UPDATE ingestion_jobs SET status='done', last_error='cancelled: source retracted', "
    "locked_until=NULL WHERE group_id=:g AND source_id=:s AND status IN ('pending','inflight') "
    "AND COALESCE(payload->>'job_kind', '') NOT IN ('project_facts', 'retract_cleanup')"
)
_ENQUEUE_PROJECTION = text(
    "INSERT INTO ingestion_jobs (group_id, source_id, dedup_uuid, payload) "
    "VALUES (:g, :s, :dedup, CAST(:payload AS jsonb))"
)


@dataclass(frozen=True, slots=True)
class RetractionResult:
    source_id: str
    edges_removed: int
    erased: bool


def _claim_id(source_id: str) -> UUID | None:
    # A published episode's source_id is "<group_id>:<claim_uuid>"; the claim id is the
    # segment after the last colon (group_ids also contain colons).
    try:
        return UUID(source_id.rsplit(":", 1)[-1])
    except (ValueError, IndexError):
        return None


class RetractionService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        memory: MemoryEngine,
        object_store: ObjectStore | None = None,
        fact_projection: FactProjection | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._memory = memory
        self._object_store = object_store
        self._fact_projection = fact_projection

    async def retract_source(
        self,
        *,
        group_id: str,
        source_id: str,
        actor_principal_id: UUID | None = None,
        erase_artifact: bool = False,
    ) -> Result[RetractionResult, DomainError]:
        now = utc_now()
        # Trusted read/write with an explicit group filter (same pattern as the read model),
        # so retraction works across the control-plane tables without an RLS tenant switch.
        async with self._session_factory() as session, session.begin():
            await session.execute(_GROUP_LOCK, {"g": group_id})
            await session.execute(
                text("SELECT set_config('vera.group_id', :group_id, true)"),
                {"group_id": group_id},
            )
            episode_id = await session.scalar(_FIND, {"g": group_id, "s": source_id})
            edge_uuids = (
                [
                    str(r)
                    for (r,) in (await session.execute(_EDGES, {"g": group_id, "eid": episode_id}))
                ]
                if episode_id is not None
                else []
            )
            s3_keys: list[str] = []
            artifact_version_ids: list[UUID] = []
            claim_id = _claim_id(source_id)
            if claim_id is not None:
                artifact_version_ids = [
                    r
                    for (r,) in (
                        await session.execute(
                            _ARTIFACT_VERSION_IDS, {"g": group_id, "cid": claim_id}
                        )
                    )
                ]
            if episode_id is None and not artifact_version_ids:
                return Err(NotFound(f"published source {source_id} not found in {group_id}"))
            if erase_artifact and claim_id is not None:
                s3_keys = [
                    str(r)
                    for (r,) in (await session.execute(_S3_KEYS, {"g": group_id, "cid": claim_id}))
                ]
            # Always match by run key (episode: and backfill:); on erase also match by artifact
            # version, then union, so both live and backfilled assertions are caught even when a
            # backfilled legacy assertion has a NULL artifact_version_id.
            affected_by_id: dict[UUID, dict[str, Any]] = {}
            for row in (
                await session.execute(
                    _AFFECTED_ASSERTIONS_BY_RUN,
                    {
                        "g": group_id,
                        "run_key_episode": f"episode:{source_id}",
                        "run_key_backfill": f"backfill:{source_id}",
                    },
                )
            ).mappings():
                affected_by_id[row["id"]] = dict(row)
            if erase_artifact and artifact_version_ids:
                for row in (
                    await session.execute(
                        _AFFECTED_ASSERTIONS_BY_VERSION,
                        {"g": group_id, "version_ids": artifact_version_ids},
                    )
                ).mappings():
                    affected_by_id[row["id"]] = dict(row)
            affected_assertions = list(affected_by_id.values())
            assertion_ids = [row["id"] for row in affected_assertions]
            affected_fact_ids = list({row["fact_id"] for row in affected_assertions})
            retracted_fact_keys: list[str] = []
            events = SqlAlchemyKnowledgeEventLog(session)
            if assertion_ids:
                await session.execute(
                    _WITHDRAW_ASSERTIONS,
                    {"g": group_id, "assertion_ids": assertion_ids, "now": now},
                )
                for row in affected_assertions:
                    await events.append(
                        KnowledgeEvent(
                            id=uuid7(),
                            group_id=group_id,
                            event_type=KnowledgeEventType.ASSERTION_WITHDRAWN,
                            occurred_at=now,
                            actor=str(actor_principal_id) if actor_principal_id else None,
                            source_id=source_id,
                            fact_id=row["fact_id"],
                            assertion_id=row["id"],
                            artifact_id=row["artifact_id"],
                            previous_state={"state": "active"},
                            next_state={"state": "withdrawn"},
                            reason="source retracted",
                        )
                    )
            if affected_fact_ids:
                await session.execute(
                    _RECOMPUTE_SUPPORTED_FACTS,
                    {"g": group_id, "fact_ids": affected_fact_ids, "now": now},
                )
                retracted_facts = list(
                    (
                        await session.execute(
                            _RETRACT_UNSUPPORTED_FACTS,
                            {"g": group_id, "fact_ids": affected_fact_ids, "now": now},
                        )
                    ).mappings()
                )
                retracted_fact_keys = [str(row["fact_key"]) for row in retracted_facts]
                for row in retracted_facts:
                    await events.append(
                        KnowledgeEvent(
                            id=uuid7(),
                            group_id=group_id,
                            event_type=KnowledgeEventType.FACT_RETRACTED,
                            occurred_at=now,
                            actor=str(actor_principal_id) if actor_principal_id else None,
                            source_id=source_id,
                            fact_id=row["id"],
                            previous_state={"lifecycle_state": "active"},
                            next_state={"lifecycle_state": "retracted"},
                            reason="final supporting assertion withdrawn",
                        )
                    )
            if erase_artifact and artifact_version_ids:
                await session.execute(
                    _ERASE_RETRIEVAL_INPUTS,
                    {"g": group_id, "version_ids": artifact_version_ids},
                )
            if episode_id is not None:
                await session.execute(_DELETE_EDGES, {"g": group_id, "eid": episode_id})
                await session.execute(_DELETE_NODES, {"g": group_id, "eid": episode_id})
                if erase_artifact:
                    await session.execute(_DELETE_EPISODE, {"eid": episode_id})
                else:
                    await session.execute(_MARK, {"now": now, "eid": episode_id})
            await session.execute(_CANCEL_SOURCE_JOBS, {"g": group_id, "s": source_id})
            await session.execute(
                _AUDIT,
                {
                    "actor": str(actor_principal_id) if actor_principal_id else None,
                    "g": group_id,
                    "action": "erase" if erase_artifact else "retract",
                    "target": source_id,
                },
            )
            # Durable safety net: enqueue the graph/object-store cleanup atomically with the
            # commit, so a crash before the in-process cleanup below still gets it done.
            cleanup_job_id: UUID | None = None
            if edge_uuids or (erase_artifact and s3_keys):
                cleanup_job_id = await session.scalar(
                    _ENQUEUE_CLEANUP,
                    {
                        "g": group_id,
                        "s": source_id,
                        "dedup": uuid7(),
                        "payload": json.dumps(
                            {
                                "job_kind": "retract_cleanup",
                                "edge_uuids": edge_uuids,
                                "s3_keys": s3_keys,
                                "erase": erase_artifact,
                            }
                        ),
                        "delay": _CLEANUP_SAFETY_DELAY_S,
                    },
                )
            if affected_assertions:
                await session.execute(
                    _ENQUEUE_PROJECTION,
                    {
                        "g": group_id,
                        "s": source_id,
                        "dedup": uuid7(),
                        "payload": json.dumps({"job_kind": "project_facts", "group_id": group_id}),
                    },
                )

        # Postgres committed. Clean the projection and (for erasure) the object store now; on
        # any failure the queued job guarantees the same cleanup runs later, so the operation
        # completes durably either way.
        cleaned = True
        try:
            await self._memory.retract_episode(group_id=group_id, edge_uuids=edge_uuids)
            if self._fact_projection is not None:
                for fact_key in retracted_fact_keys:
                    await self._fact_projection.remove(group_id=group_id, fact_key=fact_key)
            if erase_artifact and self._object_store is not None:
                for key in s3_keys:
                    await self._object_store.delete(key=key)
        except Exception as exc:
            cleaned = False
            log.warning(
                "retraction.cleanup_deferred",
                group_id=group_id,
                source_id=source_id,
                job_id=str(cleanup_job_id) if cleanup_job_id else None,
                error=str(exc),
            )

        if cleaned and cleanup_job_id is not None:
            async with self._session_factory() as session, session.begin():
                await session.execute(_MARK_JOB_DONE, {"id": cleanup_job_id})

        log.info(
            "retraction.done",
            group_id=group_id,
            source_id=source_id,
            edges=len(edge_uuids),
            erased=erase_artifact,
            deferred=not cleaned,
        )
        return Ok(
            RetractionResult(
                source_id=source_id,
                edges_removed=len(edge_uuids) + len(retracted_fact_keys),
                erased=erase_artifact,
            )
        )
