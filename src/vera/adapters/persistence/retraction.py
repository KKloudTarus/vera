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
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.domain.ports.memory_engine import MemoryEngine
from vera.domain.ports.object_store import ObjectStore
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
    ) -> None:
        self._session_factory = session_factory
        self._memory = memory
        self._object_store = object_store

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
            episode_id = await session.scalar(_FIND, {"g": group_id, "s": source_id})
            if episode_id is None:
                return Err(NotFound(f"published source {source_id} not found in {group_id}"))
            edge_uuids = [
                str(r)
                for (r,) in (await session.execute(_EDGES, {"g": group_id, "eid": episode_id}))
            ]
            s3_keys: list[str] = []
            claim_id = _claim_id(source_id)
            if erase_artifact and claim_id is not None:
                s3_keys = [
                    str(r)
                    for (r,) in (await session.execute(_S3_KEYS, {"g": group_id, "cid": claim_id}))
                ]
            await session.execute(_DELETE_EDGES, {"g": group_id, "eid": episode_id})
            await session.execute(_DELETE_NODES, {"g": group_id, "eid": episode_id})
            if erase_artifact:
                await session.execute(_DELETE_EPISODE, {"eid": episode_id})
            else:
                await session.execute(_MARK, {"now": now, "eid": episode_id})
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

        # Postgres committed. Clean the projection and (for erasure) the object store now; on
        # any failure the queued job guarantees the same cleanup runs later, so the operation
        # completes durably either way.
        cleaned = True
        try:
            await self._memory.retract_episode(group_id=group_id, edge_uuids=edge_uuids)
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
                source_id=source_id, edges_removed=len(edge_uuids), erased=erase_artifact
            )
        )
