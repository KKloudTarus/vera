"""Retraction and right-to-erasure for a published source.

Withdraws one published episode from memory end to end: its edges leave the graph, its
graph maps are cleared, and the episode is marked retracted (hidden from search and
skipped by a rebuild). Erasure goes further, deleting the episode row and its raw artifact
bytes from the object store for data-subject requests. Every retraction writes an audit
event. Postgres is the source of truth, so it commits first; the graph and object store
(both rebuildable or already-recorded) are updated after.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.domain.ports.memory_engine import MemoryEngine
from vera.domain.ports.object_store import ObjectStore
from vera.observability import get_logger
from vera.shared.errors import DomainError, Err, NotFound, Ok, Result
from vera.shared.time import utc_now

log = get_logger(__name__)

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

        # Postgres committed; update the projection and (for erasure) the object store.
        await self._memory.retract_episode(group_id=group_id, edge_uuids=edge_uuids)
        if erase_artifact and self._object_store is not None:
            for key in s3_keys:
                await self._object_store.delete(key=key)
        log.info(
            "retraction.done",
            group_id=group_id,
            source_id=source_id,
            edges=len(edge_uuids),
            erased=erase_artifact,
        )
        return Ok(
            RetractionResult(
                source_id=source_id, edges_removed=len(edge_uuids), erased=erase_artifact
            )
        )
