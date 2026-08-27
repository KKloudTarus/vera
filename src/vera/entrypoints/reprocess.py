"""Reprocess command: rebuild a group's graph from Postgres (the source of truth).

Neo4j is a projection, so after an ontology change (or a lost graph) a group is rebuilt
by replaying its published episodes in valid-time order through the normal ingestion
path. Node and edge uuids are not stable across a rebuild, so the graph maps are cleared
and re-derived; canonical entities resolve by name and are kept. The result is an
equivalent graph: the same facts, retrievable the same way.
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from typing import cast

from sqlalchemy import text

from vera.bootstrap import Container, build_container, dispose_container
from vera.config.settings import get_settings
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.observability import configure_logging, get_logger
from vera.shared.types import GroupId, JsonDict, SourceId

log = get_logger(__name__)

_VERIFY = text(
    "SELECT "
    "(SELECT count(*) FROM published_episodes WHERE group_id = :g) AS episodes, "
    "(SELECT count(*) FROM graph_node_map WHERE group_id = :g) AS nodes, "
    "(SELECT count(*) FROM graph_edge_map WHERE group_id = :g) AS edges"
)


@dataclass(frozen=True, slots=True)
class RebuildReport:
    episodes: int
    nodes: int
    edges: int

    @property
    def ok(self) -> bool:
        # A group that had facts must come back with a repopulated graph projection.
        return self.episodes == 0 or self.edges > 0


async def verify_group(container: Container, group_id: str) -> RebuildReport:
    """Check a rebuilt group's graph projection was repopulated from Postgres."""
    async with container.sessionmaker() as session:
        row = (await session.execute(_VERIFY, {"g": group_id})).one()
    return RebuildReport(episodes=int(row.episodes), nodes=int(row.nodes), edges=int(row.edges))


_EPISODES = text(
    "SELECT source_id, payload, dedup_uuid FROM published_episodes "
    "WHERE group_id = :g ORDER BY reference_time ASC"
)
_CLEAR = (
    text("DELETE FROM graph_edge_map WHERE group_id = :g"),
    text("DELETE FROM graph_node_map WHERE group_id = :g"),
    text("DELETE FROM ingestion_jobs WHERE group_id = :g"),
    # Drop the embedding fingerprint so the replay re-initializes it at the current model
    # and dimension: this is how a model change is applied (re-embed the whole group).
    text("DELETE FROM group_embedding_state WHERE group_id = :g"),
)


async def rebuild_group(container: Container, group_id: str) -> int:
    """Wipe and replay one group's graph. Returns the number of episodes replayed."""
    await container.memory.clear_group(group_id)
    async with container.sessionmaker() as session, session.begin():
        for statement in _CLEAR:
            await session.execute(statement, {"g": group_id})

    async with container.sessionmaker() as session:
        rows = (await session.execute(_EPISODES, {"g": group_id})).all()

    for source_id, payload, dedup_uuid in rows:
        await container.queue.enqueue(
            group_id=GroupId(group_id),
            source_id=SourceId(str(source_id)),
            dedup_uuid=dedup_uuid,
            payload=cast("JsonDict", payload) if payload else {},
        )

    pool = LanePool(container, lanes=2, queue_maxsize=16)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=50)
    finally:
        await pool.stop()
    log.info("reprocess.done", group_id=group_id, episodes=len(rows))
    return len(rows)


async def _run(group_id: str) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    try:
        await rebuild_group(container, group_id)
        report = await verify_group(container, group_id)
        if report.ok:
            log.info(
                "reprocess.verified",
                group_id=group_id,
                episodes=report.episodes,
                nodes=report.nodes,
                edges=report.edges,
            )
        else:
            log.error(
                "reprocess.verify_failed",
                group_id=group_id,
                episodes=report.episodes,
                nodes=report.nodes,
                edges=report.edges,
            )
            raise SystemExit(f"reprocess verification failed for {group_id}")
    finally:
        await dispose_container(container)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m vera.entrypoints.reprocess <group_id>")
    asyncio.run(_run(sys.argv[1]))


if __name__ == "__main__":
    main()
