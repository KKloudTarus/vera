"""Bounded, hash-routed lane pool for ingestion.

Each group_id maps to one lane (``crc32(group_id) % lanes``), so all jobs for a
group run on the same lane, one at a time. Different groups spread across lanes and
run concurrently. Bounded lane queues provide backpressure: when a lane is full,
``submit`` blocks, which stalls the dispatcher and leaves work in Postgres. While
processing, a per-group advisory lock guards against a second replica touching the
same group, and the job is marked done in the same transaction that holds the lock.
"""

from __future__ import annotations

import asyncio
import random
import time
import zlib
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.repositories import (
    SqlAlchemyCanonicalEntityRepository,
    SqlAlchemyGraphMapRepository,
)
from vera.bootstrap import Container
from vera.domain.ports.job_queue import QueuedJob
from vera.domain.ports.memory_engine import EpisodeSpec, IngestReceipt
from vera.observability import bind_log_context, clear_log_context, get_logger, span
from vera.observability.cost import UsageContext, reset_usage_context, set_usage_context
from vera.observability.metrics import record_ingestion
from vera.shared.time import utc_now
from vera.shared.types import GroupId, JsonDict, SourceId

log = get_logger(__name__)

_MARK_DONE = text("UPDATE ingestion_jobs SET status = 'done', last_error = NULL WHERE id = :id")
_GROUP_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended(:g, 0))")
_EPISODE_BY_SOURCE = text(
    "SELECT id FROM published_episodes WHERE group_id = :group_id AND source_id = :source_id"
)


def lane_for(group_id: str, lanes: int) -> int:
    """Stable, process-independent lane assignment (crc32 is not hash-salted)."""
    return zlib.crc32(group_id.encode("utf-8")) % lanes


def _correlation(trace_context: JsonDict) -> dict[str, str]:
    cid = trace_context.get("correlation_id")
    return {"correlation_id": str(cid)} if cid else {}


class LanePool:
    def __init__(
        self,
        container: Container,
        *,
        lanes: int,
        queue_maxsize: int,
        backoff_base_s: float = 1.0,
        backoff_cap_s: float = 60.0,
    ) -> None:
        self._container = container
        self._lanes = lanes
        self._queues: list[asyncio.Queue[QueuedJob]] = [
            asyncio.Queue(maxsize=queue_maxsize) for _ in range(lanes)
        ]
        self._workers: list[asyncio.Task[None]] = []
        self._backoff_base_s = backoff_base_s
        self._backoff_cap_s = backoff_cap_s

    def start(self) -> None:
        self._workers = [
            asyncio.create_task(self._run_lane(i), name=f"lane-{i}") for i in range(self._lanes)
        ]

    async def submit(self, job: QueuedJob) -> None:
        await self._queues[lane_for(str(job.group_id), self._lanes)].put(job)

    async def join(self) -> None:
        """Wait until every queued job has been processed."""
        await asyncio.gather(*(q.join() for q in self._queues))

    async def stop(self) -> None:
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []

    def _backoff(self, attempts: int) -> float:
        ceiling = min(self._backoff_cap_s, self._backoff_base_s * (2**attempts))
        return random.uniform(0, ceiling)  # noqa: S311  jitter for retry spacing

    async def _run_lane(self, index: int) -> None:
        queue = self._queues[index]
        while True:
            job = await queue.get()
            try:
                await self._process(job)
            except Exception as exc:
                clear_log_context()
                record_ingestion(result="failed", duration_s=0.0)
                retry_in = self._backoff(job.attempts)
                await self._container.queue.fail(job.id, error=str(exc), retry_in_s=retry_in)
                log.warning(
                    "ingest.failed", job_id=str(job.id), lane=index, retry_in_s=round(retry_in, 2)
                )
            finally:
                queue.task_done()

    async def _process(self, job: QueuedJob) -> None:
        bind_log_context(
            group_id=str(job.group_id),
            source_id=str(job.source_id),
            **_correlation(job.trace_context),
        )
        # Attribute any provider tokens spent during this ingest to the episode.
        usage_token = set_usage_context(
            UsageContext(request_kind="ingest", group_id=str(job.group_id), ref=str(job.source_id))
        )
        started = time.perf_counter()
        episode_budget = self._container.settings.resilience.per_episode_timeout_s
        try:
            # A per-episode deadline bounds a hung provider call: on timeout the job
            # errors, the lane is freed, and the queue retries it (not left pinned).
            with span("ingest.job", group_id=str(job.group_id)):
                async with asyncio.timeout(episode_budget):
                    async with self._container.sessionmaker() as session, session.begin():
                        await session.execute(_GROUP_LOCK, {"g": str(job.group_id)})
                        episode = EpisodeSpec(
                            source_id=SourceId(str(job.source_id)),
                            group_id=GroupId(str(job.group_id)),
                            body=str(job.payload.get("body", "")),
                            reference_time=utc_now(),
                            metadata=job.payload,
                        )
                        receipt = await self._container.memory.ingest_episode(episode)
                        await self._stitch(session, str(job.group_id), str(job.source_id), receipt)
                        await session.execute(_MARK_DONE, {"id": job.id})
            record_ingestion(result="done", duration_s=time.perf_counter() - started)
            log.info("ingest.done", episode_uuid=receipt.episode_uuid)
        finally:
            reset_usage_context(usage_token)
            clear_log_context()

    async def _stitch(
        self, session: AsyncSession, group_id: str, source_id: str, receipt: IngestReceipt
    ) -> None:
        # Map each graph node to a canonical entity (resolve or create) and record the
        # node and edge uuids against the published episode this job came from. The
        # worker runs as a trusted role, so RLS is bypassed; the per-group advisory lock
        # already serializes canonical writes for the group.
        if not receipt.nodes and not receipt.edge_uuids:
            return
        episode_id = await session.scalar(
            _EPISODE_BY_SOURCE, {"group_id": group_id, "source_id": source_id}
        )
        canonical = SqlAlchemyCanonicalEntityRepository(session)
        graph_map = SqlAlchemyGraphMapRepository(session)
        for node in receipt.nodes:
            entity = await canonical.resolve(group_id=group_id, name=node.name)
            if entity is None:
                entity = await canonical.create(
                    group_id=group_id,
                    entity_type=node.entity_type,
                    canonical_name=node.name,
                    aliases=[],
                )
            await graph_map.record_node(
                group_id=group_id,
                node_uuid=UUID(node.uuid),
                canonical_entity_id=entity.id,
                published_episode_id=episode_id,
            )
        for edge_uuid in receipt.edge_uuids:
            await graph_map.record_edge(
                group_id=group_id, edge_uuid=UUID(edge_uuid), published_episode_id=episode_id
            )
