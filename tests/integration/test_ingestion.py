"""Ingestion plane: queue lifecycle, idempotency, reaper, DLQ, and the lane pool's
per-group serialization with cross-group parallelism. Runs against the live database.
"""

from __future__ import annotations

import asyncio
import dataclasses
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.knowledge import PublishedEpisodeRow
from vera.adapters.persistence.repositories.usage import SqlAlchemyUsageSink
from vera.adapters.queue import postgres_queue as postgres_queue_module
from vera.adapters.queue.postgres_queue import PostgresJobQueue
from vera.bootstrap import Container
from vera.domain.ports.memory_engine import EpisodeSpec, GraphHit, IngestReceipt
from vera.entrypoints.reprocess import rebuild_group
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.observability.cost import (
    ProviderBudgetContext,
    reset_provider_budget_context,
    set_provider_budget_context,
)
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.types import GroupId, SourceId

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class RecordingMemoryEngine:
    """A memory engine that records processing intervals to prove serialization."""

    def __init__(self, delay_s: float = 0.05) -> None:
        self._delay_s = delay_s
        self.intervals: list[tuple[str, str, float, float]] = []
        self._active: dict[str, int] = {}
        self.same_group_overlap = False

    async def ingest_episode(self, episode: EpisodeSpec) -> IngestReceipt:
        group = str(episode.group_id)
        self._active[group] = self._active.get(group, 0) + 1
        if self._active[group] > 1:
            self.same_group_overlap = True
        start = time.monotonic()
        await asyncio.sleep(self._delay_s)
        end = time.monotonic()
        self._active[group] -= 1
        self.intervals.append((group, str(episode.source_id), start, end))
        return IngestReceipt(episode_uuid=deterministic_id(str(episode.source_id)).hex)

    async def search(self, query: object) -> Sequence[GraphHit]:
        return []

    async def health(self) -> bool:
        return True


class ReferenceRecordingMemoryEngine(RecordingMemoryEngine):
    def __init__(self, *, failures: int = 0) -> None:
        super().__init__(delay_s=0)
        self.failures = failures
        self.episodes: list[EpisodeSpec] = []
        self.cleared_groups: list[str] = []

    async def ingest_episode(self, episode: EpisodeSpec) -> IngestReceipt:
        self.episodes.append(episode)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("graph provider unavailable")
        return IngestReceipt(episode_uuid=deterministic_id(str(episode.source_id)).hex)

    async def clear_group(self, group_id: str) -> None:
        self.cleared_groups.append(group_id)


async def _enqueue(container: Container, *, group: str, source: str) -> None:
    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={"body": f"content for {source}"},
    )


def _legacy_container(
    make_container: Callable[[object], Container], memory: ReferenceRecordingMemoryEngine
) -> Container:
    container = make_container(memory)
    settings = container.settings.model_copy(
        update={
            "memory": container.settings.memory.model_copy(
                update={"fabric_enabled": False, "fabric_write_mode": "legacy"}
            )
        }
    )
    return dataclasses.replace(container, settings=settings)


async def _seed_published_episode(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    group: str,
    source: str,
    reference_time: datetime,
) -> None:
    async with sessionmaker() as session, session.begin():
        session.add(
            PublishedEpisodeRow(
                source_id=source,
                group_id=group,
                knowledge_type="text",
                verification="human_verified",
                authority=1.0,
                confidence=1.0,
                reference_time=reference_time,
                payload={"body": f"content for {source}"},
                dedup_uuid=deterministic_id(source),
            )
        )


async def test_enqueue_is_idempotent(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    sfx = uuid7().hex[:12]
    source = f"git:{sfx}"
    first = await container.queue.enqueue(
        group_id=GroupId(f"p:{sfx}"),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={},
    )
    second = await container.queue.enqueue(
        group_id=GroupId(f"p:{sfx}"),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={},
    )
    assert first is True
    assert second is False
    async with sessionmaker() as s:
        count = await s.scalar(
            text("SELECT count(*) FROM ingestion_jobs WHERE source_id = :src"), {"src": source}
        )
    assert count == 1


async def test_claim_carries_trace_context(
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    sfx = uuid7().hex[:12]
    source = f"conf:{sfx}"
    await container.queue.enqueue(
        group_id=GroupId(f"p:{sfx}"),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={"body": "x"},
        trace_context={"correlation_id": "corr-123"},
    )
    jobs = await container.queue.claim(batch_size=10)
    mine = [j for j in jobs if str(j.source_id) == source]
    assert len(mine) == 1
    assert mine[0].trace_context == {"correlation_id": "corr-123"}
    assert mine[0].attempts == 1


async def test_enqueue_propagates_provider_budget_to_worker_trace(
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    suffix = uuid7().hex[:12]
    token = set_provider_budget_context(ProviderBudgetContext(f"budget:{suffix}"))
    try:
        await _enqueue(
            container,
            group=f"p:budget:{suffix}",
            source=f"budget:{suffix}",
        )
    finally:
        reset_provider_budget_context(token)

    job = (await container.queue.claim(batch_size=1))[0]
    assert job.trace_context["_provider_budget_key"] == f"budget:{suffix}"


async def test_claim_returns_only_oldest_pending_job_per_group(
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    suffix = uuid7().hex[:12]
    group = f"p:ordered:{suffix}"
    sources = [f"ordered:{suffix}:{index}" for index in range(3)]
    for source in sources:
        await _enqueue(container, group=group, source=source)
    other_source = f"other:{suffix}"
    await _enqueue(container, group=f"p:other:{suffix}", source=other_source)

    claimed = await container.queue.claim(batch_size=10)

    assert {str(job.source_id) for job in claimed} == {sources[0], other_source}
    first = next(job for job in claimed if str(job.source_id) == sources[0])
    await container.queue.complete(first.id, claim_token=first.claim_token)
    next_claim = await container.queue.claim(batch_size=10)
    assert [str(job.source_id) for job in next_claim] == [sources[1]]


async def test_concurrent_claimer_cannot_skip_locked_group_head(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    suffix = uuid7().hex[:12]
    group = f"p:concurrent:{suffix}"
    sources = [f"concurrent:{suffix}:{index}" for index in range(2)]
    for source in sources:
        await _enqueue(container, group=group, source=source)

    async with sessionmaker() as first_session, first_session.begin():
        first_rows = (
            (
                await first_session.execute(
                    postgres_queue_module._CLAIM,  # pyright: ignore[reportPrivateUsage]
                    {"batch": 1, "visibility": 300},
                )
            )
            .mappings()
            .all()
        )
        second_rows = await container.queue.claim(batch_size=10)
        assert [str(row["source_id"]) for row in first_rows] == [sources[0]]
        assert second_rows == []


async def test_legacy_worker_claim_and_release_survive_expansion_migration(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    suffix = uuid7().hex[:12]
    source = f"legacy-rollout:{suffix}"
    await _enqueue(container, group=f"p:{suffix}", source=source)
    legacy_claim = text(
        """
        WITH ready AS (
            SELECT id FROM ingestion_jobs
            WHERE status='pending' AND next_visible_at <= now()
            ORDER BY next_visible_at, created_at
            FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE ingestion_jobs job
        SET status='inflight', attempts=job.attempts + 1,
            locked_until=now() + interval '5 minutes'
        FROM ready WHERE job.id=ready.id
        RETURNING job.id, job.claim_token
        """
    )
    async with sessionmaker() as session, session.begin():
        claimed = (await session.execute(legacy_claim)).one()
        assert claimed.claim_token is None
        await session.execute(
            text(
                "UPDATE ingestion_jobs SET status='pending', locked_until=NULL, "
                "next_visible_at=now() WHERE id=:id"
            ),
            {"id": claimed.id},
        )

    current = (await container.queue.claim(batch_size=1))[0]
    assert current.id == claimed.id
    assert current.claim_token is not None


async def test_complete_records_completion_time(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    suffix = uuid7().hex[:12]
    source = f"complete:{suffix}"
    await _enqueue(container, group=f"p:{suffix}", source=source)
    jobs = await container.queue.claim(batch_size=10)
    job = next(value for value in jobs if str(value.source_id) == source)

    await container.queue.complete(job.id, claim_token=job.claim_token)

    async with sessionmaker() as session:
        row = (
            await session.execute(
                text("SELECT status, completed_at FROM ingestion_jobs WHERE id = :id"),
                {"id": job.id},
            )
        ).one()
    assert row.status == "done"
    assert row.completed_at is not None


async def test_fail_reschedules_then_dead_letters(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    sfx = uuid7().hex[:12]
    source = f"jira:{sfx}"
    await _enqueue(container, group=f"p:{sfx}", source=source)
    jobs = await container.queue.claim(batch_size=10)
    job = next(j for j in jobs if str(j.source_id) == source)

    await container.queue.fail(
        job.id, claim_token=job.claim_token, error="transient", retry_in_s=30
    )
    async with sessionmaker() as s:
        status = await s.scalar(
            text("SELECT status FROM ingestion_jobs WHERE id = :id"), {"id": job.id}
        )
    assert status == "pending"

    # Exhaust attempts, then a failure dead-letters the job.
    async with sessionmaker() as s, s.begin():
        await s.execute(
            text(
                "UPDATE ingestion_jobs SET attempts = max_attempts - 1, "
                "next_visible_at = now() WHERE id = :id"
            ),
            {"id": job.id},
        )
    retried = next(
        value for value in await container.queue.claim(batch_size=10) if value.id == job.id
    )
    await container.queue.fail(
        retried.id,
        claim_token=retried.claim_token,
        error="permanent",
        retry_in_s=1,
    )
    async with sessionmaker() as s:
        status = await s.scalar(
            text("SELECT status FROM ingestion_jobs WHERE id = :id"), {"id": job.id}
        )
    assert status == "dead"


async def test_provider_fence_makes_failure_and_reclaim_terminal(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    suffix = uuid7().hex[:12]
    await _enqueue(container, group=f"p:fence:{suffix}", source=f"fence:{suffix}:1")
    first = (await container.queue.claim(batch_size=1))[0]
    await container.queue.fence_provider_attempt(first.id, claim_token=first.claim_token)
    await container.queue.fail(
        first.id,
        claim_token=first.claim_token,
        error="ambiguous provider result",
        retry_in_s=1,
    )

    await _enqueue(container, group=f"p:fence:{suffix}", source=f"fence:{suffix}:2")
    second = (await container.queue.claim(batch_size=1))[0]
    await container.queue.fence_provider_attempt(second.id, claim_token=second.claim_token)
    async with sessionmaker() as session, session.begin():
        await session.execute(
            text("UPDATE ingestion_jobs SET locked_until=now() - interval '1 second' WHERE id=:id"),
            {"id": second.id},
        )
    assert await container.queue.reclaim_stuck() >= 1

    async with sessionmaker() as session:
        rows = (
            await session.execute(
                text("SELECT id, status FROM ingestion_jobs WHERE id IN (:first, :second)"),
                {"first": first.id, "second": second.id},
            )
        ).all()
    assert {row.id: row.status for row in rows} == {first.id: "dead", second.id: "dead"}


async def test_provider_fence_is_bound_to_one_claim_generation(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    suffix = uuid7().hex[:12]
    await _enqueue(container, group=f"p:lease:{suffix}", source=f"lease:{suffix}")
    first = (await container.queue.claim(batch_size=1))[0]
    async with sessionmaker() as session, session.begin():
        await session.execute(
            text("UPDATE ingestion_jobs SET locked_until=now() - interval '1 second' WHERE id=:id"),
            {"id": first.id},
        )
    assert await container.queue.reclaim_stuck() >= 1
    second = (await container.queue.claim(batch_size=1))[0]
    assert second.id == first.id
    assert second.claim_token != first.claim_token

    sink = SqlAlchemyUsageSink(sessionmaker)
    with pytest.raises(RuntimeError, match="no inflight ingestion job"):
        await sink.fence_provider_attempt(str(first.id), str(first.claim_token))
    await sink.fence_provider_attempt(str(second.id), str(second.claim_token))


async def test_reclaim_stuck_returns_inflight_to_pending(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    sfx = uuid7().hex[:12]
    source = f"cmdb:{sfx}"
    await _enqueue(container, group=f"p:{sfx}", source=source)
    jobs = await container.queue.claim(batch_size=10)
    job = next(j for j in jobs if str(j.source_id) == source)

    # Simulate a crashed worker: the lock has timed out.
    async with sessionmaker() as s, s.begin():
        await s.execute(
            text(
                "UPDATE ingestion_jobs SET locked_until = now() - interval '1 minute' "
                "WHERE id = :id"
            ),
            {"id": job.id},
        )
    reclaimed = await container.queue.reclaim_stuck()
    assert reclaimed >= 1
    async with sessionmaker() as s:
        status = await s.scalar(
            text("SELECT status FROM ingestion_jobs WHERE id = :id"), {"id": job.id}
        )
    assert status == "pending"


async def test_active_job_renews_lease_before_reclamation(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    memory = RecordingMemoryEngine(delay_s=1.4)
    container = _legacy_container(make_container, memory)
    container = dataclasses.replace(
        container,
        queue=PostgresJobQueue(sessionmaker, visibility_timeout_s=1),
    )
    suffix = uuid7().hex[:12]
    group = f"p:heartbeat:{suffix}"
    source = f"heartbeat:{suffix}"
    await _enqueue(container, group=group, source=source)
    job = (await container.queue.claim(batch_size=1))[0]
    pool = LanePool(
        container,
        lanes=1,
        queue_maxsize=1,
        lease_renew_interval_s=0.1,
    )
    pool.start()
    try:
        await pool.submit(job)
        await asyncio.sleep(1.1)
        assert await container.queue.reclaim_stuck() == 0
        await asyncio.wait_for(pool.join(), timeout=3)
    finally:
        await pool.stop()

    async with sessionmaker() as session:
        status = await session.scalar(
            text("SELECT status FROM ingestion_jobs WHERE id=:id"), {"id": job.id}
        )
    assert status == "done"


async def test_release_returns_claim_without_consuming_attempt(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(RecordingMemoryEngine())
    suffix = uuid7().hex[:12]
    source = f"release:{suffix}"
    await _enqueue(container, group=f"p:{suffix}", source=source)
    jobs = await container.queue.claim(batch_size=10)
    job = next(value for value in jobs if str(value.source_id) == source)

    await container.queue.release(job.id, claim_token=job.claim_token, reason="worker shutdown")

    async with sessionmaker() as session:
        row = (
            await session.execute(
                text(
                    "SELECT status, attempts, locked_until, last_error "
                    "FROM ingestion_jobs WHERE id=:id"
                ),
                {"id": job.id},
            )
        ).one()
    assert row.status == "pending"
    assert row.attempts == 0
    assert row.locked_until is None
    assert row.last_error == "worker shutdown"


async def test_published_reference_time_is_stable_across_retry(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    memory = ReferenceRecordingMemoryEngine(failures=1)
    container = _legacy_container(make_container, memory)
    suffix = uuid7().hex[:12]
    group = f"p:{suffix}"
    source = f"cmdb:{suffix}"
    reference_time = datetime(2024, 5, 6, 7, 8, tzinfo=UTC)
    await _seed_published_episode(
        sessionmaker, group=group, source=source, reference_time=reference_time
    )
    await _enqueue(container, group=group, source=source)

    pool = LanePool(
        container,
        lanes=1,
        queue_maxsize=2,
        backoff_base_s=0,
        backoff_cap_s=0,
    )
    pool.start()
    try:
        for _ in range(2):
            jobs = await container.queue.claim(batch_size=1)
            assert len(jobs) == 1
            await pool.submit(jobs[0])
            await pool.join()
    finally:
        await pool.stop()

    assert [episode.reference_time for episode in memory.episodes] == [
        reference_time,
        reference_time,
    ]


async def test_rebuild_preserves_published_reference_time(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    memory = ReferenceRecordingMemoryEngine()
    container = _legacy_container(make_container, memory)
    suffix = uuid7().hex[:12]
    group = f"p:{suffix}"
    source = f"cmdb:{suffix}"
    reference_time = datetime(2023, 2, 3, 4, 5, tzinfo=UTC)
    await _seed_published_episode(
        sessionmaker, group=group, source=source, reference_time=reference_time
    )

    assert await rebuild_group(container, group) == 1
    assert memory.cleared_groups == [group]
    assert [episode.reference_time for episode in memory.episodes] == [reference_time]


async def test_lane_pool_serializes_per_group_and_parallelizes_across_groups(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
) -> None:
    recording = RecordingMemoryEngine(delay_s=0.08)
    container = make_container(recording)
    sfx = uuid7().hex[:12]
    group_a = f"p:a-{sfx}"
    group_b = f"p:b-{sfx}"
    sources_a = [f"a{i}:{sfx}" for i in range(3)]
    sources_b = [f"b{i}:{sfx}" for i in range(3)]
    for src in sources_a:
        await _enqueue(container, group=group_a, source=src)
    for src in sources_b:
        await _enqueue(container, group=group_b, source=src)

    pool = LanePool(container, lanes=4, queue_maxsize=8)
    pool.start()
    try:
        processed = await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()

    assert processed >= 6
    mine = [iv for iv in recording.intervals if iv[0] in {group_a, group_b}]
    # Exactly-once: each source processed a single time.
    assert sorted(iv[1] for iv in mine) == sorted(sources_a + sources_b)
    # Per-group serialization: no two same-group episodes overlapped.
    assert recording.same_group_overlap is False
    # Cross-group parallelism: some A interval overlapped some B interval.
    a_iv = [iv for iv in mine if iv[0] == group_a]
    b_iv = [iv for iv in mine if iv[0] == group_b]
    crossed = any(a[2] < b[3] and b[2] < a[3] for a in a_iv for b in b_iv)
    assert crossed is True

    # Every job for these groups is done and has a durable completion timestamp.
    async with sessionmaker() as s:
        remaining = await s.scalar(
            text(
                "SELECT count(*) FROM ingestion_jobs "
                "WHERE group_id IN (:a, :b) AND (status <> 'done' OR completed_at IS NULL)"
            ),
            {"a": group_a, "b": group_b},
        )
    assert remaining == 0
