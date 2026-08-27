"""Ingestion plane: queue lifecycle, idempotency, reaper, DLQ, and the lane pool's
per-group serialization with cross-group parallelism. Runs against the live database.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.bootstrap import Container
from vera.domain.ports.memory_engine import EpisodeSpec, GraphHit, IngestReceipt
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
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


async def _enqueue(container: Container, *, group: str, source: str) -> None:
    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={"body": f"content for {source}"},
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

    await container.queue.fail(job.id, error="transient", retry_in_s=30)
    async with sessionmaker() as s:
        status = await s.scalar(
            text("SELECT status FROM ingestion_jobs WHERE id = :id"), {"id": job.id}
        )
    assert status == "pending"

    # Exhaust attempts, then a failure dead-letters the job.
    async with sessionmaker() as s, s.begin():
        await s.execute(
            text("UPDATE ingestion_jobs SET attempts = max_attempts WHERE id = :id"),
            {"id": job.id},
        )
    await container.queue.fail(job.id, error="permanent", retry_in_s=1)
    async with sessionmaker() as s:
        status = await s.scalar(
            text("SELECT status FROM ingestion_jobs WHERE id = :id"), {"id": job.id}
        )
    assert status == "dead"


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

    # Every job for these groups is done.
    async with sessionmaker() as s:
        remaining = await s.scalar(
            text(
                "SELECT count(*) FROM ingestion_jobs "
                "WHERE group_id IN (:a, :b) AND status <> 'done'"
            ),
            {"a": group_a, "b": group_b},
        )
    assert remaining == 0
