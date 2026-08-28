"""Worker cutover: with memory.fabric_enabled the ingestion worker also reconciles each
episode's triples into the authoritative fact store (Phase 8), and is idempotent on replay.
Uses the null memory engine (the fabric step reads the job payload, not the graph).
"""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.graph.null import NullMemoryEngine
from vera.bootstrap import Container
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.types import GroupId, SourceId

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
def fabric_container(make_container: Callable[[object], Container]) -> Container:
    # Reuse the live-DB-wired container and only flip the flag, so the DSN is the working one
    # (avoids the get_settings lru-cache/env-timing trap of building a fresh container).
    container = make_container(NullMemoryEngine())
    memory = container.settings.memory.model_copy(update={"fabric_enabled": True})
    settings = container.settings.model_copy(update={"memory": memory})
    return dataclasses.replace(container, settings=settings)


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


async def _drain(container: Container) -> None:
    pool = LanePool(container, lanes=1, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()


async def _fact_counts(container: Container, group: str) -> tuple[int, int]:
    async with _tenant(container.sessionmaker, group) as s:
        facts = await s.scalar(text("SELECT count(*) FROM facts WHERE group_id = :g"), {"g": group})
        assertions = await s.scalar(
            text("SELECT count(*) FROM assertions WHERE group_id = :g"), {"g": group}
        )
    return facts, assertions


async def test_worker_populates_fabric_when_enabled_and_is_idempotent(
    fabric_container: Container,
) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    source = f"{group}:{uuid7()}"
    payload = {"triples": [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}]}

    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload=payload,
    )
    await _drain(container)
    assert await _fact_counts(container, group) == (1, 1)

    # Replaying the same episode (new job id, same source) does not duplicate the fact.
    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=uuid7(),
        payload=payload,
    )
    await _drain(container)
    assert await _fact_counts(container, group) == (1, 1)


async def test_worker_does_not_touch_fabric_when_disabled(
    make_container: Callable[[object], Container],  # default has fabric_enabled off
) -> None:
    container = make_container(NullMemoryEngine())
    assert container.settings.memory.fabric_enabled is False
    group = f"p:w-{uuid7().hex[:12]}"
    source = f"{group}:{uuid7()}"
    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={"triples": [{"subject": "a", "predicate": "RUNS_ON", "object": "b"}]},
    )
    await _drain(container)
    assert await _fact_counts(container, group) == (0, 0)  # legacy path only
