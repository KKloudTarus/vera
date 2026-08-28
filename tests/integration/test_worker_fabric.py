"""Worker cutover with real provenance (Phase A slice), against the live database.

With memory.fabric_enabled the ingestion worker reconciles each episode's triples into the
authoritative fact store using the real trust/authority/version provenance the publish path
puts in the job's `_fabric` block (no hard-coded authority), and a new artifact version
withdraws a dropped proposition's assertion and retracts the fact that loses its support.
Uses the null memory engine (the fabric step reads the job payload, not the graph).
"""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.graph.null import NullMemoryEngine
from vera.adapters.persistence.models.knowledge import ArtifactRow, ArtifactVersionRow
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.bootstrap import Container
from vera.domain.curation.trust import authority_for_tier
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.time import utc_now
from vera.shared.types import GroupId, SourceId

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
def fabric_container(make_container: Callable[[object], Container]) -> Container:
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


async def _artifact_versions(container: Container, group: str) -> tuple[UUID, UUID]:
    """One artifact with two versions in a real tenancy; returns (v1_id, v2_id)."""
    sm = container.sessionmaker
    async with SqlAlchemyUnitOfWork(sm) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{group}", name="O", group_id=f"o:{group}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{group}", name="W", group_id=f"w:{group}"
        )
        proj = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{group}", name="P", group_id=group
        )
        source_id = await uow.sources.create(
            workspace_id=ws.id, project_id=proj.id, kind="confluence", name="C", trust_tier=1
        )
        await uow.commit()
    async with _tenant(sm, group) as s:
        art = ArtifactRow(
            source_id=source_id,
            external_id="page-1",
            content_hash="h1",
            s3_key="k1",
            reference_time=utc_now(),
        )
        s.add(art)
        await s.flush()
        v1 = ArtifactVersionRow(
            artifact_id=art.id, version=1, content_hash="h1", s3_key="k1", reference_time=utc_now()
        )
        v2 = ArtifactVersionRow(
            artifact_id=art.id, version=2, content_hash="h2", s3_key="k2", reference_time=utc_now()
        )
        s.add_all([v1, v2])
        await s.flush()
        return v1.id, v2.id


def _fabric_meta(tier: int, version_id: UUID) -> dict[str, object]:
    return {
        "trust_tier": tier,
        "authority": authority_for_tier(tier),
        "confidence": 0.9,
        "verification": "human_verified",
        "ontology_version_id": None,
        "artifact_version_id": str(version_id),
    }


async def _enqueue(
    container: Container, group: str, source: str, triples: list[dict], meta: dict
) -> None:
    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={"triples": triples, "_fabric": meta},
    )


async def _drain(container: Container) -> None:
    pool = LanePool(container, lanes=1, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()


async def _fact_state(container: Container, group: str, obj: str) -> str | None:
    async with _tenant(container.sessionmaker, group) as s:
        return await s.scalar(
            text(
                "SELECT lifecycle_state FROM facts WHERE group_id = :g AND object_scalar = :o "
                "ORDER BY system_from DESC LIMIT 1"
            ),
            {"g": group, "o": obj},
        )


async def test_worker_uses_real_source_authority_not_hardcoded(fabric_container: Container) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    v1, _ = await _artifact_versions(container, group)
    source = f"{group}:{uuid7()}"
    # A Tier 2 (curated) source: authority must be 0.85, never the old hard-coded 1.0.
    await _enqueue(
        container,
        group,
        source,
        [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}],
        _fabric_meta(2, v1),
    )
    await _drain(container)
    async with _tenant(container.sessionmaker, group) as s:
        authority = await s.scalar(
            text("SELECT authority FROM facts WHERE group_id = :g"), {"g": group}
        )
    assert authority == pytest.approx(0.85)
    assert await _fact_state(container, group, "eks") == "active"

    # Replaying the same episode does not duplicate.
    await _enqueue(
        container,
        group,
        source,
        [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}],
        _fabric_meta(2, v1),
    )
    await _drain(container)
    async with _tenant(container.sessionmaker, group) as s:
        n = await s.scalar(text("SELECT count(*) FROM facts WHERE group_id = :g"), {"g": group})
    assert n == 1


async def test_new_version_withdraws_dropped_proposition_on_live_path(
    fabric_container: Container,
) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    v1, v2 = await _artifact_versions(container, group)

    # v1 asserts two facts; both active.
    await _enqueue(
        container,
        group,
        f"{group}:{uuid7()}",
        [
            {"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"},
            {"subject": "paymentapi", "predicate": "DEPENDS_ON", "object": "postgres"},
        ],
        _fabric_meta(1, v1),
    )
    await _drain(container)
    assert await _fact_state(container, group, "eks") == "active"
    assert await _fact_state(container, group, "postgres") == "active"

    # v2 of the SAME artifact keeps eks but drops depends_on postgres.
    await _enqueue(
        container,
        group,
        f"{group}:{uuid7()}",
        [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}],
        _fabric_meta(1, v2),
    )
    await _drain(container)

    assert await _fact_state(container, group, "eks") == "active"  # still supported by v2
    assert await _fact_state(container, group, "postgres") == "retracted"  # lost its only support
    # The prior version's postgres assertion is withdrawn (history preserved, not deleted).
    async with _tenant(container.sessionmaker, group) as s:
        withdrawn = await s.scalar(
            text("SELECT count(*) FROM assertions WHERE group_id = :g AND state = 'withdrawn'"),
            {"g": group},
        )
    assert withdrawn >= 1


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
    async with _tenant(container.sessionmaker, group) as s:
        facts = await s.scalar(text("SELECT count(*) FROM facts WHERE group_id = :g"), {"g": group})
    assert facts == 0  # legacy path only
