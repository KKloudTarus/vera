"""Outbox-driven fact projection (gap 8): reconciliation enqueues a projection job, and the
worker projects the group's active facts into the graph downstream of the fact store, rather
than writing the graph synchronously.
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.adapters.graph.offline import DeterministicEmbedder, NoCrossEncoder, NoLLMClient
from vera.adapters.persistence.repositories.projection import SqlAlchemyProjectionSource
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.projection.service import FactProjectionService
from vera.bootstrap import Container
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.types import GroupId, SourceId

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def graphiti_engine() -> AsyncIterator[GraphitiMemoryEngine]:
    from dotenv import load_dotenv
    from graphiti_core import Graphiti

    load_dotenv(override=True)
    client = Graphiti(
        uri=os.environ.get("VERA_NEO4J__URI", "bolt://localhost:7687"),
        user=os.environ.get("VERA_NEO4J__USER", "neo4j"),
        password=os.environ.get("VERA_NEO4J__PASSWORD", "vera-local-pass"),
        embedder=DeterministicEmbedder(1024),
        llm_client=NoLLMClient(),
        cross_encoder=NoCrossEncoder(),
    )
    engine = GraphitiMemoryEngine(client)
    if not await engine.health():
        await client.close()
        pytest.skip("Neo4j not reachable")
    await engine.ensure_schema()
    try:
        yield engine
    finally:
        await client.close()


@asynccontextmanager
async def _tenant(sm: async_sessionmaker[AsyncSession], group: str) -> AsyncIterator[AsyncSession]:
    async with sm() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


async def _provision(container: Container, group: str) -> None:
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{group}", name="O", group_id=f"o:{group}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{group}", name="W", group_id=f"w:{group}"
        )
        await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{group}", name="P", group_id=group
        )
        await uow.commit()


async def _drain(container: Container) -> None:
    pool = LanePool(container, lanes=1, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()


async def test_reconcile_enqueues_projection_and_worker_projects_facts(
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    container = make_container(graphiti_engine)
    assert container.fact_projection is not None  # a real graph => a projection is wired
    memory = container.settings.memory.model_copy(update={"fabric_enabled": True})
    container = dataclasses.replace(
        container, settings=container.settings.model_copy(update={"memory": memory})
    )

    group = f"p:proj-{uuid7().hex[:12]}"
    await _provision(container, group)
    source = f"{group}:{uuid7()}"
    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={
            "triples": [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "prod-eks"}],
            "_fabric": {
                "trust_tier": 1,
                "authority": 1.0,
                "confidence": 0.9,
                "verification": "human_verified",
                "ontology_version_id": None,
                "artifact_version_id": None,
            },
        },
    )

    await _drain(container)

    # A project_facts job flowed through the outbox (graph mutation is downstream, not sync).
    async with container.sessionmaker() as s:
        kinds = list(
            await s.scalars(
                text("SELECT payload->>'job_kind' FROM ingestion_jobs WHERE group_id = :g"),
                {"g": group},
            )
        )
    assert "project_facts" in kinds

    # The projection is in sync with the authoritative active fact set, and non-empty.
    proj_source = SqlAlchemyProjectionSource(container.sessionmaker)
    active = await proj_source.active_fact_keys(group_id=group)
    assert active
    drift = await FactProjectionService(
        source=proj_source, projection=container.fact_projection
    ).verify_group(group)
    assert drift.in_sync, f"missing={drift.missing_in_graph} extra={drift.extra_in_graph}"

    await container.fact_projection.clear(group_id=group)
