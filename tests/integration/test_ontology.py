"""Ontology versioning on episodes and rebuilding a wiped graph from Postgres."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.adapters.graph.offline import DeterministicEmbedder, NoCrossEncoder, NoLLMClient
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.application.queries.search_memory import SearchMemory, SearchMemoryHandler
from vera.bootstrap import Container
from vera.entrypoints.reprocess import rebuild_group
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.ids import uuid7
from vera.shared.types import GroupId

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


async def _provision_and_ingest(
    sessionmaker: async_sessionmaker[AsyncSession], *, group: str, obj: str
) -> UUID:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{group}", name="Org", group_id=f"o:{group}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{group}", name="WS", group_id=f"w:{group}"
        )
        proj = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{group}", name="Proj", group_id=group
        )
        source_id = await uow.sources.create(
            workspace_id=ws.id, project_id=proj.id, kind="cmdb", name="CMDB", trust_tier=1
        )
        service = CurationService(uow, StructuredClaimExtractor())
        await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=group,
                external_id=f"rec-{obj}",
                body="",
                knowledge_type="fact_triple",
                metadata={
                    "triples": [{"subject": "paymentapi", "predicate": "RUNSON", "object": obj}]
                },
            )
        )
        await uow.commit()
    return source_id


async def test_published_episode_carries_ontology_and_pipeline_versions(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    await _provision_and_ingest(sessionmaker, group=group, obj="prodeksmy")

    async with sessionmaker() as s:
        row = (
            await s.execute(
                text(
                    "SELECT ontology_version_id, pipeline FROM published_episodes "
                    "WHERE group_id = :g"
                ),
                {"g": group},
            )
        ).first()
    assert row is not None
    ontology_version_id, pipeline = row
    assert ontology_version_id is not None  # references the active ontology
    assert pipeline.get("ontology") == "1"
    assert pipeline.get("model")  # a model version is recorded


async def _find_fact(handler: SearchMemoryHandler, group: str, needle: str) -> bool:
    hits = await handler.handle(
        SearchMemory(text="paymentapi", group_ids=(GroupId(group),), limit=10)
    )
    return any(needle in h.fact for h in hits)


async def test_rebuild_reconstructs_an_equivalent_graph(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    await _provision_and_ingest(sessionmaker, group=group, obj="prodeksmy")

    container = make_container(graphiti_engine)
    pool = LanePool(container, lanes=2, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()

    handler = SearchMemoryHandler(container.memory, container.retrieval_read)
    assert await _find_fact(handler, group, "prodeksmy")

    # Wipe the graph for the group: memory is gone but Postgres still has the episode.
    await container.memory.clear_group(group)
    assert not await _find_fact(handler, group, "prodeksmy")

    # Reprocess replays the episodes and rebuilds an equivalent graph.
    replayed = await rebuild_group(container, group)
    assert replayed >= 1
    assert await _find_fact(handler, group, "prodeksmy")
