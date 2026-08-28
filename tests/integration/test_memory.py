"""Graphiti memory engine against a live Neo4j: ingest, search, as_of, health, the
port contract, and the worker's canonical stitching. Uses the deterministic embedder
and the no-LLM client, so no external provider is needed.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from datetime import timedelta

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.contracts import assert_memory_contract
from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.adapters.graph.offline import DeterministicEmbedder, NoCrossEncoder, NoLLMClient
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.application.queries.search_memory import SearchMemory, SearchMemoryHandler
from vera.bootstrap import Container
from vera.domain.ports.memory_engine import EpisodeSpec, GraphQuery
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.errors import Ok
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.time import utc_now
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


async def _ingest_triple(engine: GraphitiMemoryEngine, *, group: str, source: str) -> None:
    await engine.ingest_episode(
        EpisodeSpec(
            source_id=SourceId(source),
            group_id=GroupId(group),
            body="",
            reference_time=utc_now(),
            knowledge_type="fact_triple",
            metadata={
                "triples": [
                    {
                        "subject": "paymentapi",
                        "predicate": "RUNSON",
                        "object": "prodeksmy",
                        "entity_type": "Service",
                    }
                ]
            },
        )
    )


async def _make_edge(
    engine: GraphitiMemoryEngine, *, group: str, subject: str, obj: str, uuid: str
) -> None:
    # Seed a RELATES_TO edge directly. add_triplet would reconcile overlapping edges via
    # the LLM (unavailable offline), so connected edges are written straight to the graph.
    gid = group.replace(":", "_")
    await engine._client.driver.execute_query(  # test-only graph seeding
        "MERGE (s:Entity {group_id: $g, name: $subject}) "
        "MERGE (o:Entity {group_id: $g, name: $obj}) "
        "CREATE (s)-[:RELATES_TO {group_id: $g, name: 'DEPENDS_ON', uuid: $uuid, "
        "fact: $fact, invalid_at: null}]->(o)",
        g=gid,
        subject=subject,
        obj=obj,
        uuid=uuid,
        fact=f"{subject} DEPENDS_ON {obj}",
    )


async def test_neighbors_traverses_multiple_hops(
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    # A -> B -> C: from A, depth 2 should reach both the A->B and B->C edges.
    await _make_edge(graphiti_engine, group=group, subject="alpha", obj="bravo", uuid="edge-ab")
    await _make_edge(graphiti_engine, group=group, subject="bravo", obj="charlie", uuid="edge-bc")

    one_hop = await graphiti_engine.neighbors(
        group_ids=(GroupId(group),), center="alpha", depth=1, limit=20
    )
    assert any("bravo" in h.fact for h in one_hop)
    assert not any("charlie" in h.fact for h in one_hop)  # charlie is two hops away

    two_hop = await graphiti_engine.neighbors(
        group_ids=(GroupId(group),), center="alpha", depth=2, limit=20
    )
    facts = " ".join(h.fact for h in two_hop)
    assert "bravo" in facts and "charlie" in facts  # reached via the B->C edge


async def test_graphiti_satisfies_contract(graphiti_engine: GraphitiMemoryEngine) -> None:
    await assert_memory_contract(graphiti_engine, group=f"p:{uuid7().hex[:12]}")


async def test_ingest_triple_then_search_finds_fact(
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    await _ingest_triple(graphiti_engine, group=group, source=f"cmdb:{uuid7().hex[:8]}")

    hits = await graphiti_engine.search(
        GraphQuery(text="paymentapi", group_ids=(GroupId(group),), limit=10)
    )
    assert any("paymentapi" in h.fact for h in hits)


async def test_as_of_excludes_facts_not_yet_valid(
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    await _ingest_triple(graphiti_engine, group=group, source=f"cmdb:{uuid7().hex[:8]}")
    before = utc_now() - timedelta(days=1)

    hits = await graphiti_engine.search(
        GraphQuery(text="paymentapi", group_ids=(GroupId(group),), limit=10, as_of=before)
    )
    assert all("paymentapi" not in h.fact for h in hits)


async def test_worker_stitches_canonical_and_graph_map(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    container = make_container(graphiti_engine)
    group = f"p:{uuid7().hex[:12]}"
    source = f"cmdb:{uuid7().hex[:8]}"
    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={
            "triples": [
                {
                    "subject": "paymentapi",
                    "predicate": "RUNSON",
                    "object": "prodeksmy",
                    "entity_type": "Service",
                }
            ]
        },
    )
    pool = LanePool(container, lanes=2, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()

    async with sessionmaker() as s:
        entities = await s.scalar(
            text("SELECT count(*) FROM canonical_entities WHERE group_id = :g"), {"g": group}
        )
        mapped_nodes = await s.scalar(
            text("SELECT count(*) FROM graph_node_map WHERE group_id = :g"), {"g": group}
        )
        mapped_edges = await s.scalar(
            text("SELECT count(*) FROM graph_edge_map WHERE group_id = :g"), {"g": group}
        )
    assert entities >= 2  # paymentapi and prodeksmy
    assert mapped_nodes >= 2
    assert mapped_edges >= 1


async def test_curation_to_graph_end_to_end(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    # Curation: an authoritative source auto-publishes a claim and enqueues a job.
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
                external_id="rec-e2e",
                body="",
                knowledge_type="fact_triple",
                metadata={
                    "triples": [
                        {"subject": "paymentapi", "predicate": "RUNSON", "object": "prodeksmy"}
                    ]
                },
            )
        )
        await uow.commit()

    # The worker drains the enqueued job into the graph.
    container = make_container(graphiti_engine)
    pool = LanePool(container, lanes=2, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()

    async with sessionmaker() as s:
        entities = await s.scalar(
            text("SELECT count(*) FROM canonical_entities WHERE group_id = :g"), {"g": group}
        )
        mapped_nodes = await s.scalar(
            text("SELECT count(*) FROM graph_node_map WHERE group_id = :g"), {"g": group}
        )
    assert entities >= 2
    assert mapped_nodes >= 2


async def _publish_and_ingest(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
    *,
    group: str,
) -> Container:
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
                external_id="rec-prov",
                body="",
                knowledge_type="fact_triple",
                metadata={
                    "triples": [
                        {"subject": "paymentapi", "predicate": "RUNSON", "object": "prodeksmy"}
                    ]
                },
            )
        )
        await uow.commit()

    container = make_container(graphiti_engine)
    pool = LanePool(container, lanes=2, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()
    return container


async def test_retract_source_removes_it_from_memory(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    from vera.adapters.persistence.retraction import RetractionService

    group = f"p:{uuid7().hex[:12]}"
    container = await _publish_and_ingest(
        sessionmaker, make_container, graphiti_engine, group=group
    )
    handler = SearchMemoryHandler(container.memory, container.retrieval_read)
    hits = await handler.handle(
        SearchMemory(text="paymentapi", group_ids=(GroupId(group),), limit=5)
    )
    target = next(h for h in hits if "paymentapi" in h.fact)
    assert target.source_id is not None

    service = RetractionService(sessionmaker, container.memory, container.object_store)
    result = await service.retract_source(group_id=group, source_id=target.source_id)
    assert isinstance(result, Ok)
    assert result.value.edges_removed >= 1

    # Gone from the current view, its graph map cleared, and marked retracted in Postgres.
    after = await handler.handle(
        SearchMemory(text="paymentapi", group_ids=(GroupId(group),), limit=5)
    )
    assert all("paymentapi" not in h.fact for h in after)
    async with sessionmaker() as s:
        edges = await s.scalar(
            text("SELECT count(*) FROM graph_edge_map WHERE group_id = :g"), {"g": group}
        )
        retracted = await s.scalar(
            text(
                "SELECT count(*) FROM published_episodes "
                "WHERE group_id = :g AND retracted_at IS NOT NULL"
            ),
            {"g": group},
        )
    assert edges == 0
    assert retracted == 1


async def test_retract_cleanup_job_removes_edges_durably(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    # Simulate a crash after the retraction commit but before in-process cleanup: the durable
    # retract_cleanup job alone must still remove the fact from the graph when the worker runs.
    group = f"p:{uuid7().hex[:12]}"
    container = await _publish_and_ingest(
        sessionmaker, make_container, graphiti_engine, group=group
    )
    handler = SearchMemoryHandler(container.memory, container.retrieval_read)
    hits = await handler.handle(
        SearchMemory(text="paymentapi", group_ids=(GroupId(group),), limit=5)
    )
    assert any("paymentapi" in h.fact for h in hits)

    async with sessionmaker() as session:
        rows = await session.execute(
            text("SELECT edge_uuid FROM graph_edge_map WHERE group_id = :g"), {"g": group}
        )
        edge_uuids = [str(r) for (r,) in rows]
    assert edge_uuids

    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(f"{group}:cleanup"),
        dedup_uuid=uuid7(),
        payload={
            "job_kind": "retract_cleanup",
            "edge_uuids": edge_uuids,
            "s3_keys": [],
            "erase": False,
        },
    )
    pool = LanePool(container, lanes=2, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()

    after = await handler.handle(
        SearchMemory(text="paymentapi", group_ids=(GroupId(group),), limit=5)
    )
    assert all("paymentapi" not in h.fact for h in after)
    # The graph projection no longer has the edge, though its PG map row remains: cleanup of
    # graph_edge_map is RetractionService's job, which this durable-job path does not do.
    async with sessionmaker() as s:
        retracted = await s.scalar(
            text(
                "SELECT count(*) FROM published_episodes "
                "WHERE group_id = :g AND retracted_at IS NOT NULL"
            ),
            {"g": group},
        )
    assert retracted == 0  # the cleanup job alone does not touch published_episodes


async def test_search_carries_provenance_and_feedback_lowers_score(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    container = await _publish_and_ingest(
        sessionmaker, make_container, graphiti_engine, group=group
    )
    handler = SearchMemoryHandler(container.memory, container.retrieval_read)

    hits = await handler.handle(
        SearchMemory(text="paymentapi", group_ids=(GroupId(group),), limit=5)
    )
    top = next(h for h in hits if "paymentapi" in h.fact)
    assert top.verification == "human_verified"
    assert top.authority == 1.0  # tier-1 authoritative source
    assert top.source_id is not None
    score_before = top.score

    async with sessionmaker() as s:
        edge_uuid = await s.scalar(
            text("SELECT edge_uuid FROM graph_edge_map WHERE group_id = :g LIMIT 1"), {"g": group}
        )
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        for _ in range(5):
            await uow.feedback.record(
                group_id=group,
                principal_id=None,
                query="paymentapi",
                result_ref=str(edge_uuid),
                signal="down",
            )
        await uow.commit()

    hits_after = await handler.handle(
        SearchMemory(text="paymentapi", group_ids=(GroupId(group),), limit=5)
    )
    top_after = next(h for h in hits_after if "paymentapi" in h.fact)
    assert top_after.score < score_before
