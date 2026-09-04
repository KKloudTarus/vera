"""LLM cost tracking and the metrics endpoint against the live stack.

A metered embedder wraps the offline embedder, so ingest and search both write rows to
``llm_usage`` tagged with their request kind. That makes cost per episode and per query
a plain SQL aggregate, and the API exposes the same counters on /metrics.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Callable

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.graph.caching import CachingEmbedder
from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.adapters.graph.metered import MeteredEmbedder
from vera.adapters.graph.offline import DeterministicEmbedder, NoCrossEncoder, NoLLMClient
from vera.adapters.persistence.repositories.usage import SqlAlchemyUsageSink
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.application.queries.search_memory import SearchMemory, SearchMemoryHandler
from vera.bootstrap import Container
from vera.entrypoints.api.main import create_app
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.observability.cost import UsageEvent
from vera.shared.ids import uuid7
from vera.shared.types import GroupId

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_EMBED_MODEL = "text-embedding-3-small"


@pytest_asyncio.fixture
async def metered_engine(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> AsyncIterator[GraphitiMemoryEngine]:
    from dotenv import load_dotenv
    from graphiti_core import Graphiti

    load_dotenv(override=True)
    sink = SqlAlchemyUsageSink(sessionmaker)
    inner = MeteredEmbedder(DeterministicEmbedder(1024), model=_EMBED_MODEL, sink=sink)
    client = Graphiti(
        uri=os.environ.get("VERA_NEO4J__URI", "bolt://localhost:7687"),
        user=os.environ.get("VERA_NEO4J__USER", "neo4j"),
        password=os.environ.get("VERA_NEO4J__PASSWORD", "vera-local-pass"),
        embedder=CachingEmbedder(inner),
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


async def _publish_and_ingest(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    engine: GraphitiMemoryEngine,
    *,
    group: str,
    source: str,
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
        # The worker keys published_episodes by the ingestion source string, so the
        # curation source's external id is that same string.
        service = CurationService(uow, StructuredClaimExtractor())
        await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=group,
                external_id=source,
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

    container = make_container(engine)
    pool = LanePool(container, lanes=2, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()
    return container


async def test_ingest_and_search_record_llm_usage(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    metered_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    source = f"cmdb:{uuid7().hex[:8]}"
    container = await _publish_and_ingest(
        sessionmaker, make_container, metered_engine, group=group, source=source
    )
    sink = SqlAlchemyUsageSink(sessionmaker)

    # Ingest embedded the entities and the fact, so estimated cost is attributed to
    # the episode but remains incomplete because the offline embedder has no usage.
    assert await sink.total_cost_for_group(group) > 0
    async with sessionmaker() as s:
        ingest_rows = await s.scalar(
            text(
                "SELECT count(*) FROM llm_usage "
                "WHERE group_id = :g AND request_kind = 'ingest' AND operation = 'embedding'"
            ),
            {"g": group},
        )
        prompt_tokens = await s.scalar(
            text("SELECT sum(prompt_tokens) FROM llm_usage WHERE group_id = :g"), {"g": group}
        )
        cost_complete = await s.scalar(
            text("SELECT bool_and(cost_complete) FROM llm_usage WHERE group_id = :g"),
            {"g": group},
        )
    assert ingest_rows and ingest_rows >= 1
    assert prompt_tokens and prompt_tokens > 0
    assert cost_complete is False

    # A novel query text misses the embedding cache, so search cost is recorded too.
    handler = SearchMemoryHandler(container.memory, container.retrieval_read)
    await handler.handle(
        SearchMemory(
            text="which cluster runs the billing pipeline today",
            group_ids=(GroupId(group),),
            limit=5,
        )
    )
    async with sessionmaker() as s:
        search_rows = await s.scalar(
            text("SELECT count(*) FROM llm_usage WHERE request_kind = 'search'"),
        )
    assert search_rows and search_rows >= 1


async def test_metrics_endpoint_exposes_vera_metrics(
    make_container: Callable[[object], Container],
    metered_engine: GraphitiMemoryEngine,
) -> None:
    app = create_app()
    app.state.container = make_container(metered_engine)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/metrics")
    assert response.status_code == 200
    body = response.text
    assert "vera_ingestion_jobs_total" in body
    assert "vera_queue_depth" in body
    assert "vera_llm_tokens_total" in body


async def test_old_usage_writer_defaults_to_incomplete(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with sessionmaker() as session, session.begin():
        complete = await session.scalar(
            text(
                "INSERT INTO llm_usage "
                "(model, operation, request_kind, prompt_tokens, completion_tokens, cost_usd) "
                "VALUES ('legacy-model', 'llm', 'unknown', 1, 1, 0) "
                "RETURNING cost_complete"
            )
        )

    assert complete is False


async def test_disabled_cost_recording_does_not_remove_the_sink(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    ref = f"disabled:{uuid7().hex}"
    sink = SqlAlchemyUsageSink(sessionmaker, record_enabled=False)

    await sink.record(
        UsageEvent(
            model="gpt-4.1-mini",
            operation="llm",
            prompt_tokens=1,
            completion_tokens=1,
            cost_usd=0.000002,
            request_kind="search",
            group_id=None,
            ref=ref,
        )
    )

    async with sessionmaker() as session:
        count = await session.scalar(
            text("SELECT count(*) FROM llm_usage WHERE ref=:ref"), {"ref": ref}
        )
    assert count == 0


async def test_provider_budget_reservation_is_atomic_and_fail_closed(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    sink = SqlAlchemyUsageSink(sessionmaker)
    run_key = f"run:{uuid7()}"
    action_keys = [f"{run_key}:action:{index}" for index in range(2)]
    for action_key in action_keys:
        await sink.initialize_provider_budget(
            action_key, 1.0, run_key=run_key, run_max_cost_usd=1.0
        )

    results = await asyncio.gather(
        *(
            sink.reserve_provider_budget(action_keys[index % len(action_keys)], 0.25)
            for index in range(8)
        )
    )

    async with sessionmaker() as session:
        action_reserved = await session.scalar(
            text(
                "SELECT sum(reserved_cost_usd) FROM provider_budget_reservations "
                "WHERE run_key=:run_key"
            ),
            {"run_key": run_key},
        )
        run_reserved = await session.scalar(
            text(
                "SELECT reserved_cost_usd FROM provider_run_budget_reservations "
                "WHERE run_key=:run_key"
            ),
            {"run_key": run_key},
        )
    assert results.count(True) == 4
    assert results.count(False) == 4
    assert action_reserved == 1.0
    assert run_reserved == 1.0


async def test_provider_budget_settlement_retains_actual_cost_and_releases_the_rest(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    sink = SqlAlchemyUsageSink(sessionmaker)
    run_key = f"settle:{uuid7()}"
    action_key = f"{run_key}:action"
    await sink.initialize_provider_budget(action_key, 1.0, run_key=run_key, run_max_cost_usd=2.0)
    assert await sink.reserve_provider_budget(action_key, 0.75) is True

    assert await sink.settle_provider_budget(action_key, 0.75, 0.125) is True

    async with sessionmaker() as session:
        action_reserved = await session.scalar(
            text(
                "SELECT reserved_cost_usd FROM provider_budget_reservations "
                "WHERE action_key=:action_key"
            ),
            {"action_key": action_key},
        )
        run_reserved = await session.scalar(
            text(
                "SELECT reserved_cost_usd FROM provider_run_budget_reservations "
                "WHERE run_key=:run_key"
            ),
            {"run_key": run_key},
        )
    assert action_reserved == 0.125
    assert run_reserved == 0.125


@pytest.mark.parametrize("maximum", [0.0, -1.0, float("nan"), float("inf")])
async def test_provider_budget_rejects_invalid_maximums(
    sessionmaker: async_sessionmaker[AsyncSession], maximum: float
) -> None:
    sink = SqlAlchemyUsageSink(sessionmaker)

    with pytest.raises(ValueError, match="finite and positive"):
        await sink.initialize_provider_budget(
            f"invalid:{uuid7()}",
            maximum,
            run_key=f"run:{uuid7()}",
            run_max_cost_usd=1.0,
        )
