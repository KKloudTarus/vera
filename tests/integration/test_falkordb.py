"""FalkorDB graph backend against a live FalkorDB: ingest, search, temporal, retract.

VERA runs its own fulltext edge search on FalkorDB (Graphiti's hybrid search does not
return results there), so this guards that path. Uses the deterministic embedder and the
no-LLM client, so no external provider is needed. Skips if FalkorDB is unreachable.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.adapters.graph.offline import (
    DeterministicCommunityLLM,
    DeterministicEmbedder,
    NoCrossEncoder,
)
from vera.adapters.persistence.repositories import SqlAlchemyCanonicalEntityRepository
from vera.adapters.persistence.repositories.community import SqlAlchemyCommunityLineageRepository
from vera.adapters.persistence.repositories.fabric import SqlAlchemyFactRepository
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.bootstrap import Container
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Fact, FactLifecycle, ObjectType
from vera.domain.ports.memory_engine import EpisodeSpec, GraphQuery
from vera.entrypoints.build_communities import build_group
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import GroupId, SourceId

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


@pytest_asyncio.fixture
async def falkordb_engine() -> AsyncIterator[GraphitiMemoryEngine]:
    from graphiti_core import Graphiti
    from graphiti_core.driver.falkordb_driver import FalkorDriver

    port = int(os.environ.get("VERA_FALKOR__PORT", "6380"))
    client = Graphiti(
        graph_driver=FalkorDriver(
            host=os.environ.get("VERA_FALKOR__HOST", "localhost"),
            port=port,
            database="default_db",
        ),
        embedder=DeterministicEmbedder(1024),
        llm_client=DeterministicCommunityLLM(),
        cross_encoder=NoCrossEncoder(),
    )
    engine = GraphitiMemoryEngine(client)
    if not await engine.health():
        await client.close()
        if os.environ.get("CI"):
            pytest.fail("FalkorDB is required in CI")
        pytest.skip("FalkorDB not reachable")
    assert engine._falkordb is True
    await engine.ensure_schema()
    try:
        yield engine
    finally:
        await client.close()


async def _ingest(engine: GraphitiMemoryEngine, *, group: str, obj: str) -> None:
    await engine.ingest_episode(
        EpisodeSpec(
            source_id=SourceId(f"cmdb:{uuid7().hex[:8]}"),
            group_id=GroupId(group),
            body="",
            reference_time=utc_now(),
            knowledge_type="fact_triple",
            metadata={
                "triples": [
                    {
                        "subject": "paymentapi",
                        "predicate": "RUNS_ON",
                        "object": obj,
                        "entity_type": "Service",
                    }
                ]
            },
        )
    )


async def test_ingest_then_search_returns_the_fact(
    falkordb_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    await _ingest(falkordb_engine, group=group, obj="prod-eks")

    # This is the path that returned 0 before VERA's native FalkorDB fulltext search.
    hits = await falkordb_engine.search(
        GraphQuery(text="where does paymentapi run", group_ids=(GroupId(group),), limit=10)
    )
    assert any("paymentapi" in h.fact for h in hits)


async def test_vector_half_returns_when_fulltext_cannot_match(
    falkordb_engine: GraphitiMemoryEngine,
) -> None:
    # A query whose words are absent from the fact cannot match via fulltext; a hit here
    # proves the vector half of the hybrid search is wired (edge fact_embedding + cosine).
    group = f"p:{uuid7().hex[:12]}"
    await _ingest(falkordb_engine, group=group, obj="prod-eks")

    hits = await falkordb_engine.search(
        GraphQuery(text="kubernetes deployment location", group_ids=(GroupId(group),), limit=10)
    )
    assert any("paymentapi" in h.fact for h in hits)


async def test_as_of_past_excludes_the_fact(
    falkordb_engine: GraphitiMemoryEngine,
) -> None:
    from datetime import timedelta

    group = f"p:{uuid7().hex[:12]}"
    await _ingest(falkordb_engine, group=group, obj="prod-eks")
    before = utc_now() - timedelta(days=1)

    hits = await falkordb_engine.search(
        GraphQuery(text="paymentapi", group_ids=(GroupId(group),), limit=10, as_of=before)
    )
    assert all("paymentapi" not in h.fact for h in hits)


@pytest.mark.issue6_acceptance
async def test_build_communities_returns_lineage_tagged_derived_summary(
    falkordb_engine: GraphitiMemoryEngine,
    make_container: Callable[[object], Container],
) -> None:
    container = make_container(falkordb_engine)
    group = f"p:{uuid7().hex[:12]}"
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{group}", name="O", group_id=f"o:{group}"
        )
        workspace = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{group}", name="W", group_id=f"w:{group}"
        )
        await uow.tenancy.create_project(
            workspace_id=workspace.id, slug=f"p-{group}", name="P", group_id=group
        )
        await uow.commit()
    async with _tenant(container.sessionmaker, group) as session:
        entity = await SqlAlchemyCanonicalEntityRepository(session).create(
            group_id=group,
            entity_type="Service",
            canonical_name="paymentapi",
            aliases=[],
        )
        key = fabric.fact_key(
            scope=group,
            subject_entity_id=entity.id,
            predicate="RUNS_ON",
            object_scalar="prod-eks",
        )
        fact = await SqlAlchemyFactRepository(session).upsert(
            Fact(
                id=uuid7(),
                group_id=group,
                fact_key=key,
                slot_key=fabric.slot_key(
                    scope=group, subject_entity_id=entity.id, predicate="RUNS_ON"
                ),
                subject_entity_id=entity.id,
                predicate="RUNS_ON",
                object_type=ObjectType.SCALAR,
                normalized_object=fabric.normalize_object(object_scalar="prod-eks"),
                object_scalar="prod-eks",
                lifecycle_state=FactLifecycle.ACTIVE,
                authority=1.0,
                confidence=0.9,
            )
        )

    assert await build_group(container, group) >= 1

    results = await falkordb_engine.search_communities(
        group_ids=(GroupId(group),), query=None, limit=10
    )
    assert results
    assert results[0].derived is True
    assert results[0].derivation_run_id is not None
    assert results[0].source_fact_set_hash is not None
    lineage = await SqlAlchemyCommunityLineageRepository(container.reads).page(
        group_ids=(group,),
        community_id=UUID(results[0].community_id),
        derivation_run_id=UUID(results[0].derivation_run_id),
        cursor=None,
        limit=10,
    )
    assert [item.fact_id for item in lineage.items] == [fact.id]
