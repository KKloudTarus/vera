"""Outbox-driven fact projection (gap 8): reconciliation enqueues a projection job, and the
worker projects the group's active facts into the graph downstream of the fact store, rather
than writing the graph synchronously.
"""

from __future__ import annotations

import dataclasses
import hashlib
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import cast
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.adapters.graph.offline import (
    DeterministicCommunityLLM,
    DeterministicEmbedder,
    NoCrossEncoder,
)
from vera.adapters.persistence.repositories.community import (
    SqlAlchemyCommunityLineageRepository,
)
from vera.adapters.persistence.repositories.projection import SqlAlchemyProjectionSource
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.application.projection.service import FactProjectionService
from vera.bootstrap import Container
from vera.domain.ports.identity import ResolvedScope, ScopeResolver
from vera.entrypoints.build_communities import build_group
from vera.entrypoints.knowledge.service import KnowledgeService
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import expire_due_facts, run_until_empty
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.types import GroupId, SourceId

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Scope:
    def __init__(self, group_id: str) -> None:
        self._group_id = group_id

    async def resolve(self, principal_id: UUID) -> ResolvedScope:
        return ResolvedScope(
            group_ids=(self._group_id,),
            personal_group_id=f"u:{principal_id}",
            primary_workspace_id=None,
        )


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
        llm_client=DeterministicCommunityLLM(),
        cross_encoder=NoCrossEncoder(),
    )
    engine = GraphitiMemoryEngine(client)
    if not await engine.health():
        await client.close()
        if os.environ.get("CI"):
            pytest.fail("Neo4j not reachable in CI")
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


async def _provision(container: Container, group: str) -> tuple[UUID, UUID]:
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{group}", name="O", group_id=f"o:{group}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{group}", name="W", group_id=f"w:{group}"
        )
        project = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{group}", name="P", group_id=group
        )
        await uow.commit()
        return ws.id, project.id


async def _drain(container: Container) -> None:
    pool = LanePool(container, lanes=1, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()


@pytest.mark.issue6_acceptance
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

    assert await build_group(container, group) >= 1
    communities = await container.memory.search_communities(
        group_ids=(GroupId(group),), query=None, limit=10
    )
    assert communities
    community = communities[0]
    assert community.derived is True
    assert community.derivation_run_id is not None
    assert community.source_fact_set_hash is not None
    assert community.projection_checkpoint is not None
    lineage = await SqlAlchemyCommunityLineageRepository(container.reads).page(
        group_ids=(group,),
        community_id=UUID(community.community_id),
        derivation_run_id=UUID(community.derivation_run_id),
        cursor=None,
        limit=10,
    )
    assert lineage.items
    assert {item.fact_key for item in lineage.items} == active
    expected_hash = hashlib.sha256(
        "\n".join(sorted(str(item.fact_id) for item in lineage.items)).encode()
    ).hexdigest()
    assert community.source_fact_set_hash == expected_hash

    assert await build_group(container, group) >= 1
    rebuilt = await container.memory.search_communities(
        group_ids=(GroupId(group),), query=None, limit=10
    )
    assert rebuilt
    community = rebuilt[0]
    assert community.source_fact_set_hash == expected_hash
    assert community.projection_checkpoint == expected_hash

    knowledge = KnowledgeService(container, cast("ScopeResolver", _Scope(group)))
    derived = await knowledge.communities(uuid7(), project=group)
    assert derived
    assert derived[0]["derived"] is True
    assert derived[0]["authoritative"] is False
    assert derived[0]["evidence"] is None
    governed_lineage = await knowledge.community_lineage(
        uuid7(),
        community_id=community.community_id,
        derivation_run_id=community.derivation_run_id,
    )
    assert governed_lineage is not None
    authoritative = await knowledge.search(uuid7(), query="paymentapi", project=group)
    assert all(result["kind"] != "community_summary" for result in authoritative["results"])

    async with container.workers() as session, session.begin():
        await session.execute(
            text(
                "UPDATE facts SET expires_at = now() - interval '1 second' WHERE group_id = :group"
            ),
            {"group": group},
        )
    assert await expire_due_facts(container) == 1
    await _drain(container)

    assert not await proj_source.active_fact_keys(group_id=group)
    drift = await FactProjectionService(
        source=proj_source, projection=container.fact_projection
    ).verify_group(group)
    assert drift.in_sync, f"missing={drift.missing_in_graph} extra={drift.extra_in_graph}"

    await container.fact_projection.clear(group_id=group)


@pytest.mark.issue6_acceptance
async def test_same_source_replacement_converges_incremental_graph_projection(
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    container = make_container(graphiti_engine)
    memory = container.settings.memory.model_copy(update={"fabric_write_mode": "fabric"})
    container = dataclasses.replace(
        container, settings=container.settings.model_copy(update={"memory": memory})
    )
    assert container.fact_projection is not None
    group = f"p:replace-{uuid7().hex[:12]}"
    workspace_id, project_id = await _provision(container, group)
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group)
        source_id = await uow.sources.create(
            workspace_id=workspace_id,
            project_id=project_id,
            kind="cmdb",
            name="CMDB",
            trust_tier=1,
        )
        await uow.commit()

    async def ingest(revision: int, target: str) -> None:
        async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
            await uow.use_tenant(group)
            body = f"paymentapi runs on {target}"
            await CurationService(uow, StructuredClaimExtractor()).ingest_artifact(
                IngestArtifact(
                    source_id=source_id,
                    group_id=group,
                    external_id="payment-runtime",
                    body=body,
                    knowledge_type="fact_triple",
                    metadata={
                        "triples": [
                            {
                                "subject": "paymentapi",
                                "predicate": "RUNS_ON",
                                "object": target,
                                "source_quote": body,
                                "quote_start": 0,
                                "quote_end": len(body),
                            }
                        ]
                    },
                    source_revision=revision,
                )
            )
            await uow.commit()
        await _drain(container)

    try:
        await ingest(1, "eks")
        await ingest(2, "ecs")

        source = SqlAlchemyProjectionSource(container.sessionmaker)
        active = await source.active_fact_keys(group_id=group)
        projected = await container.fact_projection.projected_fact_keys(group_id=group)
        assert projected == active
        async with container.sessionmaker() as session:
            states = {
                str(row.object_name): (str(row.lifecycle_state), str(row.fact_key))
                for row in (
                    await session.execute(
                        text(
                            "SELECT co.canonical_name AS object_name, f.lifecycle_state, "
                            "f.fact_key "
                            "FROM facts f LEFT JOIN canonical_entities co "
                            "ON co.id = f.object_entity_id WHERE f.group_id = :group"
                        ),
                        {"group": group},
                    )
                )
            }
        assert states["eks"][0] == "retracted"
        assert states["ecs"][0] == "active"
        assert projected == {states["ecs"][1]}
        assert states["eks"][1] not in projected

        # A stale source replay emits no regression and leaves the graph converged.
        await ingest(1, "eks")
        assert await container.fact_projection.projected_fact_keys(group_id=group) == active
    finally:
        await container.fact_projection.clear(group_id=group)
