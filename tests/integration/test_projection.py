"""Graph projection: Graphiti compatibility contract + rebuild equivalence (Phase 3).

The contract tests pin the lower-level driver behavior VERA's projection depends on at the
installed Graphiti 0.29.x, so a bump that changes it fails loudly. The rebuild tests prove the
graph is a rebuildable projection: a rebuild from Postgres reproduces exactly the active fact
set, and drift is detected and repaired (ADR-0003, invariant 10, scenarios 18 and 21).
"""

from __future__ import annotations

import importlib.metadata
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.graph.fact_projection import GraphitiFactProjection
from vera.adapters.graph.offline import DeterministicEmbedder, NoCrossEncoder, NoLLMClient
from vera.adapters.persistence.models.fabric import AssertionRow
from vera.adapters.persistence.models.knowledge import (
    ArtifactRow,
    ArtifactVersionRow,
    PublishedEpisodeRow,
)
from vera.adapters.persistence.repositories import SqlAlchemyCanonicalEntityRepository
from vera.adapters.persistence.repositories.fabric import SqlAlchemyFactRepository
from vera.adapters.persistence.repositories.projection import SqlAlchemyProjectionSource
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.projection import FactProjectionService
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Fact, FactLifecycle, ObjectType
from vera.domain.ports.projection import ProjectedFact
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.time import utc_now

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def graphiti_client() -> AsyncIterator[object]:
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
    try:
        await client.driver.execute_query("RETURN 1")
    except Exception:
        await client.close()
        pytest.skip("Neo4j not reachable")
    try:
        yield client
    finally:
        await client.close()


def _projected(group: str, fact_key: str, subject: str, obj: str) -> ProjectedFact:
    return ProjectedFact(
        group_id=group,
        fact_key=fact_key,
        subject_name=subject,
        predicate="RUNS_ON",
        object_name=obj,
        fact_text=f"{subject} RUNS_ON {obj}",
        authority=1.0,
        confidence=0.9,
        supporting_episode_ids=("ep-1",),
    )


# --------------------------------------------------------- compatibility contract ---


def test_graphiti_version_is_in_the_tested_range() -> None:
    version = importlib.metadata.version("graphiti-core")
    assert version.startswith("0.29."), f"untested graphiti-core {version}; vet the driver contract"


async def test_projection_upsert_is_idempotent_by_fact_key(graphiti_client: object) -> None:
    group = f"p:proj-{uuid7().hex[:12]}"
    projection = GraphitiFactProjection(graphiti_client)
    try:
        await projection.project(_projected(group, "fk-1", "paymentapi", "eks"))
        await projection.project(_projected(group, "fk-1", "paymentapi", "eks"))  # same key
        keys = await projection.projected_fact_keys(group_id=group)
        assert keys == {"fk-1"}  # one edge, not two

        await projection.project(_projected(group, "fk-2", "billing", "ecs"))
        assert await projection.projected_fact_keys(group_id=group) == {"fk-1", "fk-2"}

        await projection.remove(group_id=group, fact_key="fk-1")
        assert await projection.projected_fact_keys(group_id=group) == {"fk-2"}
    finally:
        await projection.clear(group_id=group)


async def test_projection_clear_empties_the_group(graphiti_client: object) -> None:
    group = f"p:proj-{uuid7().hex[:12]}"
    projection = GraphitiFactProjection(graphiti_client)
    await projection.project(_projected(group, "fk-1", "paymentapi", "eks"))
    await projection.clear(group_id=group)
    assert await projection.projected_fact_keys(group_id=group) == set()


# ------------------------------------------------------------- rebuild equivalence ---


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


async def _seed_tenant(sessionmaker: async_sessionmaker[AsyncSession], group: str) -> UUID:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
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
    async with _tenant(sessionmaker, group) as s:
        entity = await SqlAlchemyCanonicalEntityRepository(s).create(
            group_id=group, entity_type="Service", canonical_name="paymentapi", aliases=[]
        )
        return entity.id


async def _add_active_fact(
    sessionmaker: async_sessionmaker[AsyncSession], group: str, subject_id: UUID, obj: str
) -> UUID:
    fk = fabric.fact_key(
        scope=group, subject_entity_id=subject_id, predicate="RUNS_ON", object_scalar=obj
    )
    sk = fabric.slot_key(scope=group, subject_entity_id=subject_id, predicate="RUNS_ON")
    async with _tenant(sessionmaker, group) as s:
        fact = await SqlAlchemyFactRepository(s).upsert(
            Fact(
                id=uuid7(),
                group_id=group,
                fact_key=fk,
                slot_key=sk,
                subject_entity_id=subject_id,
                predicate="RUNS_ON",
                object_type=ObjectType.SCALAR,
                normalized_object=fabric.normalize_object(object_scalar=obj),
                object_scalar=obj,
                lifecycle_state=FactLifecycle.ACTIVE,
                authority=1.0,
                confidence=0.9,
            )
        )
        return fact.id


async def test_projection_source_returns_exact_published_episode_ids(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:proj-{uuid7().hex[:12]}"
    subject = await _seed_tenant(sessionmaker, group)
    fact_ids = {
        "eks": await _add_active_fact(sessionmaker, group, subject, "eks"),
        "valkey": await _add_active_fact(sessionmaker, group, subject, "valkey"),
    }
    now = utc_now()
    async with _tenant(sessionmaker, group) as session:
        workspace_id, project_id = (
            await session.execute(
                text(
                    "SELECT w.id, p.id FROM projects p JOIN workspaces w ON w.id=p.workspace_id "
                    "WHERE p.group_id=:group"
                ),
                {"group": group},
            )
        ).one()
        source_id = await session.scalar(
            text(
                "INSERT INTO knowledge_sources "
                "(workspace_id, project_id, kind, name, trust_tier) "
                "VALUES (:workspace_id, :project_id, 'cmdb', 'CMDB', 1) RETURNING id"
            ),
            {"workspace_id": workspace_id, "project_id": project_id},
        )
        assert source_id is not None
        artifact = ArtifactRow(
            source_id=source_id,
            external_id="runtime",
            content_hash="hash",
            s3_key="runtime/v1",
            reference_time=now,
        )
        session.add(artifact)
        await session.flush()
        version = ArtifactVersionRow(
            artifact_id=artifact.id,
            version=1,
            content_hash="hash",
            s3_key="runtime/v1",
            reference_time=now,
        )
        session.add(version)
        await session.flush()

        episodes = {
            obj: PublishedEpisodeRow(
                source_id=f"{group}:claim-{obj}",
                artifact_version_id=version.id,
                group_id=group,
                knowledge_type="fact",
                verification="human_verified",
                authority=1.0,
                confidence=0.9,
                reference_time=now,
                payload={"object": obj},
                dedup_uuid=deterministic_id(group, obj),
            )
            for obj in fact_ids
        }
        session.add_all(episodes.values())
        await session.flush()
        session.add_all(
            AssertionRow(
                group_id=group,
                fact_id=fact_id,
                polarity="supports",
                knowledge_source_id=source_id,
                artifact_id=artifact.id,
                artifact_version_id=version.id,
                extractor_confidence=0.9,
                source_authority=1.0,
                verification_state="human_verified",
                run_key=f"episode:{episodes[obj].source_id}",
                state="active",
            )
            for obj, fact_id in fact_ids.items()
        )
        expected = {obj: str(episode.id) for obj, episode in episodes.items()}
        artifact_version_id = str(version.id)

    projected = await SqlAlchemyProjectionSource(sessionmaker).active_facts(group_id=group)
    supporting = {fact.object_name: fact.supporting_episode_ids for fact in projected}

    assert supporting == {obj: (episode_id,) for obj, episode_id in expected.items()}
    assert all(artifact_version_id not in episode_ids for episode_ids in supporting.values())


async def test_rebuild_reproduces_the_active_fact_set(
    sessionmaker: async_sessionmaker[AsyncSession], graphiti_client: object
) -> None:
    group = f"p:proj-{uuid7().hex[:12]}"
    subject = await _seed_tenant(sessionmaker, group)
    await _add_active_fact(sessionmaker, group, subject, "eks")
    await _add_active_fact(sessionmaker, group, subject, "postgres")

    service = FactProjectionService(
        source=SqlAlchemyProjectionSource(sessionmaker),
        projection=GraphitiFactProjection(graphiti_client),
    )
    try:
        projected = await service.rebuild_group(group)
        assert projected == 2
        drift = await service.verify_group(group)
        assert drift.in_sync, drift

        # A new authoritative fact makes the projection stale until the next rebuild.
        await _add_active_fact(sessionmaker, group, subject, "valkey")
        drift = await service.verify_group(group)
        assert len(drift.missing_in_graph) == 1 and not drift.extra_in_graph

        assert await service.rebuild_group(group) == 3
        assert (await service.verify_group(group)).in_sync
    finally:
        await GraphitiFactProjection(graphiti_client).clear(group_id=group)


async def test_incremental_projection_removes_stale_facts_idempotently(
    sessionmaker: async_sessionmaker[AsyncSession], graphiti_client: object
) -> None:
    group = f"p:proj-{uuid7().hex[:12]}"
    subject = await _seed_tenant(sessionmaker, group)
    await _add_active_fact(sessionmaker, group, subject, "eks")
    await _add_active_fact(sessionmaker, group, subject, "ecs")
    projection = GraphitiFactProjection(graphiti_client)
    service = FactProjectionService(
        source=SqlAlchemyProjectionSource(sessionmaker), projection=projection
    )
    try:
        assert await service.rebuild_group(group) == 2
        async with _tenant(sessionmaker, group) as s:
            await s.execute(
                text(
                    "UPDATE facts SET lifecycle_state = 'retracted' "
                    "WHERE group_id = :g AND normalized_object = 'scalar:eks'"
                ),
                {"g": group},
            )

        assert await service.project_group(group) == 1
        assert (await service.verify_group(group)).in_sync
        assert await service.project_group(group) == 1
        assert (await service.verify_group(group)).in_sync
    finally:
        await projection.clear(group_id=group)
