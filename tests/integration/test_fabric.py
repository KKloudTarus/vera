"""Knowledge Fabric repositories against the live database (Phase 1).

Exercises the new tables under the real vera_app role and RLS: idempotent chunk/fact/evidence
upsert, assertion reaffirm (not duplicate), multi-source support for one fact, fact relations,
the append-only event log, and tenant isolation. FK setup reuses the existing tenancy/source
path; artifact versions and canonical entities are created under tenant scope.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.knowledge import ArtifactRow, ArtifactVersionRow
from vera.adapters.persistence.repositories import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyCanonicalEntityRepository,
    SqlAlchemyChunkRepository,
    SqlAlchemyEvidenceRepository,
    SqlAlchemyFactRelationRepository,
    SqlAlchemyFactRepository,
    SqlAlchemyKnowledgeEventLog,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import (
    Assertion,
    Chunk,
    Evidence,
    Fact,
    FactLifecycle,
    FactRelation,
    KnowledgeEvent,
    KnowledgeEventType,
    ObjectType,
    Polarity,
    RelationType,
)
from vera.shared.ids import uuid7
from vera.shared.time import utc_now

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    """A session scoped to a tenant the way SqlAlchemyUnitOfWork.use_tenant does: switch to
    the non-superuser app role and set the RLS group, so RLS is actually enforced.
    """
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


async def _setup(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> tuple[UUID, UUID, UUID]:
    """Create tenancy, a source, one artifact version, and one canonical entity. Returns
    (knowledge_source_id, artifact_version_id, subject_entity_id).
    """
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
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
            workspace_id=ws.id, project_id=proj.id, kind="cmdb", name="C", trust_tier=1
        )
        await uow.commit()

    async with _tenant(sessionmaker, group) as s:
        artifact = ArtifactRow(
            source_id=source_id,
            external_id="a1",
            content_hash="h",
            s3_key="k",
            reference_time=utc_now(),
        )
        s.add(artifact)
        await s.flush()
        version = ArtifactVersionRow(
            artifact_id=artifact.id,
            version=1,
            content_hash="h",
            s3_key="k",
            reference_time=utc_now(),
        )
        s.add(version)
        await s.flush()
        entity = await SqlAlchemyCanonicalEntityRepository(s).create(
            group_id=group,
            entity_type="Service",
            canonical_name="paymentapi",
            aliases=[],
            embedding=None,
        )
        return source_id, version.id, entity.id


def _fact(group: str, subject_id: UUID, obj: str = "prod-eks") -> Fact:
    fk = fabric.fact_key(
        scope=group,
        subject_entity_id=subject_id,
        predicate="RUNS_ON",
        object_scalar=obj,
        qualifiers={"environment": "prod"},
    )
    sk = fabric.slot_key(
        scope=group,
        subject_entity_id=subject_id,
        predicate="RUNS_ON",
        qualifiers={"environment": "prod"},
    )
    return Fact(
        id=uuid7(),
        group_id=group,
        fact_key=fk,
        slot_key=sk,
        subject_entity_id=subject_id,
        predicate="RUNS_ON",
        object_type=ObjectType.SCALAR,
        normalized_object=fabric.normalize_object(object_scalar=obj),
        object_scalar=obj,
        qualifiers={"environment": "prod"},
        lifecycle_state=FactLifecycle.ACTIVE,
        authority=1.0,
        confidence=0.9,
    )


async def test_fact_and_chunk_and_evidence_upsert_are_idempotent(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    _, version_id, subject_id = await _setup(sessionmaker, group)

    async with _tenant(sessionmaker, group) as s:
        facts = SqlAlchemyFactRepository(s)
        f1 = await facts.upsert(_fact(group, subject_id))
        f2 = await facts.upsert(_fact(group, subject_id))  # same fact_key
        assert f1.id == f2.id  # deduplicated, not a second Fact
        slot = await facts.active_by_slot_key(group_id=group, slot_key=f1.slot_key)
        assert len(slot) == 1

        chunks = SqlAlchemyChunkRepository(s)
        ck = fabric.chunk_key(artifact_version_id=version_id, ordinal=0, content_hash="c0")
        c1 = await chunks.upsert(
            Chunk(
                id=uuid7(),
                artifact_version_id=version_id,
                group_id=group,
                chunk_key=ck,
                ordinal=0,
                text="paymentapi runs on prod-eks",
                content_hash="c0",
                token_count=6,
            )
        )
        c2 = await chunks.upsert(
            Chunk(
                id=uuid7(),
                artifact_version_id=version_id,
                group_id=group,
                chunk_key=ck,
                ordinal=0,
                text="paymentapi runs on prod-eks",
                content_hash="c0",
                token_count=6,
            )
        )
        assert c1.id == c2.id  # deterministic chunk_key -> re-chunk is a no-op

        asserts = SqlAlchemyAssertionRepository(s)
        a1 = await asserts.upsert(
            Assertion(
                id=uuid7(),
                group_id=group,
                fact_id=f1.id,
                polarity=Polarity.SUPPORTS,
                artifact_version_id=version_id,
                extractor_confidence=0.8,
                source_authority=1.0,
            )
        )
        a2 = await asserts.upsert(
            Assertion(
                id=uuid7(),
                group_id=group,
                fact_id=f1.id,
                polarity=Polarity.SUPPORTS,
                artifact_version_id=version_id,
                extractor_confidence=0.9,
                source_authority=1.0,
            )
        )
        assert a1.id == a2.id  # reaffirmed in place, not duplicated
        assert a2.recorded_at == a1.recorded_at
        assert len(await asserts.active_for_fact(group_id=group, fact_id=str(f1.id))) == 1

        evidence = SqlAlchemyEvidenceRepository(s)
        e1 = await evidence.add(
            Evidence(
                id=uuid7(),
                group_id=group,
                assertion_id=a1.id,
                content_hash="ev0",
                chunk_id=c1.id,
                excerpt="runs on prod-eks",
            )
        )
        e2 = await evidence.add(
            Evidence(
                id=uuid7(),
                group_id=group,
                assertion_id=a1.id,
                content_hash="ev0",
                chunk_id=c1.id,
                excerpt="runs on prod-eks",
            )
        )
        assert e1.id == e2.id  # same content hash -> one evidence row
        assert len(await evidence.for_assertion(group_id=group, assertion_id=str(a1.id))) == 1

        await asserts.withdraw(group_id=group, assertion_id=str(a1.id))
        membership_before = (
            await s.execute(
                text("SELECT recorded_at, withdrawn_at FROM assertions WHERE id = :id"),
                {"id": a1.id},
            )
        ).one()
        replayed = await asserts.upsert(
            Assertion(
                id=uuid7(),
                group_id=group,
                fact_id=f1.id,
                polarity=Polarity.SUPPORTS,
                artifact_version_id=version_id,
                extractor_confidence=0.9,
                source_authority=1.0,
            )
        )
        membership_after = (
            await s.execute(
                text("SELECT recorded_at, withdrawn_at FROM assertions WHERE id = :id"),
                {"id": a1.id},
            )
        ).one()
        assert replayed.state.value == "withdrawn"
        assert membership_after == membership_before


async def test_one_fact_supported_by_multiple_sources(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    _, version_a, subject_id = await _setup(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        # A second artifact version, standing in for an independent source revision.
        art = ArtifactRow(
            source_id=(await _first_source(s)),
            external_id="a2",
            content_hash="h2",
            s3_key="k2",
            reference_time=utc_now(),
        )
        s.add(art)
        await s.flush()
        version_b = ArtifactVersionRow(
            artifact_id=art.id, version=1, content_hash="h2", s3_key="k2", reference_time=utc_now()
        )
        s.add(version_b)
        await s.flush()

        facts = SqlAlchemyFactRepository(s)
        f = await facts.upsert(_fact(group, subject_id))
        asserts = SqlAlchemyAssertionRepository(s)
        await asserts.upsert(
            Assertion(
                id=uuid7(),
                group_id=group,
                fact_id=f.id,
                polarity=Polarity.SUPPORTS,
                artifact_version_id=version_a,
            )
        )
        await asserts.upsert(
            Assertion(
                id=uuid7(),
                group_id=group,
                fact_id=f.id,
                polarity=Polarity.SUPPORTS,
                artifact_version_id=version_b.id,
            )
        )
        active = await asserts.active_for_fact(group_id=group, fact_id=str(f.id))
        assert len(active) == 2  # one Fact, two independent supporting Assertions


async def _first_source(session: AsyncSession) -> UUID:
    return await session.scalar(text("SELECT id FROM knowledge_sources LIMIT 1"))  # type: ignore[return-value]


async def test_fact_relation_and_event_log(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    _, _, subject_id = await _setup(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        facts = SqlAlchemyFactRepository(s)
        old = await facts.upsert(_fact(group, subject_id, obj="eks"))
        new = await facts.upsert(_fact(group, subject_id, obj="ecs"))
        relations = SqlAlchemyFactRelationRepository(s)
        rel = await relations.add(
            FactRelation(
                id=uuid7(),
                group_id=group,
                from_fact_id=new.id,
                to_fact_id=old.id,
                relation_type=RelationType.SUPERSEDES,
            )
        )
        assert rel.relation_type is RelationType.SUPERSEDES
        assert len(await relations.from_fact(group_id=group, fact_id=str(new.id))) == 1

        log = SqlAlchemyKnowledgeEventLog(s)
        await log.append(
            KnowledgeEvent(
                id=uuid7(),
                group_id=group,
                event_type=KnowledgeEventType.FACT_SUPERSEDED,
                occurred_at=utc_now(),
                fact_id=old.id,
                reason="replaced by ecs",
            )
        )
        recent = await log.recent(group_id=group, limit=10)
        assert any(e.event_type is KnowledgeEventType.FACT_SUPERSEDED for e in recent)


async def test_rls_isolates_facts_between_tenants(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    # Distinct prefixes: uuid7().hex[:12] is only the millisecond timestamp, so two calls in
    # the same millisecond would otherwise collide into the same group id.
    sfx = uuid7().hex[:12]
    group_a = f"p:a-{sfx}"
    group_b = f"p:b-{sfx}"
    _, _, subject_a = await _setup(sessionmaker, group_a)

    async with _tenant(sessionmaker, group_a) as s:
        await SqlAlchemyFactRepository(s).upsert(_fact(group_a, subject_a))

    # Under group B's scope, RLS hides group A's facts even with no group filter in the query.
    async with _tenant(sessionmaker, group_b) as s:
        assert await s.scalar(text("SELECT count(*) FROM facts")) == 0
    async with _tenant(sessionmaker, group_a) as s:
        assert await s.scalar(text("SELECT count(*) FROM facts")) == 1
