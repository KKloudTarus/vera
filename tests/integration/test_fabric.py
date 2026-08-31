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
from sqlalchemy import exc, text
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


async def test_fact_revision_history_is_trigger_only_for_runtime_roles(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:history-{uuid7().hex[:12]}"
    _, _, subject_id = await _setup(sessionmaker, group)

    async with _tenant(sessionmaker, group) as session:
        facts = SqlAlchemyFactRepository(session)
        fact = await facts.upsert(_fact(group, subject_id))
        await facts.set_aggregates(
            group_id=group,
            fact_id=str(fact.id),
            authority=0.8,
            confidence=0.7,
        )
        revisions = (
            await session.execute(
                text(
                    "SELECT authority, confidence, system_to FROM fact_revisions "
                    "WHERE fact_id = :fact_id ORDER BY system_from"
                ),
                {"fact_id": fact.id},
            )
        ).all()

    assert len(revisions) == 2
    assert revisions[0].system_to is not None
    assert revisions[1].authority == 0.8
    assert revisions[1].confidence == 0.7
    assert revisions[1].system_to is None

    async with sessionmaker() as session:
        function_security = (
            await session.execute(
                text(
                    "SELECT function_def.prosecdef, owner.rolname, function_def.proconfig "
                    "FROM pg_catalog.pg_proc function_def "
                    "JOIN pg_catalog.pg_roles owner ON owner.oid = function_def.proowner "
                    "WHERE function_def.oid = "
                    "'public.record_fact_revision()'::regprocedure"
                )
            )
        ).one()
        runtime_grants = {
            (row.grantee, row.privilege_type)
            for row in await session.execute(
                text(
                    "SELECT grantee, privilege_type FROM information_schema.table_privileges "
                    "WHERE table_schema = 'public' AND table_name = 'fact_revisions' "
                    "AND grantee IN ('PUBLIC', 'vera_app', 'vera_trusted', 'vera_worker')"
                )
            )
        }
        runtime_execute = await session.scalar(
            text(
                "SELECT bool_or(has_function_privilege(role_name, "
                "'public.record_fact_revision()', 'EXECUTE')) "
                "FROM unnest(ARRAY['vera_app', 'vera_trusted', 'vera_worker']) "
                "AS roles(role_name)"
            )
        )

    assert function_security.prosecdef is True
    assert function_security.rolname == "vera_fact_history_writer"
    assert function_security.proconfig == ["search_path=pg_catalog"]
    assert runtime_grants == {
        ("vera_app", "SELECT"),
        ("vera_trusted", "SELECT"),
        ("vera_worker", "SELECT"),
    }
    assert runtime_execute is False

    mutations = (
        "INSERT INTO fact_revisions ("
        "group_id, fact_id, lifecycle_state, authority, confidence, valid_from, valid_to, "
        "expires_at, system_from, system_to) "
        "SELECT group_id, fact_id, lifecycle_state, authority, confidence, valid_from, valid_to, "
        "expires_at, clock_timestamp(), clock_timestamp() FROM fact_revisions "
        "WHERE fact_id = :fact_id LIMIT 1",
        "UPDATE fact_revisions SET confidence = confidence WHERE fact_id = :fact_id",
        "DELETE FROM fact_revisions WHERE fact_id = :fact_id",
        "TRUNCATE fact_revisions",
    )
    role_statements = (
        ("vera_app", text("SET LOCAL ROLE vera_app")),
        ("vera_worker", text("SET LOCAL ROLE vera_worker")),
    )
    for role, role_statement in role_statements:
        for mutation in mutations:
            async with sessionmaker() as session:
                await session.begin()
                await session.execute(role_statement)
                if role == "vera_app":
                    await session.execute(
                        text("SELECT set_config('vera.group_id', :group, true)"),
                        {"group": group},
                    )
                with pytest.raises(exc.DBAPIError, match="permission denied"):
                    params = {"fact_id": fact.id} if ":fact_id" in mutation else None
                    await session.execute(text(mutation), params)
                await session.rollback()


async def test_fact_embedding_migration_indexes_are_valid(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    expected = {
        "uq_chunk_embedding_model",
        "ix_fact_embeddings_group_active",
        "ix_fact_embeddings_ann_256",
        "ix_fact_embeddings_ann_512",
        "ix_fact_embeddings_ann_1024",
        "ix_fact_embeddings_ann_1536",
        "ix_snapshot_fact_embeddings_ann_256",
        "ix_snapshot_fact_embeddings_ann_512",
        "ix_snapshot_fact_embeddings_ann_1024",
        "ix_snapshot_fact_embeddings_ann_1536",
        "ix_chunk_embeddings_ann_256",
        "ix_chunk_embeddings_ann_512",
    }
    async with sessionmaker() as session:
        fact_embeddings_exists = await session.scalar(
            text("SELECT to_regclass('public.fact_embeddings') IS NOT NULL")
        )
        if not fact_embeddings_exists:
            pytest.skip("pgvector migration was not applied")
        indexes = (
            await session.execute(
                text(
                    "SELECT index.relname, index_def.indisvalid, index_def.indisready "
                    "FROM pg_catalog.pg_index index_def "
                    "JOIN pg_catalog.pg_class index ON index.oid = index_def.indexrelid "
                    "JOIN pg_catalog.pg_namespace namespace ON namespace.oid = index.relnamespace "
                    "WHERE namespace.nspname = 'public' AND index.relname = ANY(:names)"
                ),
                {"names": list(expected)},
            )
        ).all()

    assert {row.relname for row in indexes} == expected
    assert all(row.indisvalid and row.indisready for row in indexes)


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
