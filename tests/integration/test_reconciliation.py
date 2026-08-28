"""Ontology-driven artifact reconciliation against the live database (Phase 2).

Covers the required scenarios: repeated facts reaffirm rather than duplicate, single-valued
supersession by authority, lower/equal-authority contradiction handling, qualifier-scoped
non-contradiction, multi-valued coexistence, assertion withdrawal on drop, final-support
retraction, and refutation. All under the real vera_app role and RLS.
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
    SqlAlchemyEvidenceRepository,
    SqlAlchemyFactRelationRepository,
    SqlAlchemyFactRepository,
    SqlAlchemyKnowledgeEventLog,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation.reconciliation import (
    ArtifactReconciliation,
    ReconciliationService,
    ResolvedProposition,
)
from vera.domain.curation.trust import authority_for_tier
from vera.domain.knowledge.fabric import Polarity
from vera.shared.ids import uuid7
from vera.shared.time import utc_now

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


def _service(session: AsyncSession) -> ReconciliationService:
    return ReconciliationService(
        facts=SqlAlchemyFactRepository(session),
        assertions=SqlAlchemyAssertionRepository(session),
        evidence=SqlAlchemyEvidenceRepository(session),
        relations=SqlAlchemyFactRelationRepository(session),
        events=SqlAlchemyKnowledgeEventLog(session),
    )


async def _bootstrap(
    sessionmaker: async_sessionmaker[AsyncSession], group: str, *, external_id: str = "art-1"
) -> tuple[UUID, UUID, UUID, UUID]:
    """Tenancy + source + one artifact with two versions + a subject entity. Returns
    (source_id, artifact_id, version1_id, version2_id).
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
            workspace_id=ws.id, project_id=proj.id, kind="confluence", name="C", trust_tier=1
        )
        await uow.commit()

    async with _tenant(sessionmaker, group) as s:
        artifact = ArtifactRow(
            source_id=source_id,
            external_id=external_id,
            content_hash="h1",
            s3_key="k1",
            reference_time=utc_now(),
        )
        s.add(artifact)
        await s.flush()
        v1 = ArtifactVersionRow(
            artifact_id=artifact.id,
            version=1,
            content_hash="h1",
            s3_key="k1",
            reference_time=utc_now(),
        )
        v2 = ArtifactVersionRow(
            artifact_id=artifact.id,
            version=2,
            content_hash="h2",
            s3_key="k2",
            reference_time=utc_now(),
        )
        s.add_all([v1, v2])
        await s.flush()
        return source_id, artifact.id, v1.id, v2.id


async def _subject(sessionmaker: async_sessionmaker[AsyncSession], group: str) -> UUID:
    async with _tenant(sessionmaker, group) as s:
        entity = await SqlAlchemyCanonicalEntityRepository(s).create(
            group_id=group, entity_type="Service", canonical_name="paymentapi", aliases=[]
        )
        return entity.id


def _runs_on(
    subject: UUID, obj: str, *, env: str = "prod", conf: float = 0.9
) -> ResolvedProposition:
    return ResolvedProposition(
        subject_entity_id=subject,
        predicate="RUNS_ON",
        object_scalar=obj,
        qualifiers={"environment": env},
        extractor_confidence=conf,
        excerpt=f"runs on {obj} in {env}",
    )


def _depends_on(subject: UUID, obj: str) -> ResolvedProposition:
    return ResolvedProposition(
        subject_entity_id=subject,
        predicate="DEPENDS_ON",
        object_scalar=obj,
        extractor_confidence=0.8,
        excerpt=f"depends on {obj}",
    )


def _req(
    group: str,
    artifact_id: UUID,
    version_id: UUID,
    source_id: UUID,
    tier: int,
    props: list[ResolvedProposition],
) -> ArtifactReconciliation:
    return ArtifactReconciliation(
        group_id=group,
        artifact_version_id=version_id,
        source_authority=authority_for_tier(tier),
        trust_tier=tier,
        propositions=props,
        knowledge_source_id=source_id,
        artifact_id=artifact_id,
    )


async def _fact_states(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> dict[str, str]:
    """normalized_object -> lifecycle_state for every fact in the group."""
    async with _tenant(sessionmaker, group) as s:
        rows = await s.execute(text("SELECT normalized_object, lifecycle_state FROM facts"))
        return dict(rows.all())  # type: ignore[arg-type]


async def _count(sessionmaker: async_sessionmaker[AsyncSession], group: str, sql: str) -> int:
    async with _tenant(sessionmaker, group) as s:
        return await s.scalar(text(sql))  # type: ignore[return-value]


async def test_repeated_proposition_reaffirms_and_does_not_duplicate(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:a-{uuid7().hex[:12]}"
    source, artifact, v1, _ = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        # The same fact stated twice in one version: one Fact, one Assertion, one Evidence.
        report = await _service(s).reconcile(
            _req(
                group, artifact, v1, source, 1, [_runs_on(subject, "eks"), _runs_on(subject, "eks")]
            )
        )
    assert report.facts_activated == 1
    assert (
        await _count(
            sessionmaker, group, "SELECT count(*) FROM facts WHERE lifecycle_state='active'"
        )
        == 1
    )
    assert (
        await _count(sessionmaker, group, "SELECT count(*) FROM assertions WHERE state='active'")
        == 1
    )
    assert await _count(sessionmaker, group, "SELECT count(*) FROM evidence") == 1


async def test_new_version_reaffirms_same_fact_and_withdraws_prior_assertion(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:b-{uuid7().hex[:12]}"
    source, artifact, v1, v2 = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(
            _req(group, artifact, v1, source, 1, [_runs_on(subject, "eks")])
        )
    async with _tenant(sessionmaker, group) as s:
        # v2 re-states the same fact plus unrelated prose (no new proposition): still one Fact.
        await _service(s).reconcile(
            _req(group, artifact, v2, source, 1, [_runs_on(subject, "eks")])
        )
    assert await _count(sessionmaker, group, "SELECT count(*) FROM facts") == 1
    # Exactly one active assertion (the v2 one); the v1 assertion is withdrawn, not deleted.
    assert (
        await _count(sessionmaker, group, "SELECT count(*) FROM assertions WHERE state='active'")
        == 1
    )
    assert (
        await _count(sessionmaker, group, "SELECT count(*) FROM assertions WHERE state='withdrawn'")
        == 1
    )
    assert (await _fact_states(sessionmaker, group))["scalar:eks"] == "active"


async def test_single_valued_higher_authority_supersedes(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:c-{uuid7().hex[:12]}"
    source_a, art_a, va, _ = await _bootstrap(sessionmaker, group, external_id="A")
    source_b, art_b, vb, _ = await _bootstrap_second_artifact(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_a, va, source_a, 2, [_runs_on(subject, "eks")]))
    async with _tenant(sessionmaker, group) as s:
        report = await _service(s).reconcile(
            _req(group, art_b, vb, source_b, 1, [_runs_on(subject, "ecs")])
        )
    assert report.facts_superseded == 1
    states = await _fact_states(sessionmaker, group)
    assert states["scalar:ecs"] == "active"
    assert states["scalar:eks"] == "superseded"
    assert (
        await _count(
            sessionmaker,
            group,
            "SELECT count(*) FROM fact_relations WHERE relation_type='SUPERSEDES'",
        )
        == 1
    )


async def test_lower_authority_does_not_overwrite(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:d-{uuid7().hex[:12]}"
    source_a, art_a, va, _ = await _bootstrap(sessionmaker, group, external_id="A")
    source_b, art_b, vb, _ = await _bootstrap_second_artifact(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_a, va, source_a, 1, [_runs_on(subject, "eks")]))
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_b, vb, source_b, 3, [_runs_on(subject, "ecs")]))
    states = await _fact_states(sessionmaker, group)
    assert states["scalar:eks"] == "active"  # Tier 1 authority untouched
    assert states["scalar:ecs"] != "active"  # the Tier 3 value did not overwrite it


async def test_equal_authority_contradiction_is_disputed(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:e-{uuid7().hex[:12]}"
    source_a, art_a, va, _ = await _bootstrap(sessionmaker, group, external_id="A")
    source_b, art_b, vb, _ = await _bootstrap_second_artifact(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_a, va, source_a, 1, [_runs_on(subject, "eks")]))
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_b, vb, source_b, 1, [_runs_on(subject, "ecs")]))
    states = await _fact_states(sessionmaker, group)
    assert states["scalar:eks"] == "disputed"
    assert states["scalar:ecs"] == "disputed"


async def test_qualifiers_prevent_false_contradiction(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:f-{uuid7().hex[:12]}"
    source, artifact, v1, _ = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(
            _req(
                group,
                artifact,
                v1,
                source,
                1,
                [_runs_on(subject, "eks", env="prod"), _runs_on(subject, "ecs", env="dev")],
            )
        )
    states = await _fact_states(sessionmaker, group)
    assert states["scalar:eks"] == "active"
    assert states["scalar:ecs"] == "active"  # different qualifier slot -> no contradiction


async def test_multi_valued_predicate_values_coexist(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:g-{uuid7().hex[:12]}"
    source, artifact, v1, _ = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(
            _req(
                group,
                artifact,
                v1,
                source,
                1,
                [_depends_on(subject, "postgres"), _depends_on(subject, "valkey")],
            )
        )
    states = await _fact_states(sessionmaker, group)
    assert states["scalar:postgres"] == "active"
    assert states["scalar:valkey"] == "active"


async def test_dropping_a_fact_retracts_it_when_support_is_gone(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:h-{uuid7().hex[:12]}"
    source, artifact, v1, v2 = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(
            _req(
                group,
                artifact,
                v1,
                source,
                1,
                [_runs_on(subject, "eks"), _depends_on(subject, "postgres")],
            )
        )
    async with _tenant(sessionmaker, group) as s:
        # v2 keeps runs_on but drops depends_on postgres.
        report = await _service(s).reconcile(
            _req(group, artifact, v2, source, 1, [_runs_on(subject, "eks")])
        )
    assert report.facts_retracted == 1
    states = await _fact_states(sessionmaker, group)
    assert states["scalar:eks"] == "active"
    assert states["scalar:postgres"] == "retracted"


async def test_withdrawing_one_assertion_keeps_fact_supported_elsewhere(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:i-{uuid7().hex[:12]}"
    source_a, art_a, va1, va2 = await _bootstrap(sessionmaker, group, external_id="A")
    source_b, art_b, vb, _ = await _bootstrap_second_artifact(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    # Two artifacts assert the same fact.
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(
            _req(group, art_a, va1, source_a, 1, [_runs_on(subject, "eks")])
        )
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_b, vb, source_b, 1, [_runs_on(subject, "eks")]))
    # Artifact A drops the fact in a new version; artifact B still supports it.
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_a, va2, source_a, 1, []))
    assert (await _fact_states(sessionmaker, group))["scalar:eks"] == "active"


async def test_refutation_disputes_the_fact_and_is_not_a_supporting_edge(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:j-{uuid7().hex[:12]}"
    source_a, art_a, va, _ = await _bootstrap(sessionmaker, group, external_id="A")
    source_b, art_b, vb, _ = await _bootstrap_second_artifact(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_a, va, source_a, 1, [_runs_on(subject, "eks")]))
    refute = ResolvedProposition(
        subject_entity_id=subject,
        predicate="RUNS_ON",
        object_scalar="eks",
        qualifiers={"environment": "prod"},
        polarity=Polarity.REFUTES,
        excerpt="no longer on eks",
    )
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_b, vb, source_b, 1, [refute]))
    assert (await _fact_states(sessionmaker, group))["scalar:eks"] == "disputed"
    assert (
        await _count(
            sessionmaker, group, "SELECT count(*) FROM assertions WHERE polarity='refutes'"
        )
        == 1
    )
    assert (
        await _count(
            sessionmaker,
            group,
            "SELECT count(*) FROM assertions WHERE polarity='supports' AND state='active'",
        )
        == 1
    )


async def _bootstrap_second_artifact(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> tuple[UUID, UUID, UUID, UUID]:
    """A second artifact + version in an existing group (its tenancy already exists)."""
    async with _tenant(sessionmaker, group) as s:
        source_id = await s.scalar(text("SELECT id FROM knowledge_sources LIMIT 1"))
        artifact = ArtifactRow(
            source_id=source_id,
            external_id="B",
            content_hash="hb",
            s3_key="kb",
            reference_time=utc_now(),
        )
        s.add(artifact)
        await s.flush()
        v = ArtifactVersionRow(
            artifact_id=artifact.id,
            version=1,
            content_hash="hb",
            s3_key="kb",
            reference_time=utc_now(),
        )
        s.add(v)
        await s.flush()
        return source_id, artifact.id, v.id, v.id  # type: ignore[return-value]
