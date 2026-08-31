"""Ontology-driven artifact reconciliation against the live database (Phase 2).

Covers the required scenarios: repeated facts reaffirm rather than duplicate, single-valued
supersession by authority, lower/equal-authority contradiction handling, qualifier-scoped
non-contradiction, multi-valued coexistence, assertion withdrawal on drop, final-support
retraction, and refutation. All under the real vera_app role and RLS.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import exc, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.fabric import ExtractionRunRow
from vera.adapters.persistence.models.knowledge import ArtifactRow, ArtifactVersionRow
from vera.adapters.persistence.repositories import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyCanonicalEntityRepository,
    SqlAlchemyCommunityLineageRepository,
    SqlAlchemyEvidenceRepository,
    SqlAlchemyFactExpiryRepository,
    SqlAlchemyFactRelationRepository,
    SqlAlchemyFactRepository,
    SqlAlchemyKnowledgeEventLog,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation.reconciliation import (
    ArtifactReconciliation,
    FactExpiryService,
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
        subject_entity_type="Service",
        object_entity_type="Environment",
        qualifiers={"environment": env},
        extractor_confidence=conf,
        excerpt=f"runs on {obj} in {env}",
    )


def _depends_on(subject: UUID, obj: str) -> ResolvedProposition:
    return ResolvedProposition(
        subject_entity_id=subject,
        predicate="DEPENDS_ON",
        object_scalar=obj,
        subject_entity_type="Service",
        object_entity_type="Datastore",
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
    *,
    extraction_run_id: UUID | None = None,
    valid_from: datetime | None = None,
) -> ArtifactReconciliation:
    return ArtifactReconciliation(
        group_id=group,
        artifact_version_id=version_id,
        source_authority=authority_for_tier(tier),
        trust_tier=tier,
        propositions=props,
        knowledge_source_id=source_id,
        artifact_id=artifact_id,
        extraction_run_id=extraction_run_id,
        valid_from=valid_from,
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


async def test_new_extraction_run_withdraws_dropped_claims_from_the_same_version(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:reextract-{uuid7().hex[:12]}"
    source, artifact, version, _ = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    first_run = uuid7()
    second_run = uuid7()
    async with _tenant(sessionmaker, group) as session:
        session.add_all(
            [
                ExtractionRunRow(
                    id=run_id,
                    group_id=group,
                    artifact_version_id=version,
                    model="test",
                    provider="test",
                    prompt_version="test",
                    pipeline_version={},
                    started_at=utc_now(),
                )
                for run_id in (first_run, second_run)
            ]
        )
        await session.flush()
        await _service(session).reconcile(
            _req(
                group,
                artifact,
                version,
                source,
                1,
                [_runs_on(subject, "eks")],
                extraction_run_id=first_run,
            )
        )
        report = await _service(session).reconcile(
            _req(
                group,
                artifact,
                version,
                source,
                1,
                [],
                extraction_run_id=second_run,
            )
        )

    assert report.assertions_withdrawn == 1
    assert (
        await _count(sessionmaker, group, "SELECT count(*) FROM assertions WHERE state='active'")
        == 0
    )
    assert await _fact_states(sessionmaker, group) == {"scalar:eks": "retracted"}


async def test_uri_only_evidence_survives_live_and_snapshot_retrieval(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    from vera.adapters.persistence.repositories.passage_index import SqlAlchemyFactCandidateSource

    group = f"p:uri-{uuid7().hex[:12]}"
    source, artifact, version, _ = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    citation_uri = "confluence://space/runtime#paymentapi"
    proposition = ResolvedProposition(
        subject_entity_id=subject,
        predicate="RUNS_ON",
        object_scalar="eks",
        subject_entity_type="Service",
        object_entity_type="Environment",
        qualifiers={"environment": "prod"},
        extractor_confidence=0.9,
        citation_uri=citation_uri,
    )
    async with _tenant(sessionmaker, group) as session:
        report = await _service(session).reconcile(
            _req(group, artifact, version, source, 1, [proposition])
        )
    assert report.evidence_added == 1

    candidates = SqlAlchemyFactCandidateSource(sessionmaker)
    live = await candidates.search(group_id=group, query="eks", limit=10)
    assert live[0].evidence_citation_uri == citation_uri
    assert live[0].evidence_id is not None

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.set_repeatable_read()
        await uow.use_tenant(group)
        snapshot = await uow.snapshots.create(group_id=group, policy_version="ontology-v2")
        await uow.commit()
    frozen = await candidates.search(group_id=group, query="eks", limit=10, snapshot_id=snapshot.id)
    assert frozen[0].evidence_citation_uri == citation_uri
    assert frozen[0].evidence_id == live[0].evidence_id


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


async def test_newer_same_artifact_value_replaces_without_equal_authority_dispute(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:b2-{uuid7().hex[:12]}"
    source, artifact, v1, v2 = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(
            _req(group, artifact, v1, source, 1, [_runs_on(subject, "eks")])
        )
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(
            _req(group, artifact, v2, source, 1, [_runs_on(subject, "ecs")])
        )

    states = await _fact_states(sessionmaker, group)
    assert states["scalar:ecs"] == "active"
    assert states["scalar:eks"] == "retracted"
    assert "disputed" not in states.values()


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


async def test_later_lower_authority_does_not_overwrite(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:d-{uuid7().hex[:12]}"
    source_a, art_a, va, _ = await _bootstrap(sessionmaker, group, external_id="A")
    source_b, art_b, vb, _ = await _bootstrap_second_artifact(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    earlier = utc_now() - timedelta(days=1)
    later = utc_now()
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(
            _req(
                group,
                art_a,
                va,
                source_a,
                1,
                [_runs_on(subject, "eks")],
                valid_from=earlier,
            )
        )
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(
            _req(
                group,
                art_b,
                vb,
                source_b,
                2,
                [_runs_on(subject, "ecs")],
                valid_from=later,
            )
        )
    states = await _fact_states(sessionmaker, group)
    assert states["scalar:eks"] == "active"
    assert states["scalar:ecs"] == "proposed"


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


async def test_unverified_refutation_does_not_dispute_an_active_fact(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:k-{uuid7().hex[:12]}"
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
        needs_review=True,
    )
    async with _tenant(sessionmaker, group) as s:
        await _service(s).reconcile(_req(group, art_b, vb, source_b, 1, [refute]))

    assert (await _fact_states(sessionmaker, group))["scalar:eks"] == "active"
    assert (
        await _count(
            sessionmaker, group, "SELECT count(*) FROM assertions WHERE state='needs_review'"
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


async def test_type_and_qualifier_violations_are_routed_to_review(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:govern-{uuid7().hex[:12]}"
    source, artifact, version, _ = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    invalid_type = ResolvedProposition(
        subject_entity_id=subject,
        predicate="RUNS_ON",
        object_scalar="payments-repo",
        subject_entity_type="Team",
        object_entity_type="Repository",
    )
    missing_qualifier = ResolvedProposition(
        subject_entity_id=subject,
        predicate="HAS_STATUS",
        object_scalar="degraded",
        subject_entity_type="Service",
    )

    async with _tenant(sessionmaker, group) as session:
        await _service(session).reconcile(
            _req(group, artifact, version, source, 1, [invalid_type, missing_qualifier])
        )

    async with _tenant(sessionmaker, group) as session:
        states = list(
            await session.scalars(
                text("SELECT state FROM assertions WHERE group_id = :group ORDER BY created_at"),
                {"group": group},
            )
        )
        lifecycles = list(
            await session.scalars(
                text(
                    "SELECT lifecycle_state FROM facts WHERE group_id = :group ORDER BY created_at"
                ),
                {"group": group},
            )
        )
        reasons = list(
            await session.scalars(
                text(
                    "SELECT reason FROM knowledge_events WHERE group_id = :group "
                    "AND reason IS NOT NULL ORDER BY occurred_at"
                ),
                {"group": group},
            )
        )
    assert states == ["needs_review", "needs_review"]
    assert lifecycles == ["proposed", "proposed"]
    assert any("subject type Team" in reason for reason in reasons)
    assert any("required qualifier environment" in reason for reason in reasons)


async def test_ttl_freshness_expires_active_fact_and_emits_event(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:ttl-{uuid7().hex[:12]}"
    source, artifact, version, _ = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    status = ResolvedProposition(
        subject_entity_id=subject,
        predicate="HAS_STATUS",
        object_scalar="healthy",
        subject_entity_type="Service",
        qualifiers={"environment": "prod"},
    )

    async with _tenant(sessionmaker, group) as session:
        await _service(session).reconcile(_req(group, artifact, version, source, 1, [status]))
        expires_at = await session.scalar(
            text("SELECT expires_at FROM facts WHERE group_id = :group"), {"group": group}
        )
        assert expires_at is not None
        report = await FactExpiryService(
            facts=SqlAlchemyFactExpiryRepository(session),
            events=SqlAlchemyKnowledgeEventLog(session),
        ).run(at=expires_at + timedelta(seconds=1))
        repeated = await FactExpiryService(
            facts=SqlAlchemyFactExpiryRepository(session),
            events=SqlAlchemyKnowledgeEventLog(session),
        ).run(at=expires_at + timedelta(seconds=2))

    assert report.expired == 1
    assert report.group_ids == (group,)
    assert repeated.expired == 0
    assert repeated.group_ids == ()
    async with _tenant(sessionmaker, group) as session:
        lifecycle, valid_to = (
            await session.execute(
                text("SELECT lifecycle_state, valid_to FROM facts WHERE group_id = :group"),
                {"group": group},
            )
        ).one()
        current_revision = (
            await session.execute(
                text(
                    "SELECT valid_to, system_to FROM fact_revisions "
                    "WHERE group_id = :group AND system_to IS NULL"
                ),
                {"group": group},
            )
        ).one()
        open_revisions = await session.scalar(
            text(
                "SELECT count(*) FROM fact_revisions WHERE group_id = :group AND system_to IS NULL"
            ),
            {"group": group},
        )
        event_count = await session.scalar(
            text(
                "SELECT count(*) FROM knowledge_events WHERE group_id = :group "
                "AND event_type = 'FACT_EXPIRED'"
            ),
            {"group": group},
        )
    assert lifecycle == "expired"
    assert valid_to == expires_at
    assert current_revision.valid_to == expires_at
    assert current_revision.system_to is None
    assert open_revisions == 1
    assert event_count == 1


async def test_community_fact_lineage_is_scope_safe_and_paginated(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:community-{uuid7().hex[:12]}"
    source, artifact, version, _ = await _bootstrap(sessionmaker, group)
    subject = await _subject(sessionmaker, group)
    async with _tenant(sessionmaker, group) as session:
        await _service(session).reconcile(
            _req(
                group,
                artifact,
                version,
                source,
                1,
                [_depends_on(subject, "postgres"), _depends_on(subject, "redis")],
            )
        )
        fact_ids = tuple(
            await session.scalars(
                text("SELECT id FROM facts WHERE group_id = :group ORDER BY id"),
                {"group": group},
            )
        )

    community_id = uuid7()
    run_id = uuid7()
    repository = SqlAlchemyCommunityLineageRepository(sessionmaker)
    await repository.record(
        group_id=group,
        community_id=community_id,
        derivation_run_id=run_id,
        fact_ids=fact_ids,
    )
    first = await repository.page(
        group_ids=(group,),
        community_id=community_id,
        derivation_run_id=run_id,
        cursor=None,
        limit=1,
    )
    assert len(first.items) == 1
    assert first.next_cursor is not None
    second = await repository.page(
        group_ids=(group,),
        community_id=community_id,
        derivation_run_id=run_id,
        cursor=UUID(first.next_cursor),
        limit=1,
    )
    assert len(second.items) == 1
    assert second.items[0].fact_id != first.items[0].fact_id
    hidden = await repository.page(
        group_ids=("p:other",),
        community_id=community_id,
        derivation_run_id=run_id,
        cursor=None,
        limit=10,
    )
    assert hidden.items == ()

    other_group = f"p:community-{uuid7().hex[:12]}"
    other_source, other_artifact, other_version, _ = await _bootstrap(sessionmaker, other_group)
    other_subject = await _subject(sessionmaker, other_group)
    async with _tenant(sessionmaker, other_group) as session:
        await _service(session).reconcile(
            _req(
                other_group,
                other_artifact,
                other_version,
                other_source,
                1,
                [_depends_on(other_subject, "secret")],
            )
        )
        foreign_fact_id = await session.scalar(
            text("SELECT id FROM facts WHERE group_id = :group"),
            {"group": other_group},
        )
    assert foreign_fact_id is not None
    with pytest.raises(exc.IntegrityError):
        await repository.record(
            group_id=group,
            community_id=community_id,
            derivation_run_id=uuid7(),
            fact_ids=(foreign_fact_id,),
        )
