"""Curation pipeline against the live database: trust tiers, verification workflow,
conflict policy, contamination guard, and publish -> outbox. Postgres only.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.domain.knowledge.models import VerificationStatus
from vera.domain.ports.curation import ExtractedClaim
from vera.shared.errors import is_err, is_ok
from vera.shared.ids import uuid7
from vera.shared.time import utc_now

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Fixture:
    def __init__(self, sfx: str, group: str, workspace_id: UUID, project_id: UUID) -> None:
        self.sfx = sfx
        self.group = group
        self.workspace_id = workspace_id
        self.project_id = project_id


@pytest_asyncio.fixture
async def tenant(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> AsyncIterator[_Fixture]:
    sfx = uuid7().hex[:12]
    group = f"p:{sfx}"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{sfx}", name="Org", group_id=f"o:{sfx}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{sfx}", name="WS", group_id=f"w:{sfx}"
        )
        proj = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{sfx}", name="Proj", group_id=group
        )
        await uow.commit()
        yield _Fixture(sfx, group, ws.id, proj.id)


async def _source(uow: SqlAlchemyUnitOfWork, fx: _Fixture, *, kind: str, tier: int) -> UUID:
    return await uow.sources.create(
        workspace_id=fx.workspace_id,
        project_id=fx.project_id,
        kind=kind,
        name=f"{kind}-source",
        trust_tier=tier,
    )


def _triple_meta(subject: str, predicate: str, obj: str) -> dict:
    return {"triples": [{"subject": subject, "predicate": predicate, "object": obj}]}


async def _counts(sm: async_sessionmaker[AsyncSession], group: str) -> tuple[int, int]:
    async with sm() as s:
        episodes = await s.scalar(
            text("SELECT count(*) FROM published_episodes WHERE group_id = :g"), {"g": group}
        )
        jobs = await s.scalar(
            text("SELECT count(*) FROM ingestion_jobs WHERE group_id = :g"), {"g": group}
        )
    return episodes, jobs


async def test_tier1_auto_publishes_and_enqueues(
    sessionmaker: async_sessionmaker[AsyncSession], tenant: _Fixture
) -> None:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(tenant.group)
        source_id = await _source(uow, tenant, kind="cmdb", tier=1)
        service = CurationService(uow, StructuredClaimExtractor())
        result = await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=tenant.group,
                external_id="rec-1",
                body="",
                knowledge_type="fact_triple",
                metadata=_triple_meta("paymentapi", "RUNSON", "prodeksmy"),
            )
        )
        await uow.commit()

    assert is_ok(result)
    assert result.value.action == "auto_publish"
    assert result.value.published == 1
    episodes, jobs = await _counts(sessionmaker, tenant.group)
    assert episodes == 1
    assert jobs == 1


async def test_tier4_only_proposes(
    sessionmaker: async_sessionmaker[AsyncSession], tenant: _Fixture
) -> None:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(tenant.group)
        source_id = await _source(uow, tenant, kind="slack", tier=4)
        service = CurationService(uow, StructuredClaimExtractor())
        result = await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=tenant.group,
                external_id="msg-1",
                body="",
                knowledge_type="fact_triple",
                metadata=_triple_meta("paymentapi", "MAYBEON", "somewhere"),
            )
        )
        await uow.commit()

    assert is_ok(result)
    assert result.value.action == "proposal_only"
    assert result.value.published == 0
    episodes, jobs = await _counts(sessionmaker, tenant.group)
    assert episodes == 0
    assert jobs == 0


async def test_tier3_requires_review_then_publishes(
    sessionmaker: async_sessionmaker[AsyncSession], tenant: _Fixture
) -> None:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(tenant.group)
        source_id = await _source(uow, tenant, kind="confluence", tier=3)
        service = CurationService(uow, StructuredClaimExtractor())
        result = await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=tenant.group,
                external_id="page-1",
                body="",
                knowledge_type="fact_triple",
                metadata=_triple_meta("paymentapi", "DOCUMENTEDIN", "wiki"),
            )
        )
        assert is_ok(result)
        assert result.value.action == "review_required"
        assert result.value.published == 0
        claim_id = UUID(result.value.claim_ids[0])

        # A reviewer approves; the claim publishes and enqueues.
        approved = await service.review_claim(
            claim_id=claim_id, reviewer_principal_id=None, approve=True, authority="human"
        )
        await uow.commit()

    assert is_ok(approved)
    assert approved.value.status == "published"
    episodes, jobs = await _counts(sessionmaker, tenant.group)
    assert episodes == 1
    assert jobs == 1


async def test_trusted_conflict_supersedes_prior_fact(
    sessionmaker: async_sessionmaker[AsyncSession], tenant: _Fixture
) -> None:
    # RUNS_ON is functional: a newer value from a trusted (tier-1) source supersedes.
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(tenant.group)
        source_id = await _source(uow, tenant, kind="cmdb", tier=1)
        service = CurationService(uow, StructuredClaimExtractor())
        first = await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=tenant.group,
                external_id="rec-a",
                body="",
                knowledge_type="fact_triple",
                metadata=_triple_meta("paymentapi", "RUNS_ON", "prodeksmy"),
            )
        )
        second = await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=tenant.group,
                external_id="rec-b",
                body="",
                knowledge_type="fact_triple",
                metadata=_triple_meta("paymentapi", "RUNS_ON", "prodekssg"),
            )
        )
        await uow.commit()

    assert is_ok(first) and first.value.published == 1
    assert is_ok(second) and second.value.published == 1  # supersede, not flag
    async with sessionmaker() as s:
        total = await s.scalar(
            text("SELECT count(*) FROM published_episodes WHERE group_id = :g"),
            {"g": tenant.group},
        )
        current = await s.scalar(
            text(
                "SELECT count(*) FROM published_episodes WHERE group_id = :g AND invalid_at IS NULL"
            ),
            {"g": tenant.group},
        )
        superseded = await s.scalar(
            text(
                "SELECT count(*) FROM published_episodes "
                "WHERE group_id = :g AND superseded_by_source IS NOT NULL"
            ),
            {"g": tenant.group},
        )
    assert total == 2  # history is kept
    assert current == 1  # only the newest is current
    assert superseded == 1  # the prior one is marked superseded


async def test_multivalued_predicate_keeps_both(
    sessionmaker: async_sessionmaker[AsyncSession], tenant: _Fixture
) -> None:
    # DEPENDS_ON is multi-valued: a different object is not a contradiction.
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(tenant.group)
        source_id = await _source(uow, tenant, kind="cmdb", tier=1)
        service = CurationService(uow, StructuredClaimExtractor())
        for ext, obj in (("dep-a", "postgres"), ("dep-b", "valkey")):
            await service.ingest_artifact(
                IngestArtifact(
                    source_id=source_id,
                    group_id=tenant.group,
                    external_id=ext,
                    body="",
                    knowledge_type="fact_triple",
                    metadata=_triple_meta("paymentapi", "DEPENDS_ON", obj),
                )
            )
        await uow.commit()

    async with sessionmaker() as s:
        current = await s.scalar(
            text(
                "SELECT count(*) FROM published_episodes WHERE group_id = :g AND invalid_at IS NULL"
            ),
            {"g": tenant.group},
        )
    assert current == 2  # both dependencies coexist


async def test_publish_requires_verified(
    sessionmaker: async_sessionmaker[AsyncSession], tenant: _Fixture
) -> None:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(tenant.group)
        source_id = await _source(uow, tenant, kind="slack", tier=4)
        service = CurationService(uow, StructuredClaimExtractor())
        result = await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=tenant.group,
                external_id="msg-2",
                body="",
                knowledge_type="fact_triple",
                metadata=_triple_meta("x", "REL", "y"),
            )
        )
        assert is_ok(result)
        claim_id = UUID(result.value.claim_ids[0])
        # The claim is unverified in a shared scope; publishing must be rejected.
        rejected = await service.publish_claim(claim_id)
        await uow.rollback()

    assert is_err(rejected)
    assert rejected.error.code == "policy_rejected"


async def test_optimistic_transition_rejects_stale_version(
    sessionmaker: async_sessionmaker[AsyncSession], tenant: _Fixture
) -> None:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(tenant.group)
        source_id = await _source(uow, tenant, kind="slack", tier=4)
        ref = await uow.artifacts.create_with_version(
            source_id=source_id,
            external_id="art-1",
            title=None,
            content_hash="sha256:x",
            s3_key="k",
            reference_time=utc_now(),
        )
        claim = await uow.claims.create(
            artifact_version_id=ref.version_id,
            group_id=tenant.group,
            claim=ExtractedClaim(statement="s p o", subject="s", predicate="p", object="o"),
        )
        good = await uow.claims.transition(
            claim_id=claim.id,
            expected_version=claim.version_id,
            to_status=VerificationStatus.VERIFIED,
        )
        stale = await uow.claims.transition(
            claim_id=claim.id,
            expected_version=claim.version_id,
            to_status=VerificationStatus.DISPUTED,
        )
        await uow.rollback()

    assert good is True
    assert stale is False  # version already advanced
