"""Combined retrieval over the live database (Phase 4): Postgres full-text passage, code, and
fact candidate sources, and the ContextAssembler that fuses and cites them.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import exc, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.knowledge import (
    ArtifactRow,
    ArtifactVersionRow,
    KnowledgeSourceRow,
)
from vera.adapters.persistence.repositories import SqlAlchemyCanonicalEntityRepository
from vera.adapters.persistence.repositories.fabric import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyChunkRepository,
    SqlAlchemyEvidenceRepository,
    SqlAlchemyFactRepository,
)
from vera.adapters.persistence.repositories.passage_index import (
    SqlAlchemyCodeIndex,
    SqlAlchemyFactCandidateSource,
    SqlAlchemyPassageIndex,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.retrieval import ContextAssembler
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import (
    Assertion,
    Chunk,
    Evidence,
    Fact,
    FactLifecycle,
    ObjectType,
    Polarity,
)
from vera.domain.ports.retrieval_index import RetrievalFilters
from vera.domain.ports.snapshot import Snapshot
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


async def _snapshot(
    sessionmaker: async_sessionmaker[AsyncSession],
    group: str,
    *,
    as_of: datetime | None = None,
) -> Snapshot:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.set_repeatable_read()
        await uow.use_tenant(group)
        snapshot = await uow.snapshots.create(
            group_id=group, policy_version="ontology-v2", as_of=as_of
        )
        await uow.commit()
        return snapshot


async def _setup(sessionmaker: async_sessionmaker[AsyncSession], group: str) -> tuple[UUID, UUID]:
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
        art = ArtifactRow(
            source_id=source_id,
            external_id="a1",
            content_hash="h",
            s3_key="k",
            reference_time=utc_now(),
        )
        s.add(art)
        await s.flush()
        ver = ArtifactVersionRow(
            artifact_id=art.id, version=1, content_hash="h", s3_key="k", reference_time=utc_now()
        )
        s.add(ver)
        await s.flush()
        entity = await SqlAlchemyCanonicalEntityRepository(s).create(
            group_id=group, entity_type="Service", canonical_name="paymentapi", aliases=[]
        )
        return ver.id, entity.id


async def _chunk(
    sessionmaker,
    group,
    version_id,
    ordinal,
    body,
    *,
    heading_path=None,
    symbol=None,
    start_line=None,
    end_line=None,
):
    ck = fabric.chunk_key(
        artifact_version_id=version_id, ordinal=ordinal, content_hash=f"c{ordinal}"
    )
    async with _tenant(sessionmaker, group) as s:
        stored = await SqlAlchemyChunkRepository(s).upsert(
            Chunk(
                id=uuid7(),
                artifact_version_id=version_id,
                group_id=group,
                chunk_key=ck,
                ordinal=ordinal,
                text=body,
                content_hash=f"c{ordinal}",
                token_count=len(body) // 4,
                heading_path=heading_path,
                symbol_name=symbol,
                start_line=start_line,
                end_line=end_line,
            )
        )
        return stored.id


async def _fact(sessionmaker, group, subject_id, obj, *, valid_from=None, valid_to=None):
    fk = fabric.fact_key(
        scope=group, subject_entity_id=subject_id, predicate="RUNS_ON", object_scalar=obj
    )
    sk = fabric.slot_key(scope=group, subject_entity_id=subject_id, predicate="RUNS_ON")
    async with _tenant(sessionmaker, group) as s:
        stored = await SqlAlchemyFactRepository(s).upsert(
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
                valid_from=valid_from,
                valid_to=valid_to,
            )
        )
        return stored.id


async def _source_artifact(
    sessionmaker: async_sessionmaker[AsyncSession], group: str, version_id: UUID
) -> tuple[UUID, UUID]:
    async with _tenant(sessionmaker, group) as session:
        row = (
            await session.execute(
                text(
                    "SELECT a.source_id, a.id AS artifact_id FROM artifact_versions av "
                    "JOIN artifacts a ON a.id = av.artifact_id WHERE av.id = :version_id"
                ),
                {"version_id": version_id},
            )
        ).one()
        return row.source_id, row.artifact_id


async def _add_source_version(
    sessionmaker: async_sessionmaker[AsyncSession],
    group: str,
    template_source_id: UUID,
    *,
    repository: str,
) -> tuple[UUID, UUID, UUID]:
    async with _tenant(sessionmaker, group) as session:
        template = await session.get(KnowledgeSourceRow, template_source_id)
        assert template is not None
        source = KnowledgeSourceRow(
            workspace_id=template.workspace_id,
            project_id=template.project_id,
            kind="confluence",
            name=f"Source {repository}",
            config={"repository": repository},
            trust_tier=1,
        )
        session.add(source)
        await session.flush()
        artifact = ArtifactRow(
            source_id=source.id,
            external_id=f"artifact-{repository}",
            content_hash=f"hash-{repository}",
            s3_key=f"key-{repository}",
            reference_time=utc_now(),
        )
        session.add(artifact)
        await session.flush()
        version = ArtifactVersionRow(
            artifact_id=artifact.id,
            version=1,
            content_hash=artifact.content_hash,
            s3_key=artifact.s3_key,
            reference_time=utc_now(),
        )
        session.add(version)
        await session.flush()
        return source.id, artifact.id, version.id


async def _assertion(
    sessionmaker: async_sessionmaker[AsyncSession],
    group: str,
    fact_id: UUID,
    *,
    source_id: UUID | None = None,
    artifact_id: UUID | None = None,
    version_id: UUID | None = None,
) -> UUID:
    async with _tenant(sessionmaker, group) as session:
        stored = await SqlAlchemyAssertionRepository(session).upsert(
            Assertion(
                id=uuid7(),
                group_id=group,
                fact_id=fact_id,
                polarity=Polarity.SUPPORTS,
                knowledge_source_id=source_id,
                artifact_id=artifact_id,
                artifact_version_id=version_id,
                extractor_confidence=0.9,
                source_authority=1.0,
                recorded_at=utc_now(),
                run_key=None if version_id is not None else str(uuid7()),
            )
        )
        return stored.id


async def _evidence(
    sessionmaker: async_sessionmaker[AsyncSession],
    group: str,
    assertion_id: UUID,
    chunk_id: UUID,
    version_id: UUID,
    *,
    body: str,
    quote: str,
) -> None:
    start = body.index(quote)
    quote_hash = hashlib.sha256(quote.encode()).hexdigest()
    async with _tenant(sessionmaker, group) as session:
        await SqlAlchemyEvidenceRepository(session).add(
            Evidence(
                id=uuid7(),
                group_id=group,
                assertion_id=assertion_id,
                chunk_id=chunk_id,
                artifact_version_id=version_id,
                content_hash=quote_hash,
                excerpt=quote,
                quote_start=start,
                quote_end=start + len(quote),
                quote_hash=quote_hash,
            )
        )


async def test_passage_and_code_full_text_search(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:r-{uuid7().hex[:12]}"
    version, _ = await _setup(sessionmaker, group)
    await _chunk(
        sessionmaker,
        group,
        version,
        0,
        "The payment service deploys on the eks cluster in production.",
    )
    await _chunk(
        sessionmaker,
        group,
        version,
        1,
        "def deploy_payment():\n    return provision_eks()",
        symbol="deploy_payment",
        start_line=1,
        end_line=2,
    )

    passage_index = SqlAlchemyPassageIndex(sessionmaker)
    passages = await passage_index.search(group_id=group, query="eks cluster", limit=10)
    assert any("eks" in p.text for p in passages)

    code = await SqlAlchemyCodeIndex(sessionmaker).search(
        group_id=group, query="deploy_payment", limit=10
    )
    assert len(code) == 1 and code[0].symbol_name == "deploy_payment"  # only the code chunk

    other_group = f"p:r-{uuid7().hex[:12]}"
    foreign_version, _ = await _setup(sessionmaker, other_group)
    await _chunk(
        sessionmaker,
        group,
        foreign_version,
        99,
        "cross tenant lexical secret",
    )
    assert (
        await passage_index.search(group_id=group, query="cross tenant lexical secret", limit=10)
        == []
    )
    snapshot = await _snapshot(sessionmaker, group)
    assert (
        await passage_index.search(
            group_id=group,
            query="cross tenant lexical secret",
            limit=10,
            snapshot_id=snapshot.id,
        )
        == []
    )


async def test_fact_candidate_source_matches_subject_and_object(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:r-{uuid7().hex[:12]}"
    _, subject = await _setup(sessionmaker, group)
    await _fact(sessionmaker, group, subject, "eks")
    source = SqlAlchemyFactCandidateSource(sessionmaker)
    assert any(
        h.object_name == "eks" for h in await source.search(group_id=group, query="eks", limit=10)
    )
    assert any(
        h.subject_name == "paymentapi"
        for h in await source.search(group_id=group, query="paymentapi", limit=10)
    )


async def test_fact_candidate_source_respects_as_of(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:r-{uuid7().hex[:12]}"
    _, subject = await _setup(sessionmaker, group)
    await _fact(sessionmaker, group, subject, "eks", valid_from=utc_now() + timedelta(days=1))
    source = SqlAlchemyFactCandidateSource(sessionmaker)
    assert (
        await source.search(group_id=group, query="eks", limit=10, as_of=utc_now()) == []
    )  # not yet valid
    assert await source.search(group_id=group, query="eks", limit=10) == []


async def test_historical_retrieval_requires_support_recorded_by_as_of(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:r-{uuid7().hex[:12]}"
    version, subject = await _setup(sessionmaker, group)
    source_id, artifact_id = await _source_artifact(sessionmaker, group, version)
    fact_id = await _fact(
        sessionmaker,
        group,
        subject,
        "eks",
        valid_from=utc_now() - timedelta(days=1),
    )
    before_support = utc_now()
    await _assertion(
        sessionmaker,
        group,
        fact_id,
        source_id=source_id,
        artifact_id=artifact_id,
        version_id=version,
    )

    source = SqlAlchemyFactCandidateSource(sessionmaker)
    assert await source.search(group_id=group, query="eks", limit=10, as_of=before_support) == []
    snapshot = await _snapshot(sessionmaker, group, as_of=before_support)
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        assert await uow.snapshots.fact_ids(group_id=group, snapshot_id=snapshot.id) == set()


async def test_fact_citation_does_not_follow_cross_tenant_evidence(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:r-{uuid7().hex[:12]}a"
    other_group = f"p:r-{uuid7().hex[:12]}b"
    local_version, subject = await _setup(sessionmaker, group)
    local_source, _ = await _source_artifact(sessionmaker, group, local_version)
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text(
                'UPDATE knowledge_sources SET config = \'{"repository":"forged"}\'::jsonb '
                "WHERE id = :source_id"
            ),
            {"source_id": local_source},
        )
    fact_id = await _fact(sessionmaker, group, subject, "eks")
    assertion_id = await _assertion(sessionmaker, group, fact_id)

    other_version, _ = await _setup(sessionmaker, other_group)
    _, foreign_artifact = await _source_artifact(sessionmaker, other_group, other_version)
    await _assertion(
        sessionmaker,
        group,
        fact_id,
        source_id=local_source,
        artifact_id=foreign_artifact,
        version_id=other_version,
    )
    foreign_body = "other-tenant evidence says paymentapi runs on eks"
    other_chunk = await _chunk(sessionmaker, other_group, other_version, 0, foreign_body)
    await _evidence(
        sessionmaker,
        other_group,
        assertion_id,
        other_chunk,
        other_version,
        body=foreign_body,
        quote=foreign_body,
    )

    hits = await SqlAlchemyFactCandidateSource(sessionmaker).search(
        group_id=group, query="eks", limit=10
    )
    assert len(hits) == 1
    assert hits[0].evidence_excerpt is None
    assert hits[0].evidence_chunk_id is None
    snapshot = await _snapshot(sessionmaker, group)
    assert (
        await SqlAlchemyFactCandidateSource(sessionmaker).search(
            group_id=group,
            query="eks",
            limit=10,
            snapshot_id=snapshot.id,
            filters=RetrievalFilters(repository="forged"),
        )
        == []
    )
    assert str(local_source) not in snapshot.source_boundaries


async def test_snapshot_fact_citation_ignores_later_evidence(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:r-{uuid7().hex[:12]}s"
    version, subject = await _setup(sessionmaker, group)
    source_id, artifact_id = await _source_artifact(sessionmaker, group, version)
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text(
                'UPDATE knowledge_sources SET config = \'{"repository":"original"}\'::jsonb '
                "WHERE id = :source_id"
            ),
            {"source_id": source_id},
        )
    fact_id = await _fact(sessionmaker, group, subject, "eks")
    body = "original evidence: paymentapi runs on eks\nlater evidence: paymentapi runs on eks"
    chunk_id = await _chunk(sessionmaker, group, version, 0, body)
    assertion_id = await _assertion(
        sessionmaker,
        group,
        fact_id,
        source_id=source_id,
        artifact_id=artifact_id,
        version_id=version,
    )
    original = "original evidence: paymentapi runs on eks"
    await _evidence(
        sessionmaker,
        group,
        assertion_id,
        chunk_id,
        version,
        body=body,
        quote=original,
    )
    snapshot = await _snapshot(sessionmaker, group)

    later = "later evidence: paymentapi runs on eks"
    await _evidence(
        sessionmaker,
        group,
        assertion_id,
        chunk_id,
        version,
        body=body,
        quote=later,
    )
    source = SqlAlchemyFactCandidateSource(sessionmaker)
    live = await source.search(
        group_id=group,
        query="eks",
        limit=10,
        restrict_fact_ids={str(fact_id)},
    )
    await _assertion(
        sessionmaker,
        group,
        fact_id,
        source_id=source_id,
        artifact_id=artifact_id,
        version_id=version,
    )
    async with _tenant(sessionmaker, group) as session:
        await SqlAlchemyAssertionRepository(session).withdraw(
            group_id=group, assertion_id=str(assertion_id)
        )
        await session.execute(
            text(
                "UPDATE facts SET authority = 0.1, confidence = 0.2, "
                "lifecycle_state = 'retracted' WHERE id = :fact_id"
            ),
            {"fact_id": fact_id},
        )
        await session.execute(
            text("UPDATE canonical_entities SET canonical_name = 'renamed' WHERE id = :id"),
            {"id": subject},
        )
        await session.execute(
            text(
                'UPDATE knowledge_sources SET config = \'{"repository":"changed"}\'::jsonb '
                "WHERE id = :source_id"
            ),
            {"source_id": source_id},
        )
    frozen = await source.search(
        group_id=group,
        query="eks",
        limit=10,
        restrict_fact_ids={str(fact_id)},
        snapshot_id=snapshot.id,
        filters=RetrievalFilters(repository="original"),
    )

    assert live[0].evidence_excerpt == later
    assert frozen[0].evidence_excerpt == original
    assert frozen[0].subject_name == "paymentapi"
    assert frozen[0].authority == 1.0
    assert frozen[0].confidence == 0.9
    assert frozen[0].lifecycle_state == "active"
    assert frozen[0].evidence_quote_hash == hashlib.sha256(original.encode()).hexdigest()


async def test_historical_snapshot_retains_withdrawn_supporting_evidence(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:r-{uuid7().hex[:12]}h"
    version, subject = await _setup(sessionmaker, group)
    source_id, artifact_id = await _source_artifact(sessionmaker, group, version)
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text(
                "UPDATE knowledge_sources SET project_id = NULL, "
                'config = \'{"repository":"history"}\'::jsonb '
                "WHERE id = :source_id"
            ),
            {"source_id": source_id},
        )
    reference_time = utc_now()
    fact_id = await _fact(
        sessionmaker,
        group,
        subject,
        "eks",
        valid_from=reference_time - timedelta(days=1),
        valid_to=reference_time + timedelta(days=1),
    )
    body = "historical evidence: paymentapi runs on eks"
    chunk_id = await _chunk(sessionmaker, group, version, 0, body)
    assertion_id = await _assertion(
        sessionmaker,
        group,
        fact_id,
        source_id=source_id,
        artifact_id=artifact_id,
        version_id=version,
    )
    await _evidence(
        sessionmaker,
        group,
        assertion_id,
        chunk_id,
        version,
        body=body,
        quote=body,
    )
    as_of = utc_now()
    active_snapshot = await _snapshot(sessionmaker, group)
    active_hits = await SqlAlchemyFactCandidateSource(sessionmaker).search(
        group_id=group,
        query="eks",
        limit=10,
        snapshot_id=active_snapshot.id,
        filters=RetrievalFilters(repository="history"),
    )
    assert active_hits[0].evidence_excerpt == body
    async with _tenant(sessionmaker, group) as session:
        await SqlAlchemyAssertionRepository(session).withdraw(
            group_id=group, assertion_id=str(assertion_id)
        )

    historical_live = await SqlAlchemyFactCandidateSource(sessionmaker).search(
        group_id=group,
        query="eks",
        limit=10,
        as_of=as_of,
        filters=RetrievalFilters(repository="history"),
    )
    assert historical_live[0].evidence_excerpt == body

    current_snapshot = await _snapshot(sessionmaker, group)
    current_hits = await SqlAlchemyFactCandidateSource(sessionmaker).search(
        group_id=group,
        query="eks",
        limit=10,
        snapshot_id=current_snapshot.id,
        filters=RetrievalFilters(repository="history"),
    )
    assert current_hits == []

    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text("UPDATE facts SET lifecycle_state = 'retracted' WHERE id = :fact_id"),
            {"fact_id": fact_id},
        )

    review_source, review_artifact, review_version = await _add_source_version(
        sessionmaker, group, source_id, repository="review-only"
    )
    review_assertion = await _assertion(
        sessionmaker,
        group,
        fact_id,
        source_id=review_source,
        artifact_id=review_artifact,
        version_id=review_version,
    )
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text("UPDATE assertions SET state = 'needs_review' WHERE id = :assertion_id"),
            {"assertion_id": review_assertion},
        )

    snapshot = await _snapshot(sessionmaker, group, as_of=as_of)
    hits = await SqlAlchemyFactCandidateSource(sessionmaker).search(
        group_id=group,
        query="eks",
        limit=10,
        snapshot_id=snapshot.id,
        filters=RetrievalFilters(repository="history"),
    )

    assert len(hits) == 1
    assert hits[0].fact_id == str(fact_id)
    assert hits[0].evidence_excerpt == body
    assert (
        await SqlAlchemyFactCandidateSource(sessionmaker).search(
            group_id=group,
            query="eks",
            limit=10,
            snapshot_id=snapshot.id,
            filters=RetrievalFilters(repository="review-only"),
        )
        == []
    )


async def test_fact_citation_respects_source_filters(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:r-{uuid7().hex[:12]}f"
    allowed_version, subject = await _setup(sessionmaker, group)
    allowed_source, allowed_artifact = await _source_artifact(sessionmaker, group, allowed_version)
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text(
                'UPDATE knowledge_sources SET config = \'{"repository":"allowed"}\'::jsonb '
                "WHERE id = :source_id"
            ),
            {"source_id": allowed_source},
        )
    denied_source, denied_artifact, denied_version = await _add_source_version(
        sessionmaker, group, allowed_source, repository="denied"
    )
    fact_id = await _fact(sessionmaker, group, subject, "eks")

    allowed_body = "allowed source confirms paymentapi runs on eks"
    allowed_chunk = await _chunk(
        sessionmaker, group, allowed_version, 0, allowed_body, heading_path="allowed.md"
    )
    allowed_assertion = await _assertion(
        sessionmaker,
        group,
        fact_id,
        source_id=allowed_source,
        artifact_id=allowed_artifact,
        version_id=allowed_version,
    )
    await _evidence(
        sessionmaker,
        group,
        allowed_assertion,
        allowed_chunk,
        allowed_version,
        body=allowed_body,
        quote=allowed_body,
    )

    denied_body = "denied source later confirms paymentapi runs on eks"
    denied_chunk = await _chunk(
        sessionmaker, group, denied_version, 0, denied_body, heading_path="denied.md"
    )
    denied_assertion = await _assertion(
        sessionmaker,
        group,
        fact_id,
        source_id=denied_source,
        artifact_id=denied_artifact,
        version_id=denied_version,
    )
    await _evidence(
        sessionmaker,
        group,
        denied_assertion,
        denied_chunk,
        denied_version,
        body=denied_body,
        quote=denied_body,
    )

    source = SqlAlchemyFactCandidateSource(sessionmaker)
    unfiltered = await source.search(group_id=group, query="eks", limit=10)
    filtered = await source.search(
        group_id=group,
        query="eks",
        limit=10,
        filters=RetrievalFilters(repository="allowed", code_path="allowed.md"),
    )

    assert unfiltered[0].evidence_excerpt == denied_body
    assert filtered[0].evidence_excerpt == allowed_body
    assert filtered[0].evidence_chunk_id == str(allowed_chunk)

    uncited_fact = await _fact(sessionmaker, group, subject, "fargate")
    await _assertion(
        sessionmaker,
        group,
        uncited_fact,
        source_id=allowed_source,
        artifact_id=allowed_artifact,
        version_id=allowed_version,
    )
    uncited = await source.search(
        group_id=group,
        query="fargate",
        limit=10,
        filters=RetrievalFilters(repository="allowed"),
    )
    assert len(uncited) == 1 and uncited[0].evidence_excerpt is None


async def test_erasure_removes_source_only_snapshot_and_denies_trusted_role(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:erase-{uuid7().hex[:12]}"
    version, subject = await _setup(sessionmaker, group)
    source_id, artifact_id = await _source_artifact(sessionmaker, group, version)
    fact_id = await _fact(sessionmaker, group, subject, "eks")
    await _assertion(
        sessionmaker,
        group,
        fact_id,
        source_id=source_id,
        artifact_id=artifact_id,
        version_id=version,
    )
    snapshot = await _snapshot(sessionmaker, group)
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        pack = await uow.context_packs.save(
            group_id=group,
            query="eks",
            snapshot_id=snapshot.id,
            token_estimate=0,
            result_count=0,
            omitted=0,
            conflicts=0,
            freshness_warnings=0,
            results=[],
            request_hash="0" * 64,
            result_references=[],
            expires_at=utc_now() + timedelta(days=1),
            assembler_version="context-assembler-v2",
            request={"query": "eks", "snapshot_id": snapshot.id},
        )
        await uow.commit()

    with pytest.raises(exc.DBAPIError, match="permission denied"):
        async with sessionmaker() as session, session.begin():
            await session.execute(text("SET LOCAL ROLE vera_trusted"))
            await session.execute(
                text("SELECT set_config('vera.group_id', :group, true)"), {"group": group}
            )
            await session.execute(
                text("SELECT erase_artifact_retrieval_inputs(:group, CAST(:versions AS uuid[]))"),
                {"group": group, "versions": [version]},
            )

    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text("SELECT erase_artifact_retrieval_inputs(:group, CAST(:versions AS uuid[]))"),
            {"group": group, "versions": [version]},
        )
    async with sessionmaker() as session:
        counts = (
            await session.execute(
                text(
                    "SELECT "
                    "(SELECT count(*) FROM knowledge_snapshots WHERE id = :snapshot), "
                    "(SELECT count(*) FROM context_packs WHERE id = :pack)"
                ),
                {"snapshot": snapshot.id, "pack": pack.id},
            )
        ).one()
    assert tuple(counts) == (0, 0)


async def test_assemble_returns_combined_cited_context(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:r-{uuid7().hex[:12]}"
    version, subject = await _setup(sessionmaker, group)
    await _fact(sessionmaker, group, subject, "eks")
    for i in range(8):
        await _chunk(
            sessionmaker,
            group,
            version,
            i,
            f"paragraph {i}: payment runs on the eks cluster today.",
        )

    assembler = ContextAssembler(
        facts=SqlAlchemyFactCandidateSource(sessionmaker),
        passages=SqlAlchemyPassageIndex(sessionmaker),
        code=SqlAlchemyCodeIndex(sessionmaker),
    )
    result = await assembler.assemble(query="where does payment run eks", group_id=group, limit=5)

    assert result.results, "combined retrieval returned nothing"
    assert all(r.citation.ref for r in result.results)  # every hit is cited
    kinds = {r.kind for r in result.results}
    # Eight same-artifact passages compete for five slots; without source-diversity they would
    # fill every slot and bury the one authoritative fact. Diversity keeps the fact in.
    assert "fact" in kinds
    assert sum(1 for r in result.results if r.kind == "passage") < 5
