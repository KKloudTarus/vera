"""Combined retrieval over the live database (Phase 4): Postgres full-text passage, code, and
fact candidate sources, and the ContextAssembler that fuses and cites them.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.knowledge import ArtifactRow, ArtifactVersionRow
from vera.adapters.persistence.repositories import SqlAlchemyCanonicalEntityRepository
from vera.adapters.persistence.repositories.fabric import (
    SqlAlchemyChunkRepository,
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
from vera.domain.knowledge.fabric import Chunk, Fact, FactLifecycle, ObjectType
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
    sessionmaker, group, version_id, ordinal, body, *, symbol=None, start_line=None, end_line=None
):
    ck = fabric.chunk_key(
        artifact_version_id=version_id, ordinal=ordinal, content_hash=f"c{ordinal}"
    )
    async with _tenant(sessionmaker, group) as s:
        await SqlAlchemyChunkRepository(s).upsert(
            Chunk(
                id=uuid7(),
                artifact_version_id=version_id,
                group_id=group,
                chunk_key=ck,
                ordinal=ordinal,
                text=body,
                content_hash=f"c{ordinal}",
                token_count=len(body) // 4,
                symbol_name=symbol,
                start_line=start_line,
                end_line=end_line,
            )
        )


async def _fact(sessionmaker, group, subject_id, obj, *, valid_from=None):
    fk = fabric.fact_key(
        scope=group, subject_entity_id=subject_id, predicate="RUNS_ON", object_scalar=obj
    )
    sk = fabric.slot_key(scope=group, subject_entity_id=subject_id, predicate="RUNS_ON")
    async with _tenant(sessionmaker, group) as s:
        await SqlAlchemyFactRepository(s).upsert(
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

    passages = await SqlAlchemyPassageIndex(sessionmaker).search(
        group_id=group, query="eks cluster", limit=10
    )
    assert any("eks" in p.text for p in passages)

    code = await SqlAlchemyCodeIndex(sessionmaker).search(
        group_id=group, query="deploy_payment", limit=10
    )
    assert len(code) == 1 and code[0].symbol_name == "deploy_payment"  # only the code chunk


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
    assert (
        len(await source.search(group_id=group, query="eks", limit=10)) == 1
    )  # visible as of now (default)


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
