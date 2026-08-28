"""Retrieval-quality regression gate over the committed golden set (gap 18).

Seeds datasets/retrieval/golden.json into a fresh group, runs the real ContextAssembler for
each case, and gates hit@k, nDCG@k, and the citation rate against the thresholds in the file.
A regression in ranking, dedup, or the fact/passage queries fails the build here.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
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
from vera.application.queries.retrieval_eval import citation_rate, score
from vera.application.retrieval import ContextAssembler
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Chunk, Fact, FactLifecycle, ObjectType
from vera.domain.ontology import is_edge_predicate
from vera.shared.ids import uuid7
from vera.shared.time import utc_now

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_GOLDEN = json.loads((Path(__file__).parents[2] / "datasets/retrieval/golden.json").read_text())


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


async def _seed(sessionmaker: async_sessionmaker[AsyncSession], group: str) -> None:
    seed = _GOLDEN["seed"]
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

    ids: dict[str, UUID] = {}
    async with _tenant(sessionmaker, group) as s:
        canonical = SqlAlchemyCanonicalEntityRepository(s)
        for ent in seed["entities"]:
            e = await canonical.create(
                group_id=group, entity_type=ent["type"], canonical_name=ent["name"], aliases=[]
            )
            ids[ent["name"]] = e.id

    async with _tenant(sessionmaker, group) as s:
        facts = SqlAlchemyFactRepository(s)
        for f in seed["facts"]:
            pred = f["predicate"]
            subj_id = ids[f["subject"]]
            obj_entity_id = ids[f["object"]] if is_edge_predicate(pred) else None
            obj_scalar = None if obj_entity_id else f["object"]
            fk = fabric.fact_key(
                scope=group,
                subject_entity_id=subj_id,
                predicate=pred,
                object_entity_id=obj_entity_id,
                object_scalar=obj_scalar,
            )
            await facts.upsert(
                Fact(
                    id=uuid7(),
                    group_id=group,
                    fact_key=fk,
                    slot_key=fabric.slot_key(
                        scope=group, subject_entity_id=subj_id, predicate=pred
                    ),
                    subject_entity_id=subj_id,
                    predicate=pred.upper(),
                    object_type=ObjectType.ENTITY if obj_entity_id else ObjectType.SCALAR,
                    normalized_object=fabric.normalize_object(
                        object_entity_id=obj_entity_id, object_scalar=obj_scalar
                    ),
                    object_entity_id=obj_entity_id,
                    object_scalar=obj_scalar,
                    lifecycle_state=FactLifecycle.ACTIVE,
                    authority=float(f["authority"]),
                    confidence=float(f["confidence"]),
                )
            )

    chunks = seed.get("chunks", [])
    if chunks:
        async with _tenant(sessionmaker, group) as s:
            art = ArtifactRow(
                source_id=source_id,
                external_id="golden",
                content_hash="h",
                s3_key="k",
                reference_time=utc_now(),
            )
            s.add(art)
            await s.flush()
            ver = ArtifactVersionRow(
                artifact_id=art.id,
                version=1,
                content_hash="h",
                s3_key="k",
                reference_time=utc_now(),
            )
            s.add(ver)
            await s.flush()
            repo = SqlAlchemyChunkRepository(s)
            for i, c in enumerate(chunks):
                await repo.upsert(
                    Chunk(
                        id=uuid7(),
                        artifact_version_id=ver.id,
                        group_id=group,
                        chunk_key=fabric.chunk_key(
                            artifact_version_id=ver.id, ordinal=i, content_hash=f"c{i}"
                        ),
                        ordinal=i,
                        text=c["text"],
                        content_hash=f"c{i}",
                        token_count=len(c["text"]) // 4,
                        heading_path=c.get("heading_path"),
                    )
                )


async def test_golden_retrieval_meets_quality_thresholds(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:gold-{uuid7().hex[:12]}"
    await _seed(sessionmaker, group)
    assembler = ContextAssembler(
        facts=SqlAlchemyFactCandidateSource(sessionmaker),
        passages=SqlAlchemyPassageIndex(sessionmaker),
        code=SqlAlchemyCodeIndex(sessionmaker),
    )
    k = int(_GOLDEN["k"])
    per_case: list[tuple[list[str], list[str]]] = []
    cited: list[bool] = []
    for case in _GOLDEN["cases"]:
        assembled = await assembler.assemble(query=case["query"], group_id=group, limit=k)
        per_case.append(([r.text for r in assembled.results], case["expected"]))
        cited.extend(bool(r.citation.ref) for r in assembled.results)

    report = score(per_case, k=k)
    cite = citation_rate(cited)
    thr = _GOLDEN["thresholds"]
    # Surfaced on failure so a regression is diagnosable from the run output.
    assert report.hit_rate >= thr["min_hit_rate"], f"hit_rate={report.hit_rate:.3f} report={report}"
    assert report.ndcg >= thr["min_ndcg"], f"ndcg={report.ndcg:.3f} report={report}"
    assert cite >= thr["min_citation_rate"], f"citation_rate={cite:.3f}"
