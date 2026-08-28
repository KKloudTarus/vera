"""Knowledge snapshots and context packs over the live database (Phase 5).

Covers snapshot capture and reproducibility (scenario 17: a snapshot still answers with its
frozen facts after newer knowledge supersedes them), context-pack persistence and retrieval,
and the SNAPSHOT_CREATED / CONTEXT_PACK_CREATED ledger entries.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories import SqlAlchemyCanonicalEntityRepository
from vera.adapters.persistence.repositories.fabric import SqlAlchemyFactRepository
from vera.adapters.persistence.repositories.passage_index import (
    SqlAlchemyCodeIndex,
    SqlAlchemyFactCandidateSource,
    SqlAlchemyPassageIndex,
)
from vera.adapters.persistence.repositories.snapshot import (
    SqlAlchemyContextPackRepository,
    SqlAlchemySnapshotRepository,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.retrieval import ContextAssembler
from vera.application.snapshot import ContextPackService, SnapshotService
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Fact, FactLifecycle, ObjectType
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


async def _setup(sessionmaker: async_sessionmaker[AsyncSession], group: str) -> UUID:
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


async def _add_fact(
    sessionmaker,
    group,
    subject_id,
    obj,
    *,
    valid_from: datetime | None = None,
    valid_to: datetime | None = None,
) -> tuple[UUID, str]:
    fk = fabric.fact_key(
        scope=group, subject_entity_id=subject_id, predicate="RUNS_ON", object_scalar=obj
    )
    sk = fabric.slot_key(scope=group, subject_entity_id=subject_id, predicate="RUNS_ON")
    fact_id = uuid7()
    async with _tenant(sessionmaker, group) as s:
        stored = await SqlAlchemyFactRepository(s).upsert(
            Fact(
                id=fact_id,
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
    return stored.id, fk


async def _supersede(sessionmaker, group, fact_id) -> None:
    async with _tenant(sessionmaker, group) as s:
        await SqlAlchemyFactRepository(s).set_lifecycle(
            group_id=group, fact_id=str(fact_id), state=FactLifecycle.SUPERSEDED
        )


def _assembler(sessionmaker: async_sessionmaker[AsyncSession]) -> ContextAssembler:
    return ContextAssembler(
        facts=SqlAlchemyFactCandidateSource(sessionmaker),
        passages=SqlAlchemyPassageIndex(sessionmaker),
        code=SqlAlchemyCodeIndex(sessionmaker),
    )


async def _count(sessionmaker, group, sql) -> int:
    async with _tenant(sessionmaker, group) as s:
        return await s.scalar(text(sql))  # type: ignore[return-value]


async def test_snapshot_captures_active_facts_and_emits_event(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:s-{uuid7().hex[:12]}"
    subject = await _setup(sessionmaker, group)
    await _add_fact(sessionmaker, group, subject, "eks")
    await _add_fact(sessionmaker, group, subject, "postgres")

    snapshots = SqlAlchemySnapshotRepository(sessionmaker)
    checkpoint = uuid7()
    async with sessionmaker() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO ingestion_jobs "
                "(id, group_id, source_id, dedup_uuid, payload, status) "
                "VALUES (:id, :g, 'projection', :dedup, CAST(:payload AS jsonb), 'done')"
            ),
            {
                "id": checkpoint,
                "g": group,
                "dedup": uuid7(),
                "payload": json.dumps({"job_kind": "project_facts"}),
            },
        )
    embedding_version = {
        "provider": "test",
        "model": "deterministic",
        "model_version": "2",
        "dimension": 1024,
    }
    snap = await SnapshotService(snapshots=snapshots).create(
        group_id=group,
        embedding_version=embedding_version,
        retrieval_index_version="hybrid-rrf-v1",
    )
    assert snap.fact_count == 2
    assert snap.as_of_valid_time == snap.frozen_at_system_time
    assert snap.embedding_version == embedding_version
    assert snap.retrieval_index_version == "hybrid-rrf-v1"
    assert snap.graph_projection_checkpoint == str(checkpoint)
    fetched = await SnapshotService(snapshots=snapshots).get(group_id=group, snapshot_id=snap.id)
    assert fetched == snap
    assert (
        await _count(
            sessionmaker,
            group,
            "SELECT count(*) FROM knowledge_events WHERE event_type='SNAPSHOT_CREATED'",
        )
        == 1
    )


async def test_snapshot_excludes_fact_not_valid_at_requested_time(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:sv-{uuid7().hex[:12]}"
    subject = await _setup(sessionmaker, group)
    as_of = utc_now()
    valid_id, _ = await _add_fact(
        sessionmaker,
        group,
        subject,
        "eks",
        valid_from=as_of - timedelta(days=1),
    )
    future_id, _ = await _add_fact(
        sessionmaker,
        group,
        subject,
        "ecs",
        valid_from=as_of + timedelta(days=1),
    )

    snapshots = SqlAlchemySnapshotRepository(sessionmaker)
    snap = await SnapshotService(snapshots=snapshots).create(group_id=group, as_of=as_of)
    fact_ids = await snapshots.fact_ids(group_id=group, snapshot_id=snap.id)

    assert snap.as_of_valid_time == as_of
    assert str(valid_id) in fact_ids
    assert str(future_id) not in fact_ids


async def test_snapshot_query_is_reproducible_after_supersession(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:s-{uuid7().hex[:12]}"
    subject = await _setup(sessionmaker, group)
    eks_id, eks_key = await _add_fact(sessionmaker, group, subject, "eks")

    snapshots = SqlAlchemySnapshotRepository(sessionmaker)
    packs = SqlAlchemyContextPackRepository(sessionmaker)
    snap = await SnapshotService(snapshots=snapshots).create(group_id=group)

    # Newer knowledge supersedes the snapshot's fact.
    await _supersede(sessionmaker, group, eks_id)
    await _add_fact(sessionmaker, group, subject, "ecs")

    service = ContextPackService(
        assembler=_assembler(sessionmaker), snapshots=snapshots, packs=packs
    )

    # As of the snapshot, the frozen fact is still answered even though it is now superseded.
    pinned = await service.create(group_id=group, query="eks", snapshot_id=snap.id)
    assert any(r["kind"] == "fact" and r["ref"] == eks_key for r in pinned.results)

    # Against the latest state, the superseded fact is gone.
    latest = await service.create(group_id=group, query="eks")
    assert not any(r["kind"] == "fact" and r["ref"] == eks_key for r in latest.results)


async def test_context_pack_is_persisted_and_retrievable(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:s-{uuid7().hex[:12]}"
    subject = await _setup(sessionmaker, group)
    await _add_fact(sessionmaker, group, subject, "eks")

    snapshots = SqlAlchemySnapshotRepository(sessionmaker)
    packs = SqlAlchemyContextPackRepository(sessionmaker)
    service = ContextPackService(
        assembler=_assembler(sessionmaker), snapshots=snapshots, packs=packs
    )

    created = await service.create(group_id=group, query="eks", hints={"task": "deploy"})
    assert created.result_count >= 1
    assert created.token_estimate > 0

    fetched = await service.get(group_id=group, pack_id=created.id)
    assert fetched is not None
    assert fetched.query == "eks"
    assert len(fetched.results) == created.result_count
    assert all(r["citation"]["ref"] for r in fetched.results)  # citations survive the round trip
    assert (
        await _count(
            sessionmaker,
            group,
            "SELECT count(*) FROM knowledge_events WHERE event_type='CONTEXT_PACK_CREATED'",
        )
        == 1
    )


async def test_context_pack_over_snapshot_excludes_later_ingested_passages(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    """gap 12: a pack against a snapshot must reproduce the passages that existed when the
    snapshot was taken. A chunk ingested after the snapshot must not leak into the pack.
    """
    from vera.adapters.persistence.models.knowledge import ArtifactRow, ArtifactVersionRow
    from vera.adapters.persistence.repositories.fabric import SqlAlchemyChunkRepository
    from vera.domain.knowledge.fabric import Chunk

    group = f"p:sp-{uuid7().hex[:12]}"
    await _setup(sessionmaker, group)
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o2-{group}", name="O", group_id=f"o2:{group}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w2-{group}", name="W", group_id=f"w2:{group}"
        )
        source_id = await uow.sources.create(
            workspace_id=ws.id, project_id=None, kind="confluence", name="C", trust_tier=1
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
        version_id = ver.id

    async def _chunk(ordinal: int, body: str) -> None:
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
                )
            )

    await _chunk(1, "deployment runbook alpha describes the rollout")
    snapshot = await SqlAlchemySnapshotRepository(sessionmaker).create(
        group_id=group, policy_version="ontology-v1"
    )
    await _chunk(2, "deployment runbook bravo describes a later rollout")

    packs = ContextPackService(
        assembler=_assembler(sessionmaker),
        snapshots=SqlAlchemySnapshotRepository(sessionmaker),
        packs=SqlAlchemyContextPackRepository(sessionmaker),
    )
    pack = await packs.create(
        group_id=group, query="deployment runbook rollout", snapshot_id=snapshot.id
    )
    texts = " ".join(r["text"] for r in pack.results)
    assert "alpha" in texts  # existed at snapshot time
    assert "bravo" not in texts  # ingested after the snapshot: excluded

    # Without a snapshot the live pack sees both, proving the cutoff (not a seeding bug) filters it.
    live = await packs.create(group_id=group, query="deployment runbook rollout")
    live_texts = " ".join(r["text"] for r in live.results)
    assert "alpha" in live_texts and "bravo" in live_texts
