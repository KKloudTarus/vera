"""Knowledge snapshots and context packs over the live database (Phase 5).

Covers snapshot capture and reproducibility (scenario 17: a snapshot still answers with its
frozen facts after newer knowledge supersedes them), context-pack persistence and retrieval,
and the SNAPSHOT_CREATED / CONTEXT_PACK_CREATED ledger entries.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import exc, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from vera.adapters.persistence.base import create_sessionmaker
from vera.adapters.persistence.repositories import SqlAlchemyCanonicalEntityRepository
from vera.adapters.persistence.repositories.fabric import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyFactRepository,
)
from vera.adapters.persistence.repositories.passage_index import (
    SqlAlchemyCodeIndex,
    SqlAlchemyFactCandidateSource,
    SqlAlchemyPassageIndex,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.retrieval import ContextAssembler
from vera.application.snapshot import (
    ContextPackExpiredError,
    ContextPackService,
    SnapshotNotFoundError,
    SnapshotNotReproducibleError,
    SnapshotService,
)
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Assertion, Fact, FactLifecycle, ObjectType, Polarity
from vera.domain.ports.retrieval_index import RetrievalFilters
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


def _snapshot_service(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> SnapshotService:
    return SnapshotService(uow_factory=lambda: SqlAlchemyUnitOfWork(sessionmaker))


def _context_service(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    read_sessionmaker: async_sessionmaker[AsyncSession] | None = None,
) -> ContextPackService:
    return ContextPackService(
        assembler=_assembler(read_sessionmaker or sessionmaker),
        uow_factory=lambda: SqlAlchemyUnitOfWork(sessionmaker),
    )


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
    disputed_id, _ = await _add_fact(sessionmaker, group, subject, "ecs")
    async with _tenant(sessionmaker, group) as session:
        await SqlAlchemyFactRepository(session).set_lifecycle(
            group_id=group,
            fact_id=str(disputed_id),
            state=FactLifecycle.DISPUTED,
        )

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
    service = _snapshot_service(sessionmaker)
    snap = await service.create(
        group_id=group,
        embedding_version=embedding_version,
        retrieval_index_version="hybrid-rrf-v1",
    )
    assert snap.fact_count == 3
    assert snap.as_of_valid_time == snap.frozen_at_system_time
    assert snap.embedding_version == embedding_version
    assert snap.retrieval_index_version == "hybrid-rrf-v1"
    assert snap.graph_projection_checkpoint == str(checkpoint)
    assert snap.retrieval_frozen is True
    assert snap.ontology_version_id is not None
    restricted = await SqlAlchemyFactCandidateSource(sessionmaker).search(
        group_id=group,
        query="paymentapi",
        limit=10,
        snapshot_id=snap.id,
        restrict_fact_ids={str(disputed_id)},
    )
    assert [hit.fact_id for hit in restricted] == [str(disputed_id)]
    fetched = await service.get(group_id=group, snapshot_id=snap.id)
    assert fetched == snap
    assert (
        await _count(
            sessionmaker,
            group,
            "SELECT count(*) FROM knowledge_events WHERE event_type='SNAPSHOT_CREATED'",
        )
        == 1
    )


async def test_snapshot_inputs_are_immutable_and_protect_live_facts(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:si-{uuid7().hex[:12]}"
    subject = await _setup(sessionmaker, group)
    fact_id, _ = await _add_fact(sessionmaker, group, subject, "eks")
    snapshot = await _snapshot_service(sessionmaker).create(group_id=group)
    later_fact_id, _ = await _add_fact(sessionmaker, group, subject, "ecs")

    with pytest.raises(exc.DBAPIError, match="snapshot retrieval inputs are immutable"):
        async with sessionmaker() as session, session.begin():
            await session.execute(
                text("UPDATE snapshot_facts SET authority = 0 WHERE snapshot_id = :snapshot_id"),
                {"snapshot_id": snapshot.id},
            )
    with pytest.raises(exc.DBAPIError, match="snapshot retrieval inputs are immutable"):
        async with sessionmaker() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO snapshot_facts (snapshot_id, fact_id, group_id) "
                    "VALUES (:snapshot_id, :fact_id, :group_id)"
                ),
                {
                    "snapshot_id": snapshot.id,
                    "fact_id": later_fact_id,
                    "group_id": group,
                },
            )
    with pytest.raises(exc.DBAPIError, match="knowledge snapshots are immutable"):
        async with sessionmaker() as session, session.begin():
            await session.execute(
                text("UPDATE knowledge_snapshots SET fact_count = 0 WHERE id = :snapshot_id"),
                {"snapshot_id": snapshot.id},
            )
    with pytest.raises(exc.DBAPIError, match="knowledge snapshots are immutable"):
        async with sessionmaker() as session, session.begin():
            await session.execute(
                text("DELETE FROM knowledge_snapshots WHERE id = :snapshot_id"),
                {"snapshot_id": snapshot.id},
            )
    with pytest.raises(exc.IntegrityError):
        async with sessionmaker() as session, session.begin():
            await session.execute(
                text("DELETE FROM facts WHERE id = :fact_id"), {"fact_id": fact_id}
            )


async def test_role_enforced_snapshot_and_context_pack_writes(
    engine: AsyncEngine,
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:sr-{uuid7().hex[:12]}"
    subject = await _setup(sessionmaker, group)
    await _add_fact(sessionmaker, group, subject, "eks")
    reads = create_sessionmaker(engine, role="vera_trusted")
    snapshot = await _snapshot_service(sessionmaker).create(group_id=group)
    service = _context_service(sessionmaker, read_sessionmaker=reads)
    pack = await service.create(group_id=group, query="eks", snapshot_id=snapshot.id)

    assert pack.snapshot_id == snapshot.id
    assert await service.get(group_id=group, pack_id=pack.id) == pack


async def test_snapshot_seal_waits_for_in_flight_child_insert(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:sl-{uuid7().hex[:12]}"
    subject = await _setup(sessionmaker, group)
    fact_id, _ = await _add_fact(sessionmaker, group, subject, "eks")
    async with _tenant(sessionmaker, group) as session:
        snapshot_id = await session.scalar(
            text(
                "INSERT INTO knowledge_snapshots "
                "(group_id, policy_version, as_of_valid_time, retrieval_frozen) "
                "VALUES (:group_id, 'ontology-v2', now(), false) RETURNING id"
            ),
            {"group_id": group},
        )
    assert snapshot_id is not None
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text(
                "UPDATE knowledge_snapshots SET fact_count = 0, source_boundaries = '{}'::jsonb "
                "WHERE id = :snapshot_id AND retrieval_frozen = false"
            ),
            {"snapshot_id": snapshot_id},
        )
    other_group = f"p:sl-{uuid7().hex[:12]}"
    other_subject = await _setup(sessionmaker, other_group)
    other_fact_id, _ = await _add_fact(sessionmaker, other_group, other_subject, "ecs")
    with pytest.raises(exc.IntegrityError):
        async with _tenant(sessionmaker, group) as session:
            await session.execute(
                text(
                    "INSERT INTO snapshot_facts (snapshot_id, fact_id, group_id) "
                    "VALUES (:snapshot_id, :fact_id, :group_id)"
                ),
                {
                    "snapshot_id": snapshot_id,
                    "fact_id": other_fact_id,
                    "group_id": group,
                },
            )

    inserting = sessionmaker()
    await inserting.begin()
    await inserting.execute(text("SET LOCAL ROLE vera_app"))
    await inserting.execute(
        text("SELECT set_config('vera.group_id', :group_id, true)"), {"group_id": group}
    )
    await inserting.execute(
        text(
            "INSERT INTO snapshot_facts (snapshot_id, fact_id, group_id) "
            "VALUES (:snapshot_id, :fact_id, :group_id)"
        ),
        {"snapshot_id": snapshot_id, "fact_id": fact_id, "group_id": group},
    )

    async def _seal() -> None:
        async with _tenant(sessionmaker, group) as session:
            await session.execute(
                text(
                    "UPDATE knowledge_snapshots SET fact_count = 1, retrieval_frozen = true "
                    "WHERE id = :snapshot_id"
                ),
                {"snapshot_id": snapshot_id},
            )

    seal_task = asyncio.create_task(_seal())
    await asyncio.sleep(0.05)
    assert not seal_task.done()
    await inserting.commit()
    await inserting.close()
    await seal_task

    with pytest.raises(exc.DBAPIError, match="snapshot retrieval inputs are immutable"):
        async with _tenant(sessionmaker, group) as session:
            await session.execute(
                text(
                    "INSERT INTO snapshot_facts (snapshot_id, fact_id, group_id) "
                    "VALUES (:snapshot_id, :fact_id, :group_id)"
                ),
                {"snapshot_id": snapshot_id, "fact_id": fact_id, "group_id": group},
            )


@pytest.mark.issue6_acceptance
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
    async with _tenant(sessionmaker, group) as session:
        assertions = SqlAlchemyAssertionRepository(session)
        for fact_id in (valid_id, future_id):
            await assertions.upsert(
                Assertion(
                    id=uuid7(),
                    group_id=group,
                    fact_id=fact_id,
                    polarity=Polarity.SUPPORTS,
                    recorded_at=as_of - timedelta(seconds=1),
                    run_key=str(uuid7()),
                )
            )

    snap = await _snapshot_service(sessionmaker).create(group_id=group, as_of=as_of)
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        fact_ids = await uow.snapshots.fact_ids(group_id=group, snapshot_id=snap.id)

    assert snap.as_of_valid_time == as_of
    assert str(valid_id) in fact_ids
    assert str(future_id) not in fact_ids


async def test_snapshot_query_is_reproducible_after_supersession(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:s-{uuid7().hex[:12]}"
    subject = await _setup(sessionmaker, group)
    eks_id, eks_key = await _add_fact(sessionmaker, group, subject, "eks")

    snap = await _snapshot_service(sessionmaker).create(group_id=group)

    # Newer knowledge supersedes the snapshot's fact.
    await _supersede(sessionmaker, group, eks_id)
    await _add_fact(sessionmaker, group, subject, "ecs")

    service = _context_service(sessionmaker)

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

    service = _context_service(sessionmaker)

    created = await service.create(group_id=group, query="eks", hints={"task": "deploy"})
    assert created.result_count >= 1
    assert created.token_estimate > 0

    fetched = await service.get(group_id=group, pack_id=created.id)
    assert fetched == created
    assert len(created.request_hash) == 64
    assert created.result_references == [str(result["ref"]) for result in created.results]
    assert created.expires_at > created.created_at
    assert created.assembler_version == "context-assembler-v2"
    assert created.request["query"] == "eks"
    assert created.request["hints"] == {"task": "deploy"}
    canonical_request = json.dumps(created.request, sort_keys=True, separators=(",", ":"))
    assert hashlib.sha256(canonical_request.encode()).hexdigest() == created.request_hash
    assert all(r["citation"]["ref"] for r in created.results)  # citations survive the round trip
    assert await service.get(group_id=f"p:other-{uuid7().hex[:12]}", pack_id=created.id) is None
    before = await _count(sessionmaker, group, "SELECT count(*) FROM context_packs")
    assert await service.get(group_id=group, pack_id=created.id) == created
    assert await _count(sessionmaker, group, "SELECT count(*) FROM context_packs") == before
    with pytest.raises(exc.DBAPIError, match=r"context packs are immutable|permission denied"):
        async with _tenant(sessionmaker, group) as session:
            await session.execute(
                text("UPDATE context_packs SET query = 'changed' WHERE id = :id"),
                {"id": created.id},
            )
    assert (
        await _count(
            sessionmaker,
            group,
            "SELECT count(*) FROM knowledge_events WHERE event_type='CONTEXT_PACK_CREATED'",
        )
        == 1
    )

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        expired = await uow.context_packs.save(
            group_id=group,
            query="expired",
            token_estimate=0,
            result_count=0,
            omitted=0,
            conflicts=0,
            freshness_warnings=0,
            results=[],
            request_hash="0" * 64,
            result_references=[],
            expires_at=utc_now() - timedelta(seconds=1),
            assembler_version="context-assembler-v1",
            request={},
        )
        await uow.commit()
    with pytest.raises(ContextPackExpiredError, match="expired"):
        await service.get(group_id=group, pack_id=expired.id)

    with pytest.raises(SnapshotNotFoundError, match="was not found"):
        await service.create(group_id=group, query="eks", snapshot_id=str(uuid7()))
    with pytest.raises(SnapshotNotFoundError, match="was not found"):
        await service.create(group_id=group, query="eks", snapshot_id="invalid")

    other_group = f"p:s-{uuid7().hex[:12]}"
    await _setup(sessionmaker, other_group)
    foreign = await _snapshot_service(sessionmaker).create(group_id=other_group)
    with pytest.raises(SnapshotNotFoundError, match="was not found"):
        await service.create(group_id=group, query="eks", snapshot_id=foreign.id)
    with pytest.raises(exc.IntegrityError):
        async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
            await uow.use_tenant(group)
            await uow.context_packs.save(
                group_id=group,
                query="foreign snapshot",
                snapshot_id=foreign.id,
                token_estimate=0,
                result_count=0,
                omitted=0,
                conflicts=0,
                freshness_warnings=0,
                results=[],
                request_hash="1" * 64,
                result_references=[],
                expires_at=utc_now() + timedelta(days=1),
                assembler_version="context-assembler-v2",
                request={},
            )
            await uow.commit()

    current_snapshot = await _snapshot_service(sessionmaker).create(group_id=group)
    noncanonical_id = f"{{{current_snapshot.id.upper()}}}"
    canonical_pack = await service.create(group_id=group, query="eks", snapshot_id=noncanonical_id)
    assert canonical_pack.snapshot_id == current_snapshot.id
    assert canonical_pack.request["snapshot_id"] == current_snapshot.id
    with pytest.raises(SnapshotNotReproducibleError, match="valid-time boundary"):
        await service.create(
            group_id=group,
            query="eks",
            snapshot_id=current_snapshot.id,
            as_of=current_snapshot.as_of_valid_time - timedelta(seconds=1),
        )

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.set_repeatable_read()
        await uow.use_tenant(group)
        old_assembler_snapshot = await uow.snapshots.create(
            group_id=group,
            policy_version="ontology-v2",
            assembler_version="context-assembler-v1",
        )
        await uow.commit()
    with pytest.raises(SnapshotNotReproducibleError, match="assembler version"):
        await service.create(group_id=group, query="eks", snapshot_id=old_assembler_snapshot.id)

    async with sessionmaker() as session, session.begin():
        legacy_id = await session.scalar(
            text(
                "INSERT INTO knowledge_snapshots (group_id, policy_version, as_of_valid_time) "
                "VALUES (:group, 'ontology-v1', now()) RETURNING id"
            ),
            {"group": group},
        )
        legacy_fact_id = await session.scalar(
            text("SELECT id FROM facts WHERE group_id = :group LIMIT 1"),
            {"group": group},
        )
        assert legacy_fact_id is not None
        await session.execute(
            text(
                "INSERT INTO snapshot_facts (snapshot_id, fact_id, group_id) "
                "VALUES (:snapshot, :fact, :group)"
            ),
            {"snapshot": legacy_id, "fact": legacy_fact_id, "group": group},
        )
        await session.execute(
            text(
                "UPDATE knowledge_snapshots SET fact_count = 1, "
                "source_boundaries = '{}'::jsonb WHERE id = :snapshot"
            ),
            {"snapshot": legacy_id},
        )
    assert legacy_id is not None
    async with sessionmaker() as session:
        assert not bool(
            await session.scalar(
                text("SELECT retrieval_frozen FROM knowledge_snapshots WHERE id = :snapshot"),
                {"snapshot": legacy_id},
            )
        )
    with pytest.raises(SnapshotNotReproducibleError, match="predates frozen retrieval"):
        await service.create(group_id=group, query="eks", snapshot_id=str(legacy_id))


@pytest.mark.issue6_acceptance
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
    async with _tenant(sessionmaker, group) as session:
        workspace_id = await session.scalar(
            text("SELECT workspace_id FROM projects WHERE group_id = :group_id"),
            {"group_id": group},
        )
    assert workspace_id is not None
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        source_id = await uow.sources.create(
            workspace_id=workspace_id,
            project_id=None,
            kind="confluence",
            name="C",
            trust_tier=1,
        )
        await uow.commit()
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text(
                'UPDATE knowledge_sources SET config = \'{"repository":"original"}\'::jsonb '
                "WHERE id = :source_id"
            ),
            {"source_id": source_id},
        )
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
        artifact_id = art.id
        version_id = ver.id

    async def _chunk(version: UUID, ordinal: int, body: str) -> UUID:
        chunk_id = uuid7()
        ck = fabric.chunk_key(
            artifact_version_id=version, ordinal=ordinal, content_hash=f"{version}:c{ordinal}"
        )
        async with _tenant(sessionmaker, group) as s:
            await SqlAlchemyChunkRepository(s).upsert(
                Chunk(
                    id=chunk_id,
                    artifact_version_id=version,
                    group_id=group,
                    chunk_key=ck,
                    ordinal=ordinal,
                    text=body,
                    content_hash=f"{version}:c{ordinal}",
                    token_count=len(body) // 4,
                )
            )
        return chunk_id

    alpha_id = await _chunk(version_id, 1, "deployment runbook alpha describes the stale rollout")
    async with _tenant(sessionmaker, group) as session:
        current = ArtifactVersionRow(
            artifact_id=artifact_id,
            version=2,
            content_hash="h2",
            s3_key="k2",
            reference_time=utc_now(),
            predecessor_version_id=version_id,
        )
        session.add(current)
        await session.flush()
        current_version_id = current.id
    await _chunk(
        current_version_id,
        1,
        "deployment runbook current describes the rollout",
    )
    snapshot = await _snapshot_service(sessionmaker).create(group_id=group)
    assert snapshot.source_boundaries[str(source_id)] == str(current_version_id)
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text(
                'UPDATE knowledge_sources SET config = \'{"repository":"changed"}\'::jsonb '
                "WHERE id = :source_id"
            ),
            {"source_id": source_id},
        )
    await _chunk(current_version_id, 2, "deployment runbook bravo describes a later rollout")
    with pytest.raises(exc.DBAPIError, match=r"chunks are immutable|permission denied"):
        async with _tenant(sessionmaker, group) as session:
            await session.execute(
                text("UPDATE chunks SET text = 'changed' WHERE id = :chunk_id"),
                {"chunk_id": alpha_id},
            )
    with pytest.raises(exc.DBAPIError, match=r"chunks are immutable|permission denied"):
        async with _tenant(sessionmaker, group) as session:
            await session.execute(
                text("DELETE FROM chunks WHERE id = :chunk_id"), {"chunk_id": alpha_id}
            )

    packs = _context_service(sessionmaker)
    pack = await packs.create(
        group_id=group,
        query="deployment runbook rollout",
        snapshot_id=snapshot.id,
        filters=RetrievalFilters(repository="original"),
    )
    texts = " ".join(r["text"] for r in pack.results)
    assert "current describes" in texts
    assert "alpha" not in texts
    assert "bravo" not in texts

    repeated = await packs.create(
        group_id=group,
        query="deployment runbook rollout",
        snapshot_id=snapshot.id,
        filters=RetrievalFilters(repository="original"),
    )
    assert "current describes" in " ".join(r["text"] for r in repeated.results)
    assert repeated.results == pack.results

    # Live retrieval includes the later chunk; the snapshot remains at its captured membership.
    live = await packs.create(group_id=group, query="deployment runbook rollout")
    live_texts = " ".join(r["text"] for r in live.results)
    assert "current" in live_texts and "bravo" in live_texts
    assert "alpha" not in live_texts
