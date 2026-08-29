"""Hybrid retrieval and multi-model chunk embeddings against pgvector."""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.persistence.models.knowledge import ArtifactRow, ArtifactVersionRow
from vera.adapters.persistence.repositories.chunk_embedding import (
    SqlAlchemyChunkEmbeddingRepository,
)
from vera.adapters.persistence.repositories.fabric import SqlAlchemyChunkRepository
from vera.adapters.persistence.repositories.passage_index import SqlAlchemyPassageIndex
from vera.adapters.persistence.repositories.pgvector_index import (
    PgVectorPassageIndex,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.application.retrieval import HybridPassageIndex
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Chunk, ChunkEmbedding
from vera.domain.ports.snapshot import Snapshot
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import JsonDict

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_DIM = 1024


class _FakeEmbedder:
    """Deterministic 1024-dim unit vectors: identical text yields an identical vector, so the
    nearest neighbor of a chunk's own text is that chunk (distance 0).
    """

    async def embed(self, text: str) -> list[float]:
        values: list[float] = []
        counter = 0
        while len(values) < _DIM:
            digest = hashlib.sha256(f"{counter}:{text}".encode()).digest()
            for off in range(0, len(digest), 4):
                values.append(struct.unpack("<I", digest[off : off + 4])[0] / 2**32 - 0.5)
                if len(values) >= _DIM:
                    break
            counter += 1
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]


@asynccontextmanager
async def _tenant(sm: async_sessionmaker[AsyncSession], group: str) -> AsyncIterator[AsyncSession]:
    async with sm() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


async def _snapshot(
    sessionmaker: async_sessionmaker[AsyncSession],
    group: str,
    *,
    embedding_version: JsonDict,
) -> Snapshot:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.set_repeatable_read()
        await uow.use_tenant(group)
        snapshot = await uow.snapshots.create(
            group_id=group,
            policy_version="ontology-v2",
            embedding_version=embedding_version,
            retrieval_index_version="hybrid-rrf-v1",
        )
        await uow.commit()
        return snapshot


@pytest.mark.issue6_acceptance
async def test_hybrid_retrieval_and_model_rollback(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:vec-{uuid7().hex[:12]}"
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

    query = "hybrid lexical needle"
    texts = [query, "unrelated dense body"]
    embedder = _FakeEmbedder()
    async with _tenant(sessionmaker, group) as s:
        art = ArtifactRow(
            source_id=source_id,
            external_id="a",
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
        repo = SqlAlchemyChunkRepository(s)
        embedding_repo = SqlAlchemyChunkEmbeddingRepository(s)
        chunk_ids: list[str] = []
        for i, body in enumerate(texts):
            chunk = await repo.upsert(
                Chunk(
                    id=uuid7(),
                    artifact_version_id=ver.id,
                    group_id=group,
                    chunk_key=fabric.chunk_key(
                        artifact_version_id=ver.id, ordinal=i, content_hash=f"c{i}"
                    ),
                    ordinal=i,
                    text=body,
                    content_hash=f"c{i}",
                    token_count=len(body) // 4,
                )
            )
            chunk_ids.append(str(chunk.id))
            if i == 1:
                for model_version in ("1", "2"):
                    await embedding_repo.upsert(
                        ChunkEmbedding(
                            id=uuid7(),
                            group_id=group,
                            chunk_id=chunk.id,
                            provider="test",
                            model="deterministic",
                            model_version=model_version,
                            dimension=_DIM,
                            embedding=await embedder.embed(query),
                            content_hash=chunk.content_hash,
                            created_at=utc_now(),
                        )
                    )
        await embedding_repo.set_active_model(
            group_id=group,
            provider="test",
            model="deterministic",
            model_version="2",
        )
        await embedding_repo.set_active_model(
            group_id=group,
            provider="test",
            model="deterministic",
            model_version="1",
        )
        rows = (
            (
                await s.execute(
                    text(
                        "SELECT model_version, dimension, content_hash, active "
                        "FROM chunk_embeddings WHERE group_id = :g ORDER BY model_version"
                    ),
                    {"g": group},
                )
            )
            .mappings()
            .all()
        )

    assert len(rows) == 2
    assert rows[0] == {
        "model_version": "1",
        "dimension": _DIM,
        "content_hash": "c1",
        "active": True,
    }
    assert rows[1]["active"] is False

    foreign_group = f"p:vec-{uuid7().hex[:12]}"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(foreign_group)
        foreign_org = await uow.tenancy.create_organization(
            slug=f"o-{foreign_group}", name="O", group_id=f"o:{foreign_group}"
        )
        foreign_ws = await uow.tenancy.create_workspace(
            org_id=foreign_org.id,
            slug=f"w-{foreign_group}",
            name="W",
            group_id=f"w:{foreign_group}",
        )
        foreign_project = await uow.tenancy.create_project(
            workspace_id=foreign_ws.id,
            slug=f"pr-{foreign_group}",
            name="P",
            group_id=foreign_group,
        )
        foreign_source = await uow.sources.create(
            workspace_id=foreign_ws.id,
            project_id=foreign_project.id,
            kind="confluence",
            name="Foreign",
            trust_tier=1,
        )
        await uow.commit()
    async with _tenant(sessionmaker, foreign_group) as session:
        foreign_artifact = ArtifactRow(
            source_id=foreign_source,
            external_id="foreign",
            content_hash="foreign",
            s3_key="foreign",
            reference_time=utc_now(),
        )
        session.add(foreign_artifact)
        await session.flush()
        foreign_version = ArtifactVersionRow(
            artifact_id=foreign_artifact.id,
            version=1,
            content_hash="foreign",
            s3_key="foreign",
            reference_time=utc_now(),
        )
        session.add(foreign_version)
        await session.flush()
        foreign_version_id = foreign_version.id
    foreign_chunk_id = uuid7()
    async with _tenant(sessionmaker, group) as session:
        await SqlAlchemyChunkRepository(session).upsert(
            Chunk(
                id=foreign_chunk_id,
                artifact_version_id=foreign_version_id,
                group_id=group,
                chunk_key=fabric.chunk_key(
                    artifact_version_id=foreign_version_id,
                    ordinal=0,
                    content_hash="foreign",
                ),
                ordinal=0,
                text="cross tenant vector secret",
                content_hash="foreign",
                token_count=6,
            )
        )
        await SqlAlchemyChunkEmbeddingRepository(session).upsert(
            ChunkEmbedding(
                id=uuid7(),
                group_id=group,
                chunk_id=foreign_chunk_id,
                provider="test",
                model="deterministic",
                model_version="1",
                dimension=_DIM,
                embedding=await embedder.embed(query),
                content_hash="foreign",
                created_at=utc_now(),
            )
        )

    vector = PgVectorPassageIndex(
        sessionmaker,
        embedder,
        provider="test",
        model="deterministic",
        model_version="1",
        dimension=_DIM,
    )
    vector_hits = await vector.search(group_id=group, query=query, limit=3)
    assert vector_hits[0].chunk_id == chunk_ids[1]
    assert str(foreign_chunk_id) not in {hit.chunk_id for hit in vector_hits}

    hybrid = HybridPassageIndex(SqlAlchemyPassageIndex(sessionmaker), vector)
    hits = await hybrid.search(group_id=group, query=query, limit=3)
    assert {hit.chunk_id for hit in hits} == {chunk_ids[0], chunk_ids[1]}

    inactive_snapshot = await _snapshot(
        sessionmaker,
        group,
        embedding_version={
            "provider": "test",
            "model": "deterministic",
            "model_version": "2",
            "dimension": _DIM,
        },
    )
    inactive_vector = PgVectorPassageIndex(
        sessionmaker,
        embedder,
        provider="test",
        model="deterministic",
        model_version="2",
        dimension=_DIM,
    )
    assert (
        await inactive_vector.search(
            group_id=group, query=query, limit=3, snapshot_id=inactive_snapshot.id
        )
        == []
    )

    snapshot = await _snapshot(
        sessionmaker,
        group,
        embedding_version={
            "provider": "test",
            "model": "deterministic",
            "model_version": "1",
            "dimension": _DIM,
        },
    )
    frozen_before = await vector.search(
        group_id=group, query=query, limit=3, snapshot_id=snapshot.id
    )
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text(
                "DELETE FROM chunk_embeddings WHERE group_id = :g "
                "AND provider = 'test' AND model = 'deterministic' AND model_version = '1'"
            ),
            {"g": group},
        )
    assert await vector.search(group_id=group, query=query, limit=3) == []
    frozen_after = await vector.search(
        group_id=group, query=query, limit=3, snapshot_id=snapshot.id
    )
    assert frozen_after == frozen_before


async def test_live_ingest_embeds_chunks_and_rejects_dimension_mismatch(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:live-vec-{uuid7().hex[:12]}"
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
            workspace_id=ws.id, project_id=proj.id, kind="confluence", name="C", trust_tier=3
        )
        await uow.commit()

    embedder = _FakeEmbedder()
    body = "automatically embedded chunk"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        result = await CurationService(
            uow,
            StructuredClaimExtractor(),
            embedder=embedder,
            embedding_provider="test",
            embedding_model="deterministic",
            embedding_model_version="live",
            embedding_dimension=_DIM,
        ).ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=group,
                external_id="live-page",
                body=body,
                knowledge_type="text",
            )
        )
        await uow.commit()

    index = PgVectorPassageIndex(
        sessionmaker,
        embedder,
        provider="test",
        model="deterministic",
        model_version="live",
        dimension=_DIM,
    )
    hits = await index.search(group_id=group, query=body, limit=1)
    assert hits
    async with _tenant(sessionmaker, group) as session:
        stored = (
            (
                await session.execute(
                    text(
                        "SELECT ce.provider, ce.model, ce.model_version, ce.dimension, "
                        "ce.content_hash FROM chunk_embeddings ce "
                        "JOIN chunks c ON c.id = ce.chunk_id "
                        "WHERE c.artifact_version_id = :version_id"
                    ),
                    {"version_id": result.value.artifact_version_id},
                )
            )
            .mappings()
            .one()
        )
    assert stored["provider"] == "test"
    assert stored["model"] == "deterministic"
    assert stored["model_version"] == "live"
    assert stored["dimension"] == _DIM
    assert stored["content_hash"]

    with pytest.raises(ValueError, match="expected 3, got 1024"):
        async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
            await uow.use_tenant(group)
            await CurationService(
                uow,
                StructuredClaimExtractor(),
                embedder=embedder,
                embedding_provider="test",
                embedding_model="deterministic",
                embedding_model_version="invalid",
                embedding_dimension=3,
            ).ingest_artifact(
                IngestArtifact(
                    source_id=source_id,
                    group_id=group,
                    external_id="bad-dimension",
                    body="must roll back",
                    knowledge_type="text",
                )
            )
