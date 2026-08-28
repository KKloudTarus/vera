"""pgvector passage retrieval (gap 11): ANN over chunk embeddings behind the PassageIndex port.

Skips unless the pgvector chunk embedding column exists (present on the pgvector image after the
e2b3c4d5f6a7 migration; absent on a stock postgres image, where FTS remains the default).
"""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.knowledge import ArtifactRow, ArtifactVersionRow
from vera.adapters.persistence.repositories.fabric import SqlAlchemyChunkRepository
from vera.adapters.persistence.repositories.pgvector_index import (
    PgVectorPassageIndex,
    vector_literal,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Chunk
from vera.shared.ids import uuid7
from vera.shared.time import utc_now

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


async def _has_embedding_column(sm: async_sessionmaker[AsyncSession]) -> bool:
    async with sm() as s:
        return (
            await s.scalar(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = 'chunks' AND column_name = 'embedding'"
                )
            )
        ) is not None


async def test_pgvector_returns_the_nearest_chunk(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    if not await _has_embedding_column(sessionmaker):
        pytest.skip("pgvector chunk embedding column not present (stock postgres image)")

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

    texts = ["alpha deploy runbook", "bravo billing schema", "charlie network policy"]
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
            vec = vector_literal(await embedder.embed(body))
            await s.execute(
                text("UPDATE chunks SET embedding = CAST(:v AS vector) WHERE id = :id"),
                {"v": vec, "id": chunk.id},
            )

    index = PgVectorPassageIndex(sessionmaker, embedder)
    hits = await index.search(group_id=group, query="bravo billing schema", limit=3)
    assert hits  # ANN returned candidates
    assert hits[0].chunk_id == chunk_ids[1]  # the exact-text match ranks first
