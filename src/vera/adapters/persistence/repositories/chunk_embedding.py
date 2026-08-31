"""Persistence for provider-neutral, multi-model chunk embeddings."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from vera.domain.knowledge.fabric import ChunkEmbedding

_UPSERT = text(
    """
    INSERT INTO chunk_embeddings (
        id, group_id, chunk_id, provider, model, model_version, dimension,
        embedding, content_hash, created_at, active
    ) VALUES (
        :id, :group_id, :chunk_id, :provider, :model, :model_version, :dimension,
        CAST(:embedding AS vector), :content_hash, :created_at, :active
    )
    ON CONFLICT (chunk_id, provider, model, model_version, dimension) DO UPDATE SET
        embedding = EXCLUDED.embedding,
        content_hash = EXCLUDED.content_hash,
        created_at = EXCLUDED.created_at,
        active = EXCLUDED.active
    """
)


def _vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(repr(float(value)) for value in vector) + "]"


class SqlAlchemyChunkEmbeddingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, embedding: ChunkEmbedding) -> None:
        await self._session.execute(
            _UPSERT,
            {
                "id": str(embedding.id),
                "group_id": embedding.group_id,
                "chunk_id": str(embedding.chunk_id),
                "provider": embedding.provider,
                "model": embedding.model,
                "model_version": embedding.model_version,
                "dimension": embedding.dimension,
                "embedding": _vector_literal(embedding.embedding),
                "content_hash": embedding.content_hash,
                "created_at": embedding.created_at,
                "active": embedding.active,
            },
        )

    async def set_active_model(
        self,
        *,
        group_id: str,
        provider: str,
        model: str,
        model_version: str,
        dimension: int,
    ) -> None:
        await self._session.execute(
            text("UPDATE chunk_embeddings SET active = false WHERE group_id = :g"),
            {"g": group_id},
        )
        await self._session.execute(
            text(
                "UPDATE chunk_embeddings SET active = true WHERE group_id = :g "
                "AND provider = :provider AND model = :model AND model_version = :version "
                "AND dimension = :dimension"
            ),
            {
                "g": group_id,
                "provider": provider,
                "model": model,
                "version": model_version,
                "dimension": dimension,
            },
        )
