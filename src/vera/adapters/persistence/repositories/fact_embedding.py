"""Persistence for provider-neutral, multi-model fact embeddings."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from vera.domain.knowledge.fabric import FactEmbedding

_UPSERT = text(
    """
    INSERT INTO fact_embeddings (
        id, group_id, fact_id, provider, model, model_version, dimension,
        embedding, content_hash, created_at, active
    ) VALUES (
        :id, :group_id, :fact_id, :provider, :model, :model_version, :dimension,
        CAST(:embedding AS vector), :content_hash, :created_at, :active
    )
    ON CONFLICT (fact_id, provider, model, model_version, dimension) DO UPDATE SET
        embedding = EXCLUDED.embedding,
        content_hash = EXCLUDED.content_hash,
        created_at = EXCLUDED.created_at,
        active = EXCLUDED.active
    """
)


def _vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(repr(float(value)) for value in vector) + "]"


class SqlAlchemyFactEmbeddingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def exists(
        self,
        *,
        group_id: str,
        fact_id: UUID,
        provider: str,
        model: str,
        model_version: str,
        dimension: int,
    ) -> bool:
        found = await self._session.scalar(
            text(
                "SELECT 1 FROM fact_embeddings WHERE group_id = :group_id "
                "AND fact_id = :fact_id "
                "AND provider = :provider AND model = :model "
                "AND model_version = :model_version AND dimension = :dimension LIMIT 1"
            ),
            {
                "group_id": group_id,
                "fact_id": str(fact_id),
                "provider": provider,
                "model": model,
                "model_version": model_version,
                "dimension": dimension,
            },
        )
        return found is not None

    async def upsert(self, embedding: FactEmbedding) -> None:
        await self._session.execute(
            _UPSERT,
            {
                "id": str(embedding.id),
                "group_id": embedding.group_id,
                "fact_id": str(embedding.fact_id),
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
