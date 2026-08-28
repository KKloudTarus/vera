"""pgvector passage and code candidates for one active embedding model."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories.passage_index import (
    passage_hit,
    retrieval_filter_params,
)
from vera.domain.ports.embedder import Embedder
from vera.domain.ports.retrieval_index import PassageHit, RetrievalFilters

_ANN = """
SELECT c.id, c.artifact_version_id, c.text, c.heading_path, c.symbol_name,
       c.start_offset, c.end_offset, c.page_number, c.start_line, c.end_line,
       1 - (ce.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension}))) AS score
FROM chunk_embeddings ce
JOIN chunks c ON c.id = ce.chunk_id
JOIN artifact_versions av ON av.id = c.artifact_version_id
JOIN artifacts a ON a.id = av.artifact_id
JOIN knowledge_sources s ON s.id = a.source_id
WHERE ce.group_id = :g AND c.group_id = :g AND ce.active
  AND ce.provider = :provider AND ce.model = :model
  AND ce.model_version = :model_version AND ce.dimension = :dimension
  AND (CAST(:created_before AS timestamptz) IS NULL OR c.created_at <= :created_before)
  AND (CAST(:repository AS text) IS NULL OR s.config->>'repository' = :repository)
  AND (CAST(:branch AS text) IS NULL OR s.config->>'branch' = :branch)
  AND (CAST(:code_path AS text) IS NULL
       OR coalesce(c.heading_path, '') LIKE '%' || :code_path || '%')
  AND (CAST(:document_type AS text) IS NULL OR s.config->>'document_type' = :document_type)
  AND (CAST(:source_type AS text) IS NULL OR s.kind = :source_type)
  AND (CAST(:max_trust_tier AS integer) IS NULL OR s.trust_tier <= :max_trust_tier)
{code_filter}
ORDER BY ce.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension}))
LIMIT :lim
"""


def vector_literal(vector: list[float]) -> str:
    """pgvector text form, e.g. ``[0.1,0.2]``, cast to ``vector`` in the query."""
    return "[" + ",".join(repr(float(v)) for v in vector) + "]"


class _PgVectorSearch:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        embedder: Embedder,
        *,
        provider: str = "legacy",
        model: str = "legacy-1024",
        model_version: str = "1",
        dimension: int = 1024,
    ) -> None:
        if dimension <= 0:
            raise ValueError("embedding dimension must be positive")
        self._session_factory = session_factory
        self._embedder = embedder
        self._provider = provider
        self._model = model
        self._model_version = model_version
        self._dimension = dimension

    async def _search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None,
        code: bool,
        filters: RetrievalFilters | None,
    ) -> list[PassageHit]:
        vector = await self._embedder.embed(query)
        if len(vector) != self._dimension:
            raise ValueError(
                f"embedding dimension mismatch: expected {self._dimension}, got {len(vector)}"
            )
        qvec = vector_literal(vector)
        sql = _ANN.format(
            code_filter="AND c.symbol_name IS NOT NULL" if code else "",
            dimension=self._dimension,
        )
        async with self._session_factory() as session:
            rows = (
                (
                    await session.execute(
                        text(sql),
                        {
                            "g": group_id,
                            "qvec": qvec,
                            "lim": limit,
                            "created_before": created_before,
                            "provider": self._provider,
                            "model": self._model,
                            "model_version": self._model_version,
                            "dimension": self._dimension,
                            **retrieval_filter_params(filters),
                        },
                    )
                )
                .mappings()
                .all()
            )
        return [passage_hit(r) for r in rows]


class PgVectorPassageIndex(_PgVectorSearch):
    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        return await self._search(
            group_id=group_id,
            query=query,
            limit=limit,
            created_before=created_before,
            code=False,
            filters=filters,
        )


class PgVectorCodeIndex(_PgVectorSearch):
    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        return await self._search(
            group_id=group_id,
            query=query,
            limit=limit,
            created_before=created_before,
            code=True,
            filters=filters,
        )
