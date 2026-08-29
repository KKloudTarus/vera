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
SELECT c.id, c.artifact_version_id, c.text, c.content_hash, c.heading_path, c.symbol_name,
       c.start_offset, c.end_offset, c.page_number, c.start_line, c.end_line,
       1 - (ce.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension}))) AS score
FROM chunk_embeddings ce
JOIN chunks c ON c.id = ce.chunk_id
JOIN artifact_versions av ON av.id = c.artifact_version_id
JOIN artifacts a ON a.id = av.artifact_id
JOIN knowledge_sources s ON s.id = a.source_id
LEFT JOIN projects p ON p.id = s.project_id
JOIN workspaces w ON w.id = s.workspace_id
WHERE ce.group_id = :g AND c.group_id = :g
  AND ((s.project_id IS NOT NULL AND p.group_id = c.group_id)
       OR (s.project_id IS NULL AND (w.group_id = c.group_id OR EXISTS (
           SELECT 1 FROM projects wp
           WHERE wp.workspace_id = s.workspace_id AND wp.group_id = c.group_id))))
  AND ce.active
  AND ce.provider = :provider AND ce.model = :model
  AND ce.model_version = :model_version AND ce.dimension = :dimension
  AND (CAST(:created_before AS timestamptz) IS NULL OR c.created_at <= :created_before)
{source_filters}
  AND (CAST(:code_path AS text) IS NULL
       OR coalesce(c.heading_path, '') LIKE '%' || :code_path || '%')
{code_filter}
ORDER BY ce.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension})), c.id ASC
LIMIT :lim
"""

_ANN_SNAPSHOT = """
SELECT sc.chunk_id AS id, sc.artifact_version_id, sc.text, sc.content_hash,
       sc.heading_path, sc.symbol_name,
       sc.start_offset, sc.end_offset, sc.page_number, sc.start_line, sc.end_line,
       1 - (CAST(sce.embedding AS vector({dimension}))
            <=> CAST(:qvec AS vector({dimension}))) AS score
FROM snapshot_chunk_embeddings sce
JOIN snapshot_chunks sc ON sc.snapshot_id = sce.snapshot_id
 AND sc.chunk_id = sce.chunk_id AND sc.group_id = sce.group_id
JOIN snapshot_sources ss ON ss.snapshot_id = sc.snapshot_id
 AND ss.knowledge_source_id = sc.knowledge_source_id AND ss.group_id = sc.group_id
WHERE sce.snapshot_id = CAST(:snapshot_id AS uuid) AND sce.group_id = :g
  AND sce.provider = :provider AND sce.model = :model
  AND sce.model_version = :model_version AND sce.dimension = :dimension
{source_filters}
  AND (CAST(:code_path AS text) IS NULL
       OR coalesce(sc.heading_path, '') LIKE '%' || :code_path || '%')
{code_filter}
ORDER BY CAST(sce.embedding AS vector({dimension}))
         <=> CAST(:qvec AS vector({dimension})), sc.chunk_id ASC
LIMIT :lim
"""

_LIVE_SOURCE_FILTERS = """
  AND (CAST(:repository AS text) IS NULL OR s.config->>'repository' = :repository)
  AND (CAST(:branch AS text) IS NULL OR s.config->>'branch' = :branch)
  AND (CAST(:document_type AS text) IS NULL OR s.config->>'document_type' = :document_type)
  AND (CAST(:source_type AS text) IS NULL OR s.kind = :source_type)
  AND (CAST(:max_trust_tier AS integer) IS NULL OR s.trust_tier <= :max_trust_tier)
"""
_SNAPSHOT_SOURCE_FILTERS = """
  AND (CAST(:repository AS text) IS NULL OR ss.repository = :repository)
  AND (CAST(:branch AS text) IS NULL OR ss.branch = :branch)
  AND (CAST(:document_type AS text) IS NULL OR ss.document_type = :document_type)
  AND (CAST(:source_type AS text) IS NULL OR ss.source_type = :source_type)
  AND (CAST(:max_trust_tier AS integer) IS NULL OR ss.trust_tier <= :max_trust_tier)
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
        snapshot_id: str | None,
        code: bool,
        filters: RetrievalFilters | None,
    ) -> list[PassageHit]:
        vector = await self._embedder.embed(query)
        if len(vector) != self._dimension:
            raise ValueError(
                f"embedding dimension mismatch: expected {self._dimension}, got {len(vector)}"
            )
        qvec = vector_literal(vector)
        sql = (_ANN_SNAPSHOT if snapshot_id is not None else _ANN).format(
            code_filter=(
                "AND sc.symbol_name IS NOT NULL"
                if code and snapshot_id is not None
                else "AND c.symbol_name IS NOT NULL"
                if code
                else ""
            ),
            dimension=self._dimension,
            source_filters=(
                _SNAPSHOT_SOURCE_FILTERS if snapshot_id is not None else _LIVE_SOURCE_FILTERS
            ),
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
                            "snapshot_id": snapshot_id,
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
        snapshot_id: str | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        return await self._search(
            group_id=group_id,
            query=query,
            limit=limit,
            created_before=created_before,
            snapshot_id=snapshot_id,
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
        snapshot_id: str | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        return await self._search(
            group_id=group_id,
            query=query,
            limit=limit,
            created_before=created_before,
            snapshot_id=snapshot_id,
            code=True,
            filters=filters,
        )
