"""pgvector passage and code candidate sources.

Approximate nearest-neighbor search over ``chunks.embedding`` (an HNSW cosine index), behind
the same PassageIndex / CodeIndex ports as the full-text default, so the application layer
swaps backends without change. The query is embedded once per call through the Embedder port;
the score is cosine similarity (1 - distance) so it orders the same direction as ts_rank.

The embedding column is optional (see migration e2b3c4d5f6a7): rows without an embedding are
skipped, so a partial backfill degrades to fewer candidates rather than an error.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories.passage_index import passage_hit
from vera.domain.ports.embedder import Embedder
from vera.domain.ports.retrieval_index import PassageHit

_ANN = """
SELECT id, artifact_version_id, text, heading_path, symbol_name,
       start_offset, end_offset, page_number, start_line, end_line,
       1 - (embedding <=> CAST(:qvec AS vector)) AS score
FROM chunks
WHERE group_id = :g AND embedding IS NOT NULL
  AND (CAST(:created_before AS timestamptz) IS NULL OR created_at <= :created_before)
{code_filter}
ORDER BY embedding <=> CAST(:qvec AS vector)
LIMIT :lim
"""


def vector_literal(vector: list[float]) -> str:
    """pgvector text form, e.g. ``[0.1,0.2]``, cast to ``vector`` in the query."""
    return "[" + ",".join(repr(float(v)) for v in vector) + "]"


class _PgVectorSearch:
    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], embedder: Embedder
    ) -> None:
        self._session_factory = session_factory
        self._embedder = embedder

    async def _search(
        self, *, group_id: str, query: str, limit: int, created_before: datetime | None, code: bool
    ) -> list[PassageHit]:
        qvec = vector_literal(await self._embedder.embed(query))
        sql = _ANN.format(code_filter="AND symbol_name IS NOT NULL" if code else "")
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
                        },
                    )
                )
                .mappings()
                .all()
            )
        return [passage_hit(r) for r in rows]


class PgVectorPassageIndex(_PgVectorSearch):
    async def search(
        self, *, group_id: str, query: str, limit: int, created_before: datetime | None = None
    ) -> list[PassageHit]:
        return await self._search(
            group_id=group_id, query=query, limit=limit, created_before=created_before, code=False
        )


class PgVectorCodeIndex(_PgVectorSearch):
    async def search(
        self, *, group_id: str, query: str, limit: int, created_before: datetime | None = None
    ) -> list[PassageHit]:
        return await self._search(
            group_id=group_id, query=query, limit=limit, created_before=created_before, code=True
        )
