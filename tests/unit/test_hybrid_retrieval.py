from __future__ import annotations

from datetime import datetime

import pytest

from vera.application.retrieval import HybridPassageIndex
from vera.domain.ports.retrieval_index import PassageHit


class _Index:
    def __init__(self, hits: list[PassageHit]) -> None:
        self._hits = hits

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None = None,
    ) -> list[PassageHit]:
        del group_id, query, created_before
        return self._hits[:limit]


def _hit(chunk_id: str, score: float) -> PassageHit:
    return PassageHit(
        chunk_id=chunk_id,
        artifact_version_id=f"version-{chunk_id}",
        text=chunk_id,
        score=score,
    )


@pytest.mark.asyncio
async def test_rrf_keeps_lexical_only_and_vector_only_hits() -> None:
    hybrid = HybridPassageIndex(
        _Index([_hit("shared", 10.0), _hit("fts-only", 9.0)]),
        _Index([_hit("shared", 0.99), _hit("vector-only", 0.98)]),
    )

    hits = await hybrid.search(group_id="p:test", query="runtime", limit=3)

    assert [hit.chunk_id for hit in hits] == ["shared", "fts-only", "vector-only"]
    assert hits[0].score > hits[1].score
