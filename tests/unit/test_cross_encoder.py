"""Stage-3 cross-encoder reorders the reranked head by query-fact relevance."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from vera.application.queries.search_memory import SearchMemory, SearchMemoryHandler
from vera.domain.ports.memory_engine import GraphHit, GraphQuery
from vera.domain.ports.retrieval import HitProvenance
from vera.shared.types import GroupId


class _FakeEngine:
    def __init__(self, hits: list[GraphHit]) -> None:
        self._hits = hits

    async def ingest_episode(self, episode: object) -> object:  # pragma: no cover
        raise NotImplementedError

    async def search(self, query: GraphQuery) -> Sequence[GraphHit]:
        return self._hits

    async def health(self) -> bool:  # pragma: no cover
        return True


class _EmptyReadModel:
    async def enrich(
        self, *, group_ids: Sequence[str], edge_uuids: Sequence[str]
    ) -> dict[str, HitProvenance]:
        return {}

    async def feedback_counts(
        self, *, group_ids: Sequence[str], refs: Sequence[str]
    ) -> dict[str, tuple[int, int]]:
        return {}


class _FakeReranker:
    def __init__(self, scores: dict[str, float]) -> None:
        self._scores = scores

    async def rerank(self, *, query: str, facts: Sequence[str]) -> list[float]:
        return [self._scores[f] for f in facts]


@pytest.mark.asyncio
async def test_cross_encoder_reorders_by_relevance() -> None:
    # Stage-2 blend orders by relevance: A > B > C. The cross-encoder disagrees: C is the
    # most relevant, then A, then B. With weight 1.0 the final order follows the encoder.
    hits = [
        GraphHit(fact="A", score=3.0, edge_uuid="a"),
        GraphHit(fact="B", score=2.0, edge_uuid="b"),
        GraphHit(fact="C", score=1.0, edge_uuid="c"),
    ]
    handler = SearchMemoryHandler(
        _FakeEngine(hits),
        _EmptyReadModel(),
        reranker=_FakeReranker({"A": 0.4, "B": 0.1, "C": 0.9}),
        cross_encoder_weight=1.0,
        cross_encoder_top_n=10,
    )
    ranked = await handler.handle(SearchMemory(text="q", group_ids=(GroupId("p:x"),), limit=3))
    assert [h.fact for h in ranked] == ["C", "A", "B"]


@pytest.mark.asyncio
async def test_without_reranker_keeps_stage2_order() -> None:
    hits = [
        GraphHit(fact="A", score=3.0, edge_uuid="a"),
        GraphHit(fact="B", score=2.0, edge_uuid="b"),
    ]
    handler = SearchMemoryHandler(_FakeEngine(hits), _EmptyReadModel())
    ranked = await handler.handle(SearchMemory(text="q", group_ids=(GroupId("p:x"),), limit=2))
    assert [h.fact for h in ranked] == ["A", "B"]
