"""SearchMemory stage-2 rerank: signals blend, provenance is carried, ordering."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from vera.adapters.graph.null import NullMemoryEngine
from vera.application.queries import search_memory
from vera.application.queries.search_memory import SearchMemory, SearchMemoryHandler
from vera.domain.ports.memory_engine import GraphHit, GraphQuery
from vera.domain.ports.retrieval import HitProvenance
from vera.observability.cost import (
    UsageContext,
    current_usage_context,
    reset_usage_context,
    set_usage_context,
)
from vera.shared.types import GroupId


class _FakeEngine:
    def __init__(self, hits: list[GraphHit]) -> None:
        self._hits = hits
        self.usage_context: UsageContext | None = None

    async def ingest_episode(self, episode: object) -> object:  # pragma: no cover
        raise NotImplementedError

    async def search(self, query: GraphQuery) -> Sequence[GraphHit]:
        self.usage_context = current_usage_context()
        return self._hits

    async def health(self) -> bool:
        return True


class _FakeReadModel:
    def __init__(
        self,
        provenance: dict[str, HitProvenance],
        feedback: dict[str, tuple[int, int]] | None = None,
    ) -> None:
        self._provenance = provenance
        self._feedback = feedback or {}

    async def enrich(
        self, *, group_ids: Sequence[str], edge_uuids: Sequence[str]
    ) -> dict[str, HitProvenance]:
        return self._provenance

    async def feedback_counts(
        self, *, group_ids: Sequence[str], refs: Sequence[str]
    ) -> dict[str, tuple[int, int]]:
        return self._feedback


@pytest.mark.asyncio
async def test_authority_breaks_ties_and_provenance_is_carried() -> None:
    hits = [
        GraphHit(fact="low authority fact", score=1.0, edge_uuid="b"),
        GraphHit(fact="high authority fact", score=1.0, edge_uuid="a"),
    ]
    provenance = {
        "a": HitProvenance(
            edge_uuid="a", verification="human_verified", authority=1.0, source_id="src-a"
        ),
        "b": HitProvenance(edge_uuid="b", verification="pending", authority=0.4, source_id="src-b"),
    }
    handler = SearchMemoryHandler(_FakeEngine(hits), _FakeReadModel(provenance))

    ranked = await handler.handle(
        SearchMemory(text="fact", group_ids=(GroupId("p:demo"),), limit=5)
    )

    assert ranked[0].fact == "high authority fact"
    assert ranked[0].authority == 1.0
    assert ranked[0].verification == "human_verified"
    assert ranked[0].source_id == "src-a"


@pytest.mark.asyncio
async def test_downvotes_lower_the_score() -> None:
    hit = [GraphHit(fact="f", score=1.0, edge_uuid="a")]
    prov = {
        "a": HitProvenance(
            edge_uuid="a", verification="human_verified", authority=1.0, source_id="s"
        )
    }
    neutral = SearchMemoryHandler(_FakeEngine(hit), _FakeReadModel(prov))
    downvoted = SearchMemoryHandler(_FakeEngine(hit), _FakeReadModel(prov, {"a": (0, 5)}))

    q = SearchMemory(text="f", group_ids=(GroupId("p:demo"),), limit=5)
    assert (await downvoted.handle(q))[0].score < (await neutral.handle(q))[0].score


@pytest.mark.asyncio
async def test_empty_search_returns_empty() -> None:
    handler = SearchMemoryHandler(NullMemoryEngine(), _FakeReadModel({}))
    assert await handler.handle(SearchMemory(text="x", group_ids=(GroupId("p:demo"),))) == []


@pytest.mark.asyncio
async def test_search_preserves_parent_usage_ref() -> None:
    engine = _FakeEngine([])
    handler = SearchMemoryHandler(engine, _FakeReadModel({}))
    parent_context = UsageContext(request_kind="search", group_id="p:outer", ref="query-1")
    token = set_usage_context(parent_context)
    try:
        await handler.handle(SearchMemory(text="x", group_ids=(GroupId("p:demo"),)))

        assert engine.usage_context == UsageContext(
            request_kind="search", group_id="p:demo", ref="query-1"
        )
        assert current_usage_context() == parent_context
    finally:
        reset_usage_context(token)


@pytest.mark.asyncio
async def test_failed_search_records_latency(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[dict[str, float | int]] = []
    engine = _FakeEngine([])

    async def fail(_query: GraphQuery) -> Sequence[GraphHit]:
        raise TimeoutError

    monkeypatch.setattr(engine, "search", fail)
    monkeypatch.setattr(search_memory, "record_search", lambda **values: observed.append(values))
    handler = SearchMemoryHandler(engine, _FakeReadModel({}))

    with pytest.raises(TimeoutError):
        await handler.handle(SearchMemory(text="x", group_ids=(GroupId("p:demo"),)))

    assert len(observed) == 1
    assert observed[0]["duration_s"] >= 0
    assert observed[0]["hits"] == 0
