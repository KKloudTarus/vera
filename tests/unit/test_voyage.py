"""Voyage adapters: embeddings and reranking over a fake HTTP client (no network)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import SecretStr

from vera.adapters.embedding.voyage import VoyageClient, VoyageEmbedder, VoyageReranker
from vera.config.settings import (
    MemorySettings,
    VoyageSettings,
    active_embedding,
    get_settings,
    voyage_api_key,
)
from vera.domain.ports.reranker import RerankerUnavailableError
from vera.observability.cost import (
    ProviderBudgetContext,
    UsageAccountingError,
    UsageContext,
    UsageEvent,
    reset_provider_budget_context,
    reset_usage_context,
    set_provider_budget_context,
    set_usage_context,
)


class _Resp:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._payload


class _FakeHttp:
    def __init__(self, payload: Any = None, raise_exc: Exception | None = None) -> None:
        self._payload = payload or {}
        self._raise = raise_exc
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def post(self, url: str, json: dict[str, Any]) -> _Resp:
        self.calls.append((url, json))
        if self._raise is not None:
            raise self._raise
        payload = dict(self._payload) if isinstance(self._payload, dict) else self._payload
        if isinstance(payload, dict):
            payload.setdefault("model", json["model"])
        return _Resp(payload)


class _BlockingHttp(_FakeHttp):
    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__(payload)
        self.started = asyncio.Event()
        self.retried = asyncio.Event()
        self.release = asyncio.Event()

    async def post(self, url: str, json: dict[str, Any]) -> _Resp:
        self.calls.append((url, json))
        self.started.set()
        if len(self.calls) > 1:
            self.retried.set()
        await self.release.wait()
        payload = dict(self._payload)
        payload.setdefault("model", json["model"])
        return _Resp(payload)


class _Sink:
    def __init__(self) -> None:
        self.events: list[UsageEvent] = []

    async def record(self, event: UsageEvent) -> None:
        self.events.append(event)


@pytest.mark.asyncio
async def test_embedder_sends_model_and_dim_and_returns_vector() -> None:
    fake = _FakeHttp(
        {"data": [{"index": 0, "embedding": [0.1, 0.2, 0.3]}], "usage": {"total_tokens": 4}}
    )
    embedder = VoyageEmbedder(VoyageClient(api_key="k", client=fake), model="voyage-3.5", dim=1024)
    vec = await embedder.embed("hello")
    assert vec == [0.1, 0.2, 0.3]
    url, body = fake.calls[0]
    assert url == "/embeddings"
    assert body["model"] == "voyage-3.5" and body["output_dimension"] == 1024
    assert body["input"] == ["hello"]


@pytest.mark.asyncio
async def test_client_embed_returns_in_input_order() -> None:
    fake = _FakeHttp(
        {
            "data": [{"index": 1, "embedding": [1.0]}, {"index": 0, "embedding": [0.0]}],
            "usage": {"total_tokens": 2},
        }
    )
    vecs = await VoyageClient(api_key="k", client=fake).embed(["a", "b"], model="m")
    assert vecs == [[0.0], [1.0]]  # reordered by index


@pytest.mark.asyncio
async def test_reranker_maps_scores_to_input_order() -> None:
    fake = _FakeHttp(
        {
            "data": [
                {"index": 2, "relevance_score": 0.9},
                {"index": 0, "relevance_score": 0.1},
                {"index": 1, "relevance_score": 0.5},
            ],
            "usage": {"total_tokens": 9},
        }
    )
    reranker = VoyageReranker(VoyageClient(api_key="k", client=fake), model="rerank-2.5")
    scores = await reranker.rerank(query="q", facts=["a", "b", "c"])
    assert scores == [0.1, 0.5, 0.9]


@pytest.mark.asyncio
async def test_reranker_caches_successful_scores_by_query_and_facts() -> None:
    fake = _FakeHttp(
        {
            "data": [
                {"index": 0, "relevance_score": 0.1},
                {"index": 1, "relevance_score": 0.9},
            ],
            "usage": {"total_tokens": 4},
        }
    )
    reranker = VoyageReranker(VoyageClient(api_key="k", client=fake), model="rerank-2.5")

    first = await reranker.rerank(query="q", facts=["a", "b"])
    first[0] = 1.0
    second = await reranker.rerank(query="q", facts=["a", "b"])
    await reranker.rerank(query="q", facts=["b", "a"])

    assert second == [0.1, 0.9]
    assert len(fake.calls) == 2


@pytest.mark.asyncio
async def test_reranker_scopes_cached_scores_by_usage_ref() -> None:
    sink = _Sink()
    fake = _FakeHttp(
        {
            "data": [{"index": 0, "relevance_score": 0.9}],
            "usage": {"total_tokens": 4},
        }
    )
    reranker = VoyageReranker(
        VoyageClient(api_key="k", client=fake, usage_sink=sink), model="rerank-2.5"
    )

    for ref in ("run-1", "run-2"):
        token = set_usage_context(UsageContext(request_kind="search", ref=ref))
        try:
            await reranker.rerank(query="q", facts=["a"])
        finally:
            reset_usage_context(token)

    assert len(fake.calls) == 2
    assert [event.ref for event in sink.events] == ["run-1", "run-2"]


@pytest.mark.asyncio
async def test_reranker_coalesces_concurrent_requests_for_one_cache_key() -> None:
    sink = _Sink()
    fake = _BlockingHttp(
        {
            "data": [{"index": 0, "relevance_score": 0.9}],
            "usage": {"total_tokens": 4},
        }
    )
    reranker = VoyageReranker(
        VoyageClient(api_key="k", client=fake, usage_sink=sink), model="rerank-2.5"
    )
    token = set_usage_context(UsageContext(request_kind="search", ref="run-1"))
    try:
        async with asyncio.TaskGroup() as task_group:
            first = task_group.create_task(reranker.rerank(query="q", facts=["a"]))
            await fake.started.wait()
            second = task_group.create_task(reranker.rerank(query="q", facts=["a"]))
            await asyncio.sleep(0)
            fake.release.set()
    finally:
        reset_usage_context(token)

    assert first.result() == second.result() == [0.9]
    assert len(fake.calls) == 1
    assert len(sink.events) == 1


@pytest.mark.asyncio
async def test_reranker_waiter_retries_when_the_inflight_owner_is_cancelled() -> None:
    sink = _Sink()
    fake = _BlockingHttp(
        {
            "data": [{"index": 0, "relevance_score": 0.9}],
            "usage": {"total_tokens": 4},
        }
    )
    reranker = VoyageReranker(
        VoyageClient(api_key="k", client=fake, usage_sink=sink), model="rerank-2.5"
    )
    token = set_usage_context(UsageContext(request_kind="search", ref="run-1"))
    try:
        async with asyncio.TaskGroup() as task_group:
            owner = task_group.create_task(reranker.rerank(query="q", facts=["a"]))
            await fake.started.wait()
            waiter = task_group.create_task(reranker.rerank(query="q", facts=["a"]))
            await asyncio.sleep(0)
            owner.cancel()
            await fake.retried.wait()
            fake.release.set()
        cached = await reranker.rerank(query="q", facts=["a"])
    finally:
        reset_usage_context(token)

    assert owner.cancelled()
    assert waiter.result() == [0.9]
    assert cached == [0.9]
    assert len(fake.calls) == 2
    assert [event.cost_complete for event in sink.events] == [False, True]


@pytest.mark.asyncio
async def test_client_records_provider_reported_tokens() -> None:
    sink = _Sink()
    fake = _FakeHttp({"data": [{"index": 0, "embedding": [0.1]}], "usage": {"total_tokens": 7}})

    await VoyageClient(api_key="k", client=fake, usage_sink=sink).embed(["hello"], model="m")

    assert len(sink.events) == 1
    assert sink.events[0].prompt_tokens == 7
    assert sink.events[0].completion_tokens == 0


@pytest.mark.asyncio
async def test_custom_voyage_endpoint_does_not_inherit_official_pricing() -> None:
    sink = _Sink()
    fake = _FakeHttp(
        {
            "data": [{"index": 0, "embedding": [0.1]}],
            "usage": {"total_tokens": 7},
        }
    )

    await VoyageClient(
        api_key="k", base_url="https://compatible.test/v1", client=fake, usage_sink=sink
    ).embed(["hello"], model="voyage-4-lite")

    assert sink.events[0].cost_complete is False


@pytest.mark.asyncio
async def test_missing_voyage_usage_is_a_terminal_accounting_failure() -> None:
    fake = _FakeHttp({"data": [{"index": 0, "embedding": [0.1]}]})

    with pytest.raises(UsageAccountingError, match=r"usage\.total_tokens"):
        await VoyageClient(api_key="k", client=fake, usage_sink=_Sink()).embed(
            ["hello"], model="voyage-3.5"
        )

    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_non_object_voyage_response_is_a_terminal_accounting_failure() -> None:
    fake = _FakeHttp([{"index": 0, "embedding": [0.1]}])

    with pytest.raises(UsageAccountingError, match="non-object embedding"):
        await VoyageClient(api_key="k", client=fake, usage_sink=_Sink()).embed(
            ["hello"], model="voyage-3.5"
        )

    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_voyage_rejects_provider_model_substitution() -> None:
    fake = _FakeHttp(
        {
            "model": "substituted-model",
            "data": [{"index": 0, "embedding": [0.1]}],
            "usage": {"total_tokens": 7},
        }
    )

    with pytest.raises(UsageAccountingError, match="differs from reserved model"):
        await VoyageClient(api_key="k", client=fake, usage_sink=_Sink()).embed(
            ["hello"], model="voyage-3.5"
        )

    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_official_voyage_usage_settles_the_successful_call() -> None:
    class BudgetSink(_Sink):
        def __init__(self) -> None:
            super().__init__()
            self.reserved: list[tuple[str, float]] = []
            self.settled: list[tuple[str, float, float]] = []

        async def reserve_provider_budget(self, action_key: str, max_cost_usd: float) -> bool:
            self.reserved.append((action_key, max_cost_usd))
            return True

        async def settle_provider_budget(
            self, action_key: str, reserved_cost_usd: float, actual_cost_usd: float
        ) -> bool:
            self.settled.append((action_key, reserved_cost_usd, actual_cost_usd))
            return True

    sink = BudgetSink()
    fake = _FakeHttp(
        {
            "data": [{"index": 0, "embedding": [0.1]}],
            "usage": {"total_tokens": 7},
        }
    )
    token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        await VoyageClient(api_key="k", client=fake, usage_sink=sink).embed(
            ["hello"], model="voyage-3.5"
        )
    finally:
        reset_provider_budget_context(token)

    assert sink.events[0].cost_complete is True
    assert sink.reserved == [("run:case:step", sink.settled[0][1])]
    assert sink.settled[0][2] == sink.events[0].cost_usd


@pytest.mark.asyncio
async def test_reranker_empty_facts_is_empty() -> None:
    reranker = VoyageReranker(VoyageClient(api_key="k", client=_FakeHttp()), model="rerank-2.5")
    assert await reranker.rerank(query="q", facts=[]) == []


@pytest.mark.asyncio
async def test_reranker_error_reports_unavailable() -> None:
    fake = _FakeHttp(raise_exc=RuntimeError("boom"))
    sink = _Sink()
    reranker = VoyageReranker(
        VoyageClient(api_key="k", client=fake, usage_sink=sink), model="rerank-2.5"
    )
    with pytest.raises(RerankerUnavailableError):
        await reranker.rerank(query="q", facts=["a", "b"])
    assert len(sink.events) == 1
    assert sink.events[0].cost_complete is False


def test_active_embedding_honors_provider() -> None:
    base = get_settings()
    voyage = base.model_copy(
        update={
            "memory": MemorySettings(embedder="voyage"),
            "voyage": VoyageSettings(embedding_model="voyage-3.5", embedding_dim=1024),
        }
    )
    assert active_embedding(voyage) == ("voyage-3.5", 1024)
    openai = base.model_copy(
        update={"memory": MemorySettings(embedder="openai", embedding_dim=1536)}
    )
    assert active_embedding(openai) == ("text-embedding-3-small", 1536)


def test_graph_backend_default_is_neutral() -> None:
    # No backend is privileged: the default is neo4j, and falkordb is opt-in.
    from vera.config.settings import FalkorSettings, MemorySettings

    assert MemorySettings().graph_backend == "neo4j"
    assert FalkorSettings().port == 6379 and FalkorSettings().database == "default_db"


def test_voyage_api_key_treats_empty_as_none() -> None:
    base = get_settings()
    assert (
        voyage_api_key(base.model_copy(update={"voyage": VoyageSettings(api_key=SecretStr(""))}))
        is None
    )
    assert voyage_api_key(base.model_copy(update={"voyage": VoyageSettings(api_key=None)})) is None
    got = voyage_api_key(
        base.model_copy(update={"voyage": VoyageSettings(api_key=SecretStr("vk-123"))})
    )
    assert got == "vk-123"
