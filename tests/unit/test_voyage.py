"""Voyage adapters: embeddings and reranking over a fake HTTP client (no network)."""

from __future__ import annotations

from typing import Any

import pytest

from vera.adapters.embedding.voyage import VoyageClient, VoyageEmbedder, VoyageReranker
from vera.config.settings import (
    MemorySettings,
    VoyageSettings,
    active_embedding,
    get_settings,
    voyage_api_key,
)


class _Resp:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeHttp:
    def __init__(
        self, payload: dict[str, Any] | None = None, raise_exc: Exception | None = None
    ) -> None:
        self._payload = payload or {}
        self._raise = raise_exc
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def post(self, url: str, json: dict[str, Any]) -> _Resp:
        self.calls.append((url, json))
        if self._raise is not None:
            raise self._raise
        return _Resp(self._payload)


@pytest.mark.asyncio
async def test_embedder_sends_model_and_dim_and_returns_vector() -> None:
    fake = _FakeHttp({"data": [{"index": 0, "embedding": [0.1, 0.2, 0.3]}]})
    embedder = VoyageEmbedder(VoyageClient(api_key="k", client=fake), model="voyage-3.5", dim=1024)
    vec = await embedder.embed("hello")
    assert vec == [0.1, 0.2, 0.3]
    url, body = fake.calls[0]
    assert url == "/embeddings"
    assert body["model"] == "voyage-3.5" and body["output_dimension"] == 1024
    assert body["input"] == ["hello"]


@pytest.mark.asyncio
async def test_client_embed_returns_in_input_order() -> None:
    fake = _FakeHttp({"data": [{"index": 1, "embedding": [1.0]}, {"index": 0, "embedding": [0.0]}]})
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
            ]
        }
    )
    reranker = VoyageReranker(VoyageClient(api_key="k", client=fake), model="rerank-2.5")
    scores = await reranker.rerank(query="q", facts=["a", "b", "c"])
    assert scores == [0.1, 0.5, 0.9]


@pytest.mark.asyncio
async def test_reranker_empty_facts_is_empty() -> None:
    reranker = VoyageReranker(VoyageClient(api_key="k", client=_FakeHttp()), model="rerank-2.5")
    assert await reranker.rerank(query="q", facts=[]) == []


@pytest.mark.asyncio
async def test_reranker_error_falls_back_to_neutral() -> None:
    fake = _FakeHttp(raise_exc=RuntimeError("boom"))
    reranker = VoyageReranker(VoyageClient(api_key="k", client=fake), model="rerank-2.5")
    scores = await reranker.rerank(query="q", facts=["a", "b"])
    assert scores == [0.5, 0.5]  # a reranker failure must not fail search


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


def test_voyage_api_key_treats_empty_as_none() -> None:
    base = get_settings()
    assert voyage_api_key(base.model_copy(update={"voyage": VoyageSettings(api_key="")})) is None
    assert voyage_api_key(base.model_copy(update={"voyage": VoyageSettings(api_key=None)})) is None
    got = voyage_api_key(base.model_copy(update={"voyage": VoyageSettings(api_key="vk-123")}))
    assert got == "vk-123"
