"""Voyage AI adapters: embeddings and reranking over the HTTP API.

Voyage is a third-party provider like OpenAI, reached through the ``Embedder`` and
``Reranker`` ports, so it is a swappable choice, not a lock-in. One thin httpx client backs
both, plus a Graphiti embedder for the graph path (see adapters/graph/voyage_embedder.py).
The models are configurable (voyage-3.5 / voyage-4-lite / voyage-code-4; rerank-2.5 /
rerank-2.5-lite). A reranker failure degrades to a neutral score rather than failing search.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from vera.observability import get_logger
from vera.shared.errors import VeraError

log = get_logger(__name__)


class VoyageClient:
    """Minimal async client for the Voyage embeddings and rerank endpoints."""

    def __init__(
        self,
        *,
        api_key: str | None,
        base_url: str = "https://api.voyageai.com/v1",
        client: Any = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._client = client  # injectable for tests; otherwise built lazily

    def _http(self) -> Any:
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers={"Authorization": f"Bearer {self._api_key}"},
                timeout=30.0,
            )
        return self._client

    async def embed(
        self,
        texts: Sequence[str],
        *,
        model: str,
        dim: int | None = None,
        input_type: str | None = None,
    ) -> list[list[float]]:
        body: dict[str, Any] = {"input": list(texts), "model": model}
        if dim is not None:
            body["output_dimension"] = dim
        if input_type is not None:
            body["input_type"] = input_type
        response = await self._http().post("/embeddings", json=body)
        response.raise_for_status()
        data = response.json()["data"]
        # Return in input order regardless of the response ordering.
        ordered = sorted(data, key=lambda item: int(item["index"]))
        return [[float(x) for x in item["embedding"]] for item in ordered]

    async def rerank(self, query: str, documents: Sequence[str], *, model: str) -> list[float]:
        docs = list(documents)
        if not docs:
            return []
        body = {
            "query": query,
            "documents": docs,
            "model": model,
            "return_documents": False,
            "truncation": True,
        }
        response = await self._http().post("/rerank", json=body)
        response.raise_for_status()
        results = response.json()["data"]
        scores = [0.0] * len(docs)
        for item in results:
            index = int(item["index"])
            if 0 <= index < len(docs):
                scores[index] = float(item["relevance_score"])
        return scores


class VoyageEmbedder:
    """The ``Embedder`` port backed by Voyage (used for canonical-entity linking)."""

    def __init__(self, client: VoyageClient, *, model: str, dim: int) -> None:
        self._client = client
        self._model = model
        self._dim = dim

    async def embed(self, text: str) -> list[float]:
        vectors = await self._client.embed(
            [text], model=self._model, dim=self._dim, input_type="document"
        )
        if not vectors:
            raise VeraError("voyage returned no embedding")
        return vectors[0]


class VoyageReranker:
    """The ``Reranker`` port backed by a Voyage reranker (stage-3 cross-encoder)."""

    def __init__(self, client: VoyageClient, *, model: str) -> None:
        self._client = client
        self._model = model

    async def rerank(self, *, query: str, facts: Sequence[str]) -> list[float]:
        if not facts:
            return []
        try:
            return await self._client.rerank(query, list(facts), model=self._model)
        except Exception:  # a reranker error must not fail the search; fall back to neutral
            log.warning("voyage_reranker.failed")
            return [0.5] * len(facts)
