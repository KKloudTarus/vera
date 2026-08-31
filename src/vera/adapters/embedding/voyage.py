"""Voyage AI adapters: embeddings and reranking over the HTTP API.

Voyage is reached through the ``Embedder`` and ``Reranker`` ports. One thin httpx
client backs both, plus a Graphiti embedder for the graph path. Models are
configurable. Reranker failures make the semantic branch unavailable while lexical
retrieval remains available.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Sequence
from typing import Any, cast

from vera.domain.ports.reranker import RerankerUnavailableError
from vera.observability import get_logger
from vera.observability.cost import UsageSink, build_usage_event, current_usage_context, emit_usage
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
        usage_sink: UsageSink | None = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._client = client  # injectable for tests; otherwise built lazily
        self._usage_sink = usage_sink

    def _http(self) -> Any:
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers={"Authorization": f"Bearer {self._api_key}"},
                timeout=30.0,
            )
        return self._client

    async def _meter(self, payload: dict[str, Any], *, model: str, operation: str) -> None:
        usage: object = payload.get("usage")
        total_tokens: object = (
            cast("dict[str, object]", usage).get("total_tokens")
            if isinstance(usage, dict)
            else None
        )
        if isinstance(total_tokens, bool) or not isinstance(total_tokens, int) or total_tokens < 0:
            raise VeraError("voyage response omitted usage.total_tokens")
        await emit_usage(
            self._usage_sink,
            build_usage_event(
                model=model,
                operation=operation,
                prompt_tokens=total_tokens,
                completion_tokens=0,
            ),
        )

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
        payload = response.json()
        await self._meter(payload, model=model, operation="embedding")
        data = payload["data"]
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
        payload = response.json()
        await self._meter(payload, model=model, operation="llm")
        results = payload["data"]
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
        self._cache: OrderedDict[
            tuple[str | None, str, tuple[str, ...]], tuple[float, tuple[float, ...]]
        ] = OrderedDict()
        self._inflight: dict[
            tuple[str | None, str, tuple[str, ...]], asyncio.Future[tuple[float, ...]]
        ] = {}

    async def rerank(self, *, query: str, facts: Sequence[str]) -> list[float]:
        if not facts:
            return []
        usage = current_usage_context()
        key = (usage.ref if usage is not None else None, query, tuple(facts))
        cached = self._cache.get(key)
        now = time.monotonic()
        if cached is not None:
            stored_at, scores = cached
            if now - stored_at <= 86400.0:
                self._cache.move_to_end(key)
                return list(scores)
            del self._cache[key]
        inflight = self._inflight.get(key)
        if inflight is not None:
            try:
                return list(await asyncio.shield(inflight))
            except asyncio.CancelledError:
                task = asyncio.current_task()
                if task is None or task.cancelling():
                    raise
                return await self.rerank(query=query, facts=facts)
        future = asyncio.get_running_loop().create_future()
        self._inflight[key] = future
        try:
            try:
                scores = await self._client.rerank(query, list(facts), model=self._model)
            except Exception as exc:
                log.warning("voyage_reranker.failed")
                raise RerankerUnavailableError("voyage reranker failed") from exc
            stored_scores = tuple(scores)
            self._cache[key] = (time.monotonic(), stored_scores)
            self._cache.move_to_end(key)
            while len(self._cache) > 4096:
                self._cache.popitem(last=False)
            future.set_result(stored_scores)
            return scores
        except asyncio.CancelledError:
            future.cancel()
            raise
        except Exception as exc:
            future.set_exception(exc)
            future.exception()
            raise
        finally:
            self._inflight.pop(key, None)
