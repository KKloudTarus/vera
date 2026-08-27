"""An embedder wrapper that caches vectors by content.

Embeddings are a pure function of (model, text), so caching by a text hash is safe.
The query embedding on the read hot path is a paid, latency-adding call; caching it
(and repeated ingestion text) cuts cost and latency. This is an in-process LRU with a
TTL; a Valkey L2 can sit behind the same interface later.
"""

from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from collections.abc import Iterable

from graphiti_core.embedder.client import EmbedderClient


class CachingEmbedder(EmbedderClient):
    def __init__(
        self, inner: EmbedderClient, *, maxsize: int = 4096, ttl_s: float = 86400.0
    ) -> None:
        self._inner = inner
        self._maxsize = maxsize
        self._ttl_s = ttl_s
        self._cache: OrderedDict[str, tuple[float, list[float]]] = OrderedDict()

    @staticmethod
    def _key(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _get(self, key: str, now: float) -> list[float] | None:
        item = self._cache.get(key)
        if item is None:
            return None
        stored_at, vector = item
        if now - stored_at > self._ttl_s:
            del self._cache[key]
            return None
        self._cache.move_to_end(key)
        return vector

    def _put(self, key: str, vector: list[float], now: float) -> None:
        self._cache[key] = (now, vector)
        self._cache.move_to_end(key)
        while len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)

    async def create(
        self, input_data: str | list[str] | Iterable[int] | Iterable[Iterable[int]]
    ) -> list[float]:
        text = input_data if isinstance(input_data, str) else str(input_data)
        now = time.monotonic()
        cached = self._get(self._key(text), now)
        if cached is not None:
            return cached
        vector = await self._inner.create(input_data)
        self._put(self._key(text), vector, now)
        return vector

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        now = time.monotonic()
        results: list[list[float] | None] = []
        missing: list[str] = []
        missing_index: list[int] = []
        for index, text in enumerate(input_data_list):
            cached = self._get(self._key(text), now)
            results.append(cached)
            if cached is None:
                missing.append(text)
                missing_index.append(index)
        if missing:
            fresh = await self._inner.create_batch(missing)
            for index, text, vector in zip(missing_index, missing, fresh, strict=True):
                self._put(self._key(text), vector, now)
                results[index] = vector
        return [vector for vector in results if vector is not None]
