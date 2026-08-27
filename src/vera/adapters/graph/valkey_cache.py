"""Valkey-backed L2 embedding cache, shared across replicas.

Embeddings are a pure function of (model, text), so they can be cached anywhere. This L2
sits behind the in-process L1: a cache hit on any replica saves the paid provider call
for all of them. Vectors are stored as JSON with a TTL; a miss or a Valkey hiccup simply
falls through to computing the embedding.
"""

from __future__ import annotations

import json
from typing import Any


class ValkeyEmbeddingCache:
    def __init__(self, client: Any, *, prefix: str = "vera:emb", ttl_s: int = 604800) -> None:
        self._client = client
        self._prefix = prefix
        self._ttl_s = ttl_s

    def _redis_key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def get(self, key: str) -> list[float] | None:
        try:
            raw = await self._client.get(self._redis_key(key))
        except Exception:
            return None  # a cache outage must never fail a request
        if raw is None:
            return None
        try:
            return [float(x) for x in json.loads(raw)]
        except (ValueError, TypeError):
            return None

    async def set(self, key: str, vector: list[float]) -> None:
        try:
            await self._client.set(self._redis_key(key), json.dumps(vector), ex=self._ttl_s)
        except Exception:
            return  # best-effort; never fail the caller on a cache write
