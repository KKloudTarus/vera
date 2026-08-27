"""The caching embedder returns cached vectors and calls the inner embedder once."""

from __future__ import annotations

from collections.abc import Iterable

import pytest

from vera.adapters.graph.caching import CachingEmbedder


class _CountingEmbedder:
    def __init__(self) -> None:
        self.create_calls = 0
        self.batch_calls = 0

    async def create(
        self, input_data: str | list[str] | Iterable[int] | Iterable[Iterable[int]]
    ) -> list[float]:
        self.create_calls += 1
        return [0.1, 0.2, 0.3]

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        self.batch_calls += 1
        return [[float(len(text))] for text in input_data_list]


@pytest.mark.asyncio
async def test_create_is_cached_by_text() -> None:
    inner = _CountingEmbedder()
    embedder = CachingEmbedder(inner)

    first = await embedder.create("payment api")
    second = await embedder.create("payment api")

    assert first == second
    assert inner.create_calls == 1


@pytest.mark.asyncio
async def test_batch_only_fetches_missing() -> None:
    inner = _CountingEmbedder()
    embedder = CachingEmbedder(inner)

    await embedder.create("alpha")  # warms the cache for "alpha"
    inner.create_calls = 0

    results = await embedder.create_batch(["alpha", "beta"])

    assert len(results) == 2
    assert inner.batch_calls == 1  # one batch call, for the single missing item
