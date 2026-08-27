"""A Graphiti embedder wrapped in the resilience policy.

Every provider call passes the rate limiter, runs under a per-call deadline, is retried
with backoff, and is guarded by the circuit breaker. Used only for network-backed
embedders; the offline embedder needs none of this.
"""

from __future__ import annotations

from collections.abc import Iterable

from graphiti_core.embedder.client import EmbedderClient

from vera.adapters.resilience.policy import ResiliencePolicy
from vera.observability.cost import estimate_tokens


class ResilientEmbedder(EmbedderClient):
    def __init__(self, inner: EmbedderClient, policy: ResiliencePolicy) -> None:
        self._inner = inner
        self._policy = policy

    async def create(
        self, input_data: str | list[str] | Iterable[int] | Iterable[Iterable[int]]
    ) -> list[float]:
        text = input_data if isinstance(input_data, str) else str(input_data)
        return await self._policy.call(
            lambda: self._inner.create(input_data), tokens=estimate_tokens(text)
        )

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        tokens = sum(estimate_tokens(text) for text in input_data_list)
        return await self._policy.call(
            lambda: self._inner.create_batch(input_data_list), tokens=tokens
        )
