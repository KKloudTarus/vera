"""Adapt a Graphiti ``EmbedderClient`` to VERA's ``Embedder`` port.

Lets non-graph code (canonical entity linking) reuse the same embedding model and cache
as ingestion, so vectors live in one space.
"""

from __future__ import annotations

from graphiti_core.embedder.client import EmbedderClient


class GraphitiEmbedderAdapter:
    def __init__(self, inner: EmbedderClient) -> None:
        self._inner = inner

    async def embed(self, text: str) -> list[float]:
        return await self._inner.create(text)
