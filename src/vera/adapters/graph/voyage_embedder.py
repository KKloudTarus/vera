"""Adapt the Voyage client to Graphiti's ``EmbedderClient`` for the graph path.

Graphiti embeds nodes and queries through its own ``EmbedderClient``; this lets the graph
use Voyage without pulling in the ``voyageai`` SDK (the shared httpx client is enough).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import cast

from graphiti_core.embedder.client import EmbedderClient

from vera.adapters.embedding.voyage import VoyageClient


def _as_text(input_data: object) -> str:
    if isinstance(input_data, str):
        return input_data
    if isinstance(input_data, Iterable):
        # token-id inputs are not used here; join whatever parts a caller passes
        return " ".join(str(part) for part in cast("Iterable[object]", input_data))
    return str(input_data)


class GraphitiVoyageEmbedder(EmbedderClient):
    def __init__(self, client: VoyageClient, *, model: str, dim: int) -> None:
        self._client = client
        self._model = model
        self._dim = dim

    async def create(self, input_data: object) -> list[float]:
        vectors = await self._client.embed([_as_text(input_data)], model=self._model, dim=self._dim)
        return vectors[0]

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        if not input_data_list:
            return []
        return await self._client.embed(list(input_data_list), model=self._model, dim=self._dim)
