"""The ``Embedder`` port: text to a dense vector, for semantic entity linking."""

from __future__ import annotations

from typing import Protocol


class Embedder(Protocol):
    async def embed(self, text: str) -> list[float]:
        """Return the embedding vector for ``text``."""
        ...
