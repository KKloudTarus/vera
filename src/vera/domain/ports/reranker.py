"""The ``Reranker`` port: a stage-3 cross-encoder over the reranked head.

Stage 1 (graph hybrid RRF) and stage 2 (VERA's weighted blend) rank by cheap signals.
A cross-encoder reads the query and each candidate fact together and scores their direct
relevance, which catches head cases the bag-of-signals blend cannot. It runs only on the
top handful of candidates, so its per-pair cost stays bounded.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol


class RerankerUnavailableError(Exception):
    """The configured reranker could not produce trustworthy scores."""


class Reranker(Protocol):
    async def rerank(self, *, query: str, facts: Sequence[str]) -> list[float]:
        """A relevance score in [0, 1] for each fact against the query, in input order."""
        ...
