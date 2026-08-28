"""Offline, deterministic Graphiti components for local dev and tests.

`DeterministicEmbedder` maps text to a stable unit vector with no network call, so
ingestion and hybrid search run without an embedding provider. `NoLLMClient` stands
in when no LLM is configured: triple ingestion and RRF search never call it, and any
path that needs extraction fails loudly instead of hitting a provider by accident.
"""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Iterable
from typing import Any

from graphiti_core.cross_encoder.client import CrossEncoderClient
from graphiti_core.embedder.client import EmbedderClient
from graphiti_core.llm_client.client import LLMClient


class DeterministicEmbedder(EmbedderClient):
    def __init__(self, dim: int = 1024) -> None:
        self._dim = dim

    def _vector(self, text: str) -> list[float]:
        values: list[float] = []
        counter = 0
        while len(values) < self._dim:
            digest = hashlib.sha256(f"{counter}:{text}".encode()).digest()
            for offset in range(0, len(digest), 4):
                values.append(struct.unpack("<I", digest[offset : offset + 4])[0] / 2**32 - 0.5)
                if len(values) >= self._dim:
                    break
            counter += 1
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]

    async def create(
        self, input_data: str | list[str] | Iterable[int] | Iterable[Iterable[int]]
    ) -> list[float]:
        text = input_data if isinstance(input_data, str) else str(input_data)
        return self._vector(text)

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        return [self._vector(text) for text in input_data_list]


class NoLLMClient(LLMClient):
    """An LLM client that refuses to run. Use when only triple ingestion and RRF
    search are needed, so a missing provider fails fast rather than silently.
    """

    def __init__(self) -> None:
        super().__init__(config=None)

    async def _generate_response(
        self,
        messages: list[Any],
        response_model: type[Any] | None = None,
        max_tokens: int = 16384,
        model_size: Any = None,
    ) -> dict[str, Any]:
        raise RuntimeError(
            "No LLM configured. Only add_triplet ingestion and RRF search are available; "
            "set VERA_MEMORY__OPENAI_API_KEY to enable text extraction."
        )


class DeterministicCommunityLLM(LLMClient):
    """Offline LLM stub limited to Graphiti's community summary response shapes."""

    def __init__(self) -> None:
        super().__init__(config=None)

    async def _generate_response(
        self,
        messages: list[Any],
        response_model: type[Any] | None = None,
        max_tokens: int = 16384,
        model_size: Any = None,
    ) -> dict[str, Any]:
        fields = getattr(response_model, "model_fields", {})
        if "summary" in fields:
            return {"summary": "Derived summary of the projected community facts."}
        if "description" in fields:
            return {"description": "Projected fact community"}
        raise RuntimeError("DeterministicCommunityLLM only supports community summarization")


class NoCrossEncoder(CrossEncoderClient):
    """A cross-encoder that is never invoked. RRF search does not rerank with one; this
    stands in so Graphiti does not construct a provider-backed default that needs a key.
    """

    async def rank(self, query: str, passages: list[str]) -> list[tuple[str, float]]:
        raise RuntimeError("No cross-encoder configured; use an RRF search recipe.")
