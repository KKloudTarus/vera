"""LLM cross-encoder: score query-fact relevance for stage-3 reranking.

One call scores the whole head: the model sees the query and the numbered candidate facts
and returns a relevance score in [0, 1] for each. A short, bounded head keeps the cost and
latency low. On any parse problem it returns a neutral 0.5 for every fact, so a bad
response degrades to the stage-2 order rather than failing the search.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from openai import AsyncOpenAI

from vera.observability import get_logger

log = get_logger(__name__)

_SYSTEM = (
    "You score how well each candidate fact answers the query. For every fact return a "
    "relevance score between 0.0 (irrelevant) and 1.0 (directly answers it). Respond as "
    'JSON: {"scores": [number, ...]} with one score per fact, in the given order.'
)


class LlmReranker:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str,
        client: Any = None,
    ) -> None:
        self._client = (
            client if client is not None else AsyncOpenAI(api_key=api_key, base_url=base_url)
        )
        self._model = model

    async def rerank(self, *, query: str, facts: Sequence[str]) -> list[float]:
        if not facts:
            return []
        neutral = [0.5] * len(facts)
        user = json.dumps({"query": query, "facts": list(facts)})
        response = await self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        content = response.choices[0].message.content or "{}"
        try:
            parsed: dict[str, Any] = json.loads(content)
            scores = [float(s) for s in parsed["scores"]]
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            log.warning("reranker.bad_response")
            return neutral
        if len(scores) != len(facts):
            log.warning("reranker.length_mismatch", got=len(scores), want=len(facts))
            return neutral
        return [min(1.0, max(0.0, s)) for s in scores]
