"""LLM entity-resolution judge for the semantic dedup candidate set.

Embedding cosine over bare canonical names is only a candidate generator: on short names
it scores sibling entities ("payment service" and "billing service") as high as true
synonyms, and cross-lingual names far too low. This judge takes the blocked candidates and
decides which, if any, is the same real-world entity, resolving synonyms, abbreviations,
and translations the embedding step cannot separate on its own.
"""

from __future__ import annotations

import json
from typing import Any

from openai import AsyncOpenAI

from vera.observability import get_logger

log = get_logger(__name__)

_SYSTEM = (
    "You decide whether a new entity name refers to the SAME real-world entity as one of a "
    "list of existing names of the same type: the same specific system or thing, not merely "
    "another one in the same category. Treat synonyms, abbreviations, spelling variants, and "
    "names in other languages for the same thing as the same entity. Do NOT match two distinct "
    "siblings that only share a domain word (for example 'payment service' and 'billing "
    "service' are different services, so the answer is null). Return the single existing name "
    'that denotes the same entity, or null if none does. Respond as JSON: {"match": '
    "existing_name_or_null}."
)


class LlmEntityResolutionJudge:
    def __init__(self, *, api_key: str | None = None, model: str, client: Any = None) -> None:
        self._client = client if client is not None else AsyncOpenAI(api_key=api_key)
        self._model = model

    async def same_entity(
        self, *, name: str, entity_type: str, candidates: list[str]
    ) -> str | None:
        if not candidates:
            return None
        user = json.dumps({"name": name, "entity_type": entity_type, "candidates": candidates})
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
        except json.JSONDecodeError:
            log.warning("entity_resolution_judge.bad_json")
            return None
        match = parsed.get("match")
        # Only trust a name the model was actually given.
        return str(match) if match in candidates else None
