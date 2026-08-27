"""LLM contradiction judge for non-functional predicates.

For a functional predicate (RUNS_ON) any different value is a contradiction and needs no
LLM. For a multi-valued predicate (DEPENDS_ON) a different value is usually fine, but
sometimes it is a genuine replacement ("depends on postgres" then "migrated off postgres
to valkey"). This judge asks the model which of the existing values the new fact actually
contradicts, so those (and only those) are superseded.
"""

from __future__ import annotations

import json
from typing import Any

from openai import AsyncOpenAI

from vera.observability import get_logger

log = get_logger(__name__)

_SYSTEM = (
    "You decide which existing facts a new fact contradicts. Given a subject, a predicate, "
    "a new object, and a list of existing objects for the same subject and predicate, "
    "return the existing objects that the new fact makes no longer true. Return an empty "
    "list if the new fact simply adds another value that can coexist. Only judge direct "
    'contradiction. Respond as JSON: {"contradicted": [existing_object, ...]}.'
)


class LlmContradictionJudge:
    def __init__(self, *, api_key: str | None = None, model: str, client: Any = None) -> None:
        self._client = client if client is not None else AsyncOpenAI(api_key=api_key)
        self._model = model

    async def contradictions(
        self, *, subject: str, predicate: str, new_object: str, existing_objects: list[str]
    ) -> set[str]:
        if not existing_objects:
            return set()
        user = json.dumps(
            {
                "subject": subject,
                "predicate": predicate,
                "new_object": new_object,
                "existing_objects": existing_objects,
            }
        )
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
            log.warning("contradiction_judge.bad_json")
            return set()
        contradicted = {str(o) for o in parsed.get("contradicted", [])}
        # Only trust values the model was actually given.
        return contradicted & set(existing_objects)
