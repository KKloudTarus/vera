"""LLM-backed claim extractor for free text.

Structured metadata (pre-extracted claims or triples) is used as-is. Free text is turned
into normalized (subject, predicate, object) triples by one LLM call, so text becomes
first-class verified memory: the claims flow through curation's trust tiers and land as
published episodes with provenance, while the graph still ingests deterministic triples.

Extraction runs once here, not again in the graph engine. Entity names are normalized to
canonical English so the same real entity does not fork across languages.
"""

from __future__ import annotations

import json
from typing import Any, cast

from openai import AsyncOpenAI

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.domain.knowledge.models import ClaimType
from vera.domain.ports.curation import ExtractedClaim
from vera.observability import get_logger
from vera.shared.types import JsonDict

log = get_logger(__name__)

_MAX_SUBJECT_LENGTH = 512
_MAX_PREDICATE_LENGTH = 256
_MAX_OBJECT_LENGTH = 512

_SYSTEM = (
    "You extract durable, structural knowledge about software systems, teams, and "
    "engineering decisions from text, as (subject, predicate, object) triples.\n"
    "Rules:\n"
    "- Only facts explicitly stated; never infer or invent.\n"
    "- Prefer relationships between named entities (services, environments, datastores, "
    "teams, people, repositories, incidents, decisions).\n"
    "- Skip transient or trivia: env-var names, secret names, badge/CI noise, version "
    "numbers, marketing lines, and how-to prose.\n"
    "- Normalize entity names to canonical English (translate non-English names, e.g. "
    "'Doi nen tang' -> 'platform team'); lower-case service/resource names.\n"
    "- Use UPPER_SNAKE_CASE predicates, preferring: RUNS_ON, DEPENDS_ON, OWNS, "
    "DEPLOYED_TO, MEMBER_OF, CAUSED, DECIDED_BY, HAS_STATUS.\n"
    "- Return subject_type and object_type from: Service, Environment, Team, Person, "
    "Repository, Datastore, Component, Incident, Decision. Use null object_type for a scalar.\n"
    "- Return explicit qualifier keys when stated, especially environment and region.\n"
    "- Copy an exact supporting quote from the supplied text for every triple.\n"
    "- Return quote_start and quote_end as Python-style character offsets into the supplied "
    "text, with quote_end exclusive.\n"
    "- Return at most 15 of the most important triples."
)

_SCHEMA_HINT = (
    'Respond as JSON: {"facts": [{"subject": str, "predicate": str, "object": str, '
    '"subject_type": str, "object_type": str|null, "qualifiers": object, '
    '"source_quote": str, "quote_start": int, "quote_end": int}]}. '
    "Keep subject and object within 512 characters and predicate within 256 characters. "
    "Return an empty list if there are no clear facts."
)


class LlmClaimExtractor:
    def __init__(
        self,
        *,
        model: str,
        api_key: str | None = None,
        base_url: str | None = None,
        client: Any = None,
    ) -> None:
        # A client can be injected for tests; otherwise one is built from the key.
        self._client = (
            client if client is not None else AsyncOpenAI(api_key=api_key, base_url=base_url)
        )
        self._model = model
        self._structured = StructuredClaimExtractor()

    @property
    def provider(self) -> str:
        return "openai-compatible"

    @property
    def model(self) -> str:
        return self._model

    async def extract(
        self, *, body: str, knowledge_type: str, metadata: JsonDict
    ) -> list[ExtractedClaim]:
        structured = await self._structured.extract(
            body=body, knowledge_type=knowledge_type, metadata=metadata
        )
        # Prefer explicit structured input; only reach for the LLM on real free text.
        if structured or not body.strip():
            return structured
        return await self._extract_from_text(body)

    async def _extract_from_text(self, body: str) -> list[ExtractedClaim]:
        response = await self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": f"{_SYSTEM}\n{_SCHEMA_HINT}"},
                {"role": "user", "content": body},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        content = response.choices[0].message.content or "{}"
        try:
            parsed: dict[str, Any] = json.loads(content)
        except json.JSONDecodeError:
            log.warning("llm_extractor.bad_json")
            return []

        claims: list[ExtractedClaim] = []
        for fact in parsed.get("facts", []):
            subject = str(fact.get("subject", "")).strip()
            predicate = str(fact.get("predicate", "")).strip()
            obj = str(fact.get("object", "")).strip()
            source_quote = fact.get("source_quote")
            quote_start = fact.get("quote_start")
            quote_end = fact.get("quote_end")
            qualifiers_value = fact.get("qualifiers")
            qualifiers = (
                cast("JsonDict", qualifiers_value) if isinstance(qualifiers_value, dict) else {}
            )
            if not (subject and predicate and obj):
                continue
            if (
                len(subject) > _MAX_SUBJECT_LENGTH
                or len(predicate) > _MAX_PREDICATE_LENGTH
                or len(obj) > _MAX_OBJECT_LENGTH
            ):
                log.warning(
                    "llm_extractor.oversized_fact",
                    subject_length=len(subject),
                    predicate_length=len(predicate),
                    object_length=len(obj),
                )
                continue
            claims.append(
                ExtractedClaim(
                    statement=f"{subject} {predicate} {obj}",
                    claim_type=ClaimType.FACT,
                    subject=subject,
                    predicate=predicate,
                    object=obj,
                    subject_entity_type=(
                        str(fact["subject_type"]) if fact.get("subject_type") else None
                    ),
                    object_entity_type=(
                        str(fact["object_type"]) if fact.get("object_type") else None
                    ),
                    qualifiers=qualifiers,
                    confidence=0.7,
                    source_quote=source_quote if isinstance(source_quote, str) else None,
                    quote_start=(
                        quote_start
                        if isinstance(quote_start, int) and not isinstance(quote_start, bool)
                        else None
                    ),
                    quote_end=(
                        quote_end
                        if isinstance(quote_end, int) and not isinstance(quote_end, bool)
                        else None
                    ),
                )
            )
        return claims
