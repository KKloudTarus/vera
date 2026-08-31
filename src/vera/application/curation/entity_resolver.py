"""SemanticEntityResolver: resolve a surface name to a canonical entity.

Order: exact-normalized in the repository, then, on a miss, an optional semantic step.
Embedding cosine over bare names is only a candidate generator because short sibling names
can score as highly as true synonyms. An equivalence judge must confirm every semantic link.
The semantic step is off unless enabled with an embedder, and runs only on a miss, so the hot
path rarely pays for it.
"""

from __future__ import annotations

import math
import re

from vera.domain.knowledge.models import CanonicalEntity
from vera.domain.ports.curation import EntityResolutionJudge
from vera.domain.ports.embedder import Embedder
from vera.domain.ports.repositories import CanonicalEntityRepository
from vera.observability import get_logger
from vera.observability.metrics import record_entity_resolution
from vera.shared.text import normalize_name

log = get_logger(__name__)
_ACRONYM_TOKEN = re.compile(r"^[A-Z0-9]{2,8}$")


def _identifier_aliases(name: str) -> list[str]:
    """Return compact aliases for names containing an acronym-like identifier token."""
    tokens = name.split()
    if len(tokens) < 2 or not any(_ACRONYM_TOKEN.fullmatch(token) for token in tokens):
        return []
    normalized = normalize_name(name)
    compact = normalized.replace(" ", "")
    return [compact] if compact != normalized else []


def cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


class SemanticEntityResolver:
    def __init__(
        self,
        embedder: Embedder | None,
        *,
        threshold: float = 0.86,
        block_threshold: float = 0.55,
        max_candidates: int = 5,
        enabled: bool = False,
        judge: EntityResolutionJudge | None = None,
    ) -> None:
        self._embedder = embedder
        self._threshold = threshold
        self._block_threshold = block_threshold
        self._max_candidates = max_candidates
        self._enabled = enabled and embedder is not None
        self._judge = judge
        # Surface a misconfiguration instead of silently degrading dedup quality: semantic
        # linking asked for but inert without an embedder, or reduced to cosine-only (missing
        # synonyms, abbreviations, and cross-lingual merges) without the equivalence judge.
        if enabled and embedder is None:
            log.warning("entity_resolver.semantic_enabled_without_embedder")
        elif self._enabled and judge is None:
            log.warning("entity_resolver.semantic_enabled_without_judge")

    async def resolve_or_create(
        self,
        repo: CanonicalEntityRepository,
        *,
        group_id: str,
        name: str,
        entity_type: str,
    ) -> CanonicalEntity:
        existing = await repo.resolve(group_id=group_id, name=name)
        if existing is not None:
            return existing
        identifier_aliases = _identifier_aliases(name)
        for alias in identifier_aliases:
            existing = await repo.resolve(group_id=group_id, name=alias)
            if existing is not None:
                return existing

        embedding: list[float] | None = None
        if self._enabled and self._embedder is not None:
            embedding = await self._embedder.embed(name)
            candidates = await repo.candidates_with_embeddings(
                group_id=group_id, entity_type=entity_type
            )
            scored = sorted(
                ((cosine(embedding, vector), entity) for entity, vector in candidates),
                key=lambda pair: pair[0],
                reverse=True,
            )
            match = await self._match(scored, name=name, entity_type=entity_type)
            if match is not None:
                await repo.add_alias(entity_id=match.id, group_id=group_id, alias=name)
                return match
            record_entity_resolution("created")

        return await repo.create(
            group_id=group_id,
            entity_type=entity_type,
            canonical_name=name,
            aliases=identifier_aliases,
            embedding=embedding,
        )

    async def _match(
        self,
        scored: list[tuple[float, CanonicalEntity]],
        *,
        name: str,
        entity_type: str,
    ) -> CanonicalEntity | None:
        if not scored:
            return None
        best_score = scored[0][0]
        if self._judge is None:
            return None
        cutoff = self._threshold if best_score >= self._threshold else self._block_threshold
        blocked = [entity for score, entity in scored if score >= cutoff]
        blocked = blocked[: self._max_candidates]
        if not blocked:
            return None
        by_name = {entity.canonical_name: entity for entity in blocked}
        chosen = await self._judge.same_entity(
            name=name, entity_type=entity_type, candidates=list(by_name)
        )
        if chosen is not None and chosen in by_name:
            record_entity_resolution("linked_judge")
            return by_name[chosen]
        return None
