"""SemanticEntityResolver: resolve a surface name to a canonical entity.

Order: exact-normalized, then pg_trgm fuzzy (both in the repository), then, on a miss, an
optional semantic step. Embedding cosine over bare names is a weak signal (short names put
sibling services as close as true synonyms, and translations far apart), so it is used two
ways: a high-similarity match links straight away, and otherwise it blocks a small
candidate set that an LLM equivalence judge confirms. The semantic step is off unless
enabled with an embedder, and runs only on a miss, so the hot path rarely pays for it.
"""

from __future__ import annotations

import math

from vera.domain.knowledge.models import CanonicalEntity
from vera.domain.ports.curation import EntityResolutionJudge
from vera.domain.ports.embedder import Embedder
from vera.domain.ports.repositories import CanonicalEntityRepository


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

        return await repo.create(
            group_id=group_id,
            entity_type=entity_type,
            canonical_name=name,
            aliases=[],
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
        # A very close name is a synonym/spelling variant: link without spending an LLM call.
        best_score, best = scored[0]
        if best_score >= self._threshold:
            return best
        if self._judge is None:
            return None
        # Otherwise let the judge confirm one of the blocked (plausibly similar) candidates.
        blocked = [entity for score, entity in scored if score >= self._block_threshold]
        blocked = blocked[: self._max_candidates]
        if not blocked:
            return None
        by_name = {entity.canonical_name: entity for entity in blocked}
        chosen = await self._judge.same_entity(
            name=name, entity_type=entity_type, candidates=list(by_name)
        )
        return by_name.get(chosen) if chosen is not None else None
