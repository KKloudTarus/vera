"""SemanticEntityResolver: resolve a surface name to a canonical entity.

Order: exact-normalized, then pg_trgm fuzzy (both in the repository), then an optional
embedding-similarity step that links synonyms and cross-lingual names ("payment service"
to "paymentapi") to the same entity. Semantic linking is off unless enabled and an
embedder is available, and only runs on a miss, so the hot path pays for it rarely.
"""

from __future__ import annotations

import math

from vera.domain.knowledge.models import CanonicalEntity
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
        self, embedder: Embedder | None, *, threshold: float = 0.86, enabled: bool = False
    ) -> None:
        self._embedder = embedder
        self._threshold = threshold
        self._enabled = enabled and embedder is not None

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
            best: CanonicalEntity | None = None
            best_sim = 0.0
            for entity, vector in candidates:
                sim = cosine(embedding, vector)
                if sim > best_sim:
                    best, best_sim = entity, sim
            if best is not None and best_sim >= self._threshold:
                # A synonym/translation of a known entity: link, do not duplicate.
                await repo.add_alias(entity_id=best.id, group_id=group_id, alias=name)
                return best

        return await repo.create(
            group_id=group_id,
            entity_type=entity_type,
            canonical_name=name,
            aliases=[],
            embedding=embedding,
        )
