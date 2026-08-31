"""SemanticEntityResolver links synonyms/cross-lingual names by embedding similarity."""

from __future__ import annotations

import pytest

from vera.application.curation.entity_resolver import SemanticEntityResolver, cosine
from vera.domain.knowledge.models import CanonicalEntity
from vera.shared.ids import uuid7


class _FakeEmbedder:
    def __init__(self, vectors: dict[str, list[float]]) -> None:
        self._vectors = vectors

    async def embed(self, text: str) -> list[float]:
        return self._vectors[text]


class _FakeRepo:
    """In-memory canonical repo satisfying the port used by the resolver."""

    def __init__(self) -> None:
        self._by_alias: dict[str, CanonicalEntity] = {}
        self._embeddings: list[tuple[CanonicalEntity, list[float]]] = []

    async def resolve(self, *, group_id: str, name: str) -> CanonicalEntity | None:
        return self._by_alias.get(name.lower())

    async def create(
        self,
        *,
        group_id: str,
        entity_type: str,
        canonical_name: str,
        aliases: list[str],
        embedding: list[float] | None = None,
    ) -> CanonicalEntity:
        entity = CanonicalEntity(
            id=uuid7(), group_id=group_id, entity_type=entity_type, canonical_name=canonical_name
        )
        self._by_alias[canonical_name.lower()] = entity
        if embedding is not None:
            self._embeddings.append((entity, embedding))
        return entity

    async def add_alias(self, *, entity_id, group_id: str, alias: str) -> None:
        for entity, _ in self._embeddings:
            if entity.id == entity_id:
                self._by_alias[alias.lower()] = entity

    async def candidates_with_embeddings(
        self, *, group_id: str, entity_type: str
    ) -> list[tuple[CanonicalEntity, list[float]]]:
        return list(self._embeddings)


_VECTORS = {
    "paymentapi": [1.0, 0.0, 0.0],
    "payment service": [0.95, 0.31, 0.0],  # ~0.95 cosine with paymentapi
    "dich vu thanh toan": [0.9, 0.44, 0.0],  # cross-lingual, ~0.9
    "billing": [0.0, 1.0, 0.0],  # unrelated
}


def test_cosine_basic() -> None:
    assert cosine([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
    assert cosine([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_semantic_linking_merges_synonyms_and_translations() -> None:
    repo = _FakeRepo()
    resolver = SemanticEntityResolver(
        _FakeEmbedder(_VECTORS),
        threshold=0.86,
        enabled=True,
        judge=_FakeJudge("paymentapi"),
    )

    a = await resolver.resolve_or_create(
        repo, group_id="g", name="paymentapi", entity_type="Service"
    )
    b = await resolver.resolve_or_create(
        repo, group_id="g", name="payment service", entity_type="Service"
    )
    c = await resolver.resolve_or_create(
        repo, group_id="g", name="dich vu thanh toan", entity_type="Service"
    )
    d = await resolver.resolve_or_create(repo, group_id="g", name="billing", entity_type="Service")

    assert b.id == a.id  # synonym linked
    assert c.id == a.id  # cross-lingual linked
    assert d.id != a.id  # unrelated stays separate


@pytest.mark.asyncio
async def test_disabled_resolver_never_merges() -> None:
    repo = _FakeRepo()
    resolver = SemanticEntityResolver(_FakeEmbedder(_VECTORS), threshold=0.86, enabled=False)
    a = await resolver.resolve_or_create(
        repo, group_id="g", name="paymentapi", entity_type="Service"
    )
    b = await resolver.resolve_or_create(
        repo, group_id="g", name="payment service", entity_type="Service"
    )
    assert b.id != a.id  # no semantic step when disabled


@pytest.mark.asyncio
async def test_missing_embedder_falls_back_to_create() -> None:
    repo = _FakeRepo()
    resolver = SemanticEntityResolver(None, threshold=0.86, enabled=True)
    a = await resolver.resolve_or_create(
        repo, group_id="g", name="paymentapi", entity_type="Service"
    )
    assert a.canonical_name == "paymentapi"


class _FakeJudge:
    def __init__(self, match: str | None) -> None:
        self._match = match
        self.calls: list[list[str]] = []

    async def same_entity(
        self, *, name: str, entity_type: str, candidates: list[str]
    ) -> str | None:
        self.calls.append(candidates)
        return self._match if self._match in candidates else None


# "billing" sits between the block (0.55) and high-similarity (0.86) thresholds: cosine ~0.70.
_MID = {"paymentapi": [1.0, 0.0, 0.0], "billing": [0.7, 0.71, 0.0]}


@pytest.mark.asyncio
async def test_judge_confirms_a_blocked_candidate() -> None:
    repo = _FakeRepo()
    judge = _FakeJudge("paymentapi")
    resolver = SemanticEntityResolver(
        _FakeEmbedder(_MID), threshold=0.86, block_threshold=0.55, enabled=True, judge=judge
    )
    a = await resolver.resolve_or_create(
        repo, group_id="g", name="paymentapi", entity_type="Service"
    )
    # The judge confirms the blocked candidate, so the names link.
    b = await resolver.resolve_or_create(repo, group_id="g", name="billing", entity_type="Service")
    assert b.id == a.id
    assert judge.calls == [["paymentapi"]]  # the blocked candidate was offered


@pytest.mark.asyncio
async def test_judge_rejects_all_candidates_creates_new() -> None:
    repo = _FakeRepo()
    judge = _FakeJudge(None)
    resolver = SemanticEntityResolver(
        _FakeEmbedder(_MID), threshold=0.86, block_threshold=0.55, enabled=True, judge=judge
    )
    a = await resolver.resolve_or_create(
        repo, group_id="g", name="paymentapi", entity_type="Service"
    )
    b = await resolver.resolve_or_create(repo, group_id="g", name="billing", entity_type="Service")
    assert b.id != a.id  # judge said not the same -> separate entity


@pytest.mark.asyncio
async def test_no_judge_below_threshold_does_not_merge() -> None:
    repo = _FakeRepo()
    resolver = SemanticEntityResolver(
        _FakeEmbedder(_MID), threshold=0.86, block_threshold=0.55, enabled=True, judge=None
    )
    a = await resolver.resolve_or_create(
        repo, group_id="g", name="paymentapi", entity_type="Service"
    )
    b = await resolver.resolve_or_create(repo, group_id="g", name="billing", entity_type="Service")
    assert b.id != a.id  # a mid-similarity name is not merged without a judge


@pytest.mark.asyncio
async def test_no_judge_above_threshold_does_not_merge_sibling_identifiers() -> None:
    vectors = {"cluster-a": [1.0, 0.0], "cluster-b": [0.99, 0.01]}
    repo = _FakeRepo()
    resolver = SemanticEntityResolver(
        _FakeEmbedder(vectors), threshold=0.86, block_threshold=0.55, enabled=True
    )

    first = await resolver.resolve_or_create(
        repo, group_id="g", name="cluster-a", entity_type="System"
    )
    second = await resolver.resolve_or_create(
        repo, group_id="g", name="cluster-b", entity_type="System"
    )

    assert second.id != first.id


def test_enabled_without_embedder_warns_instead_of_degrading_silently() -> None:
    from structlog.testing import capture_logs

    with capture_logs() as logs:
        resolver = SemanticEntityResolver(None, enabled=True, judge=None)
    assert resolver._enabled is False  # inert without an embedder, but not silent
    assert any(e["event"] == "entity_resolver.semantic_enabled_without_embedder" for e in logs)


def test_enabled_without_judge_warns() -> None:
    from structlog.testing import capture_logs

    with capture_logs() as logs:
        SemanticEntityResolver(_FakeEmbedder(_MID), enabled=True, judge=None)
    assert any(e["event"] == "entity_resolver.semantic_enabled_without_judge" for e in logs)


def test_fully_wired_does_not_warn() -> None:
    from structlog.testing import capture_logs

    with capture_logs() as logs:
        SemanticEntityResolver(_FakeEmbedder(_MID), enabled=True, judge=_FakeJudge(None))
    assert not any("semantic_enabled_without" in e["event"] for e in logs)
