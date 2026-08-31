from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from vera.application.retrieval import HybridFactCandidateSource, HybridPassageIndex
from vera.domain.ports.retrieval_index import (
    FactCandidateSets,
    FactHit,
    PassageHit,
    RetrievalFilters,
)


class _Index:
    def __init__(self, hits: list[PassageHit]) -> None:
        self._hits = hits

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None = None,
        snapshot_id: str | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        del group_id, query, created_before, snapshot_id, filters
        return self._hits[:limit]


class _FactSource:
    def __init__(self, hits: list[FactHit]) -> None:
        self._hits = hits

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        as_of: datetime | None = None,
        known_as_of: datetime | None = None,
        restrict_fact_ids: set[str] | None = None,
        snapshot_id: str | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[FactHit]:
        del group_id, query, as_of, known_as_of, restrict_fact_ids, snapshot_id, filters
        return self._hits[:limit]


class _FactHydrator:
    def __init__(self, hits: list[FactHit]) -> None:
        self._hits = {hit.fact_id: hit for hit in hits}
        self.calls: list[dict[str, object]] = []

    async def hydrate(
        self,
        *,
        group_id: str,
        matches: list[tuple[str, float]],
        limit: int,
        as_of: datetime | None = None,
        known_as_of: datetime | None = None,
        restrict_fact_ids: set[str] | None = None,
        snapshot_id: str | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[FactHit]:
        self.calls.append(
            {
                "group_id": group_id,
                "matches": matches,
                "limit": limit,
                "as_of": as_of,
                "known_as_of": known_as_of,
                "restrict_fact_ids": restrict_fact_ids,
                "snapshot_id": snapshot_id,
                "filters": filters,
            }
        )
        return [self._hits[fact_id] for fact_id, _ in reversed(matches)]


class _FactBatchSource:
    def __init__(self) -> None:
        self.calls = 0

    async def search(self, **_kwargs: object) -> FactCandidateSets:
        self.calls += 1
        return FactCandidateSets(
            lexical=(_fact("shared", 10.0), _fact("lexical-only", 9.0)),
            semantic=(_fact("shared", 0.9), _fact("semantic-only", 0.8)),
        )


def _hit(chunk_id: str, score: float) -> PassageHit:
    return PassageHit(
        chunk_id=chunk_id,
        artifact_version_id=f"version-{chunk_id}",
        text=chunk_id,
        score=score,
    )


def _fact(fact_key: str, score: float) -> FactHit:
    return FactHit(
        fact_key=fact_key,
        fact_id=f"id-{fact_key}",
        subject_name="Platform Team",
        predicate="OWNS",
        object_name="Payment API",
        text="Platform Team OWNS Payment API",
        authority=1.0,
        confidence=1.0,
        lifecycle_state="active",
        score=score,
    )


@pytest.mark.asyncio
async def test_rrf_keeps_lexical_only_and_vector_only_hits() -> None:
    hybrid = HybridPassageIndex(
        _Index([_hit("shared", 10.0), _hit("fts-only", 9.0)]),
        _Index([_hit("shared", 0.99), _hit("vector-only", 0.98)]),
    )

    hits = await hybrid.search(group_id="p:test", query="runtime", limit=3)

    assert [hit.chunk_id for hit in hits] == ["shared", "fts-only", "vector-only"]
    assert hits[0].score > hits[1].score


@pytest.mark.asyncio
async def test_fact_rrf_keeps_semantic_only_hits() -> None:
    hybrid = HybridFactCandidateSource(
        _FactSource([_fact("shared", 10.0), _fact("lexical-only", 9.0)]),
        _FactSource([_fact("shared", 0.9), _fact("semantic-only", 0.8)]),
    )

    hits = await hybrid.search(group_id="p:test", query="runtime", limit=3)

    assert [hit.fact_key for hit in hits] == ["shared", "lexical-only", "semantic-only"]
    assert hits[0].score > hits[1].score


@pytest.mark.asyncio
async def test_fact_rrf_accepts_candidate_sets_from_one_batch() -> None:
    hybrid = HybridFactCandidateSource(batch_source=_FactBatchSource())

    hits = await hybrid.search(group_id="p:test", query="runtime", limit=3)

    assert [hit.fact_key for hit in hits] == ["shared", "lexical-only", "semantic-only"]
    assert hits[0].score > hits[1].score


@pytest.mark.asyncio
async def test_fact_batch_search_honors_shared_concurrency_limit() -> None:
    source = _FactBatchSource()
    semaphore = asyncio.Semaphore(0)
    hybrid = HybridFactCandidateSource(batch_source=source, batch_semaphore=semaphore)

    task = asyncio.create_task(hybrid.search(group_id="p:test", query="runtime", limit=3))
    await asyncio.sleep(0)

    assert source.calls == 0
    semaphore.release()
    hits = await task
    assert source.calls == 1
    assert [hit.fact_key for hit in hits] == ["shared", "lexical-only", "semantic-only"]


@pytest.mark.asyncio
async def test_fact_rrf_hydrates_once_and_restores_fused_order() -> None:
    hydrator = _FactHydrator(
        [
            replace(_fact("shared", 0.0), evidence_id="e-shared"),
            replace(_fact("lexical-only", 0.0), evidence_id="e-lexical"),
            replace(_fact("semantic-only", 0.0), evidence_id="e-semantic"),
        ]
    )
    filters = RetrievalFilters(include_predicates=("OWNS",))
    as_of = datetime(2026, 1, 1, tzinfo=UTC)
    known_as_of = datetime(2026, 1, 2, tzinfo=UTC)
    restricted = {"id-shared", "id-lexical-only", "id-semantic-only"}
    hybrid = HybridFactCandidateSource(
        _FactSource([_fact("shared", 10.0), _fact("lexical-only", 9.0)]),
        _FactSource([_fact("shared", 0.9), _fact("semantic-only", 0.8)]),
        hydrator=hydrator,
    )

    hits = await hybrid.search(
        group_id="p:test",
        query="runtime",
        limit=3,
        as_of=as_of,
        known_as_of=known_as_of,
        restrict_fact_ids=restricted,
        snapshot_id="snapshot",
        filters=filters,
    )

    assert [hit.fact_key for hit in hits] == ["shared", "lexical-only", "semantic-only"]
    assert [hit.evidence_id for hit in hits] == ["e-shared", "e-lexical", "e-semantic"]
    assert [hit.score for hit in hits] == pytest.approx([2 / 61, 1 / 62, 1 / 62])
    assert hydrator.calls == [
        {
            "group_id": "p:test",
            "matches": [
                ("id-shared", 2 / 61),
                ("id-lexical-only", 1 / 62),
                ("id-semantic-only", 1 / 62),
            ],
            "limit": 3,
            "as_of": as_of,
            "known_as_of": known_as_of,
            "restrict_fact_ids": restricted,
            "snapshot_id": "snapshot",
            "filters": filters,
        }
    ]
