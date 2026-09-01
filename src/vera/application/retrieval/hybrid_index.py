"""Reciprocal-rank fusion for lexical and vector retrieval candidates."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime

from vera.domain.ports.retrieval_index import (
    FactCandidateBatchSource,
    FactCandidateHydrator,
    FactCandidateSets,
    FactCandidateSource,
    FactHit,
    PassageHit,
    PassageIndex,
    RetrievalFilters,
)


class HybridPassageIndex:
    def __init__(self, *indexes: PassageIndex, rank_constant: int = 60) -> None:
        if not indexes:
            raise ValueError("at least one retrieval index is required")
        self._indexes = indexes
        self._rank_constant = rank_constant

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
        async with asyncio.TaskGroup() as group:
            tasks = [
                group.create_task(
                    index.search(
                        group_id=group_id,
                        query=query,
                        limit=limit,
                        created_before=created_before,
                        snapshot_id=snapshot_id,
                        filters=filters,
                    )
                )
                for index in self._indexes
            ]
        result_sets = [task.result() for task in tasks]
        hits: dict[str, PassageHit] = {}
        scores: dict[str, float] = {}
        for result_set in result_sets:
            for rank, hit in enumerate(result_set, start=1):
                hits.setdefault(hit.chunk_id, hit)
                scores[hit.chunk_id] = scores.get(hit.chunk_id, 0.0) + 1.0 / (
                    self._rank_constant + rank
                )
        ranked = sorted(scores, key=lambda chunk_id: (-scores[chunk_id], chunk_id))[:limit]
        return [replace(hits[chunk_id], score=scores[chunk_id]) for chunk_id in ranked]


class HybridFactCandidateSource:
    def __init__(
        self,
        *sources: FactCandidateSource,
        rank_constant: int = 60,
        hydrator: FactCandidateHydrator | None = None,
        batch_source: FactCandidateBatchSource | None = None,
    ) -> None:
        if not sources and batch_source is None:
            raise ValueError("at least one fact candidate source is required")
        if sources and batch_source is not None:
            raise ValueError("fact candidate sources and batch_source are mutually exclusive")
        self._sources = sources
        self._rank_constant = rank_constant
        self._hydrator = hydrator
        self._batch_source = batch_source

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
        batch: FactCandidateSets | None = None
        if self._batch_source is None:
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(
                        source.search(
                            group_id=group_id,
                            query=query,
                            limit=limit,
                            as_of=as_of,
                            known_as_of=known_as_of,
                            restrict_fact_ids=restrict_fact_ids,
                            snapshot_id=snapshot_id,
                            filters=filters,
                        )
                    )
                    for source in self._sources
                ]
            result_sets = [task.result() for task in tasks]
        else:
            batch_source = self._batch_source
            batch = await batch_source.search(
                group_id=group_id,
                query=query,
                limit=limit,
                as_of=as_of,
                known_as_of=known_as_of,
                restrict_fact_ids=restrict_fact_ids,
                snapshot_id=snapshot_id,
                filters=filters,
            )
            result_sets = [batch.lexical, batch.semantic]
        hits: dict[str, FactHit] = {}
        scores: dict[str, float] = {}
        for result_set in result_sets:
            for rank, hit in enumerate(result_set, start=1):
                hits.setdefault(hit.fact_key, hit)
                scores[hit.fact_key] = scores.get(hit.fact_key, 0.0) + 1.0 / (
                    self._rank_constant + rank
                )
        ranked = sorted(scores, key=lambda fact_key: (-scores[fact_key], fact_key))[:limit]
        if self._hydrator is None or (batch is not None and batch.hydrated):
            return [replace(hits[fact_key], score=scores[fact_key]) for fact_key in ranked]
        hydrated = await self._hydrator.hydrate(
            group_id=group_id,
            matches=[(hits[fact_key].fact_id, scores[fact_key]) for fact_key in ranked],
            limit=len(ranked),
            as_of=as_of,
            known_as_of=known_as_of,
            restrict_fact_ids=restrict_fact_ids,
            snapshot_id=snapshot_id,
            filters=filters,
        )
        hydrated_by_id = {hit.fact_id: hit for hit in hydrated}
        return [
            replace(hydrated_by_id[hits[fact_key].fact_id], score=scores[fact_key])
            for fact_key in ranked
            if hits[fact_key].fact_id in hydrated_by_id
        ]
