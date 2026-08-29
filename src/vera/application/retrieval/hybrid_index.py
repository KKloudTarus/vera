"""Reciprocal-rank fusion for lexical and vector passage candidates."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime

from vera.domain.ports.retrieval_index import PassageHit, PassageIndex, RetrievalFilters


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
