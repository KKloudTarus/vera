"""SearchMemory: the retrieval read model (stage-1 retrieve, stage-2 rerank).

Stage 1 delegates candidate generation to the ``MemoryEngine``. Stage 2 is VERA's:
normalize relevance per batch, enrich each hit with provenance in one batched query,
and blend relevance, authority, verification, recency, and feedback. Results carry
their provenance so an agent can reason about how much to trust each fact.
"""

from __future__ import annotations

import asyncio
import math
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime

from vera.domain.ports.memory_engine import GraphHit, GraphQuery, MemoryEngine
from vera.domain.ports.retrieval import HitProvenance, RetrievalReadModel
from vera.observability import span
from vera.observability.cost import UsageContext, reset_usage_context, set_usage_context
from vera.observability.metrics import record_search
from vera.shared.time import utc_now
from vera.shared.types import GroupId


@dataclass(frozen=True, slots=True)
class SearchMemory:
    text: str
    group_ids: tuple[GroupId, ...]
    limit: int = 10
    as_of: datetime | None = None


@dataclass(frozen=True, slots=True)
class RankedHit:
    fact: str
    score: float
    source_id: str | None
    verification: str | None
    authority: float
    valid_at: datetime | None
    invalid_at: datetime | None


# Stage-2 blend weights (Σ = 1). Tune offline against retrieval feedback.
_W_REL, _W_AUTH, _W_VER, _W_TMP, _W_FB = 0.45, 0.20, 0.15, 0.12, 0.08
_RECENCY_HALF_LIFE_S = 30 * 24 * 3600.0
_VERIFICATION_SCORE = {"human_verified": 1.0, "auto": 0.8, "pending": 0.5}


def _normalize(values: list[float]) -> list[float]:
    if not values:
        return []
    lo, hi = min(values), max(values)
    span = hi - lo + 1e-9  # epsilon guards divide-by-zero when all scores are equal
    return [(v - lo) / span for v in values]


def _recency(valid_at: datetime | None, invalid_at: datetime | None, now: datetime) -> float:
    if invalid_at is not None:
        return 0.0
    if valid_at is None:
        return 0.5
    delta = max((now - valid_at).total_seconds(), 0.0)
    return math.exp(-math.log(2) * delta / _RECENCY_HALF_LIFE_S)


class SearchMemoryHandler:
    def __init__(
        self,
        engine: MemoryEngine,
        read_model: RetrievalReadModel,
        *,
        read_timeout_s: float | None = None,
    ) -> None:
        self._engine = engine
        self._read_model = read_model
        self._read_timeout_s = read_timeout_s

    async def handle(self, query: SearchMemory) -> list[RankedHit]:
        started = time.perf_counter()
        results: list[RankedHit] = []
        # Tag any query-embedding provider call this search triggers as search cost.
        token = set_usage_context(UsageContext(request_kind="search"))
        try:
            async with AsyncExitStack() as stack:
                # A tight read-path budget: a slow dependency fails the query fast
                # rather than hanging the caller.
                if self._read_timeout_s is not None:
                    await stack.enter_async_context(asyncio.timeout(self._read_timeout_s))
                stack.enter_context(span("memory.search", group_count=len(query.group_ids)))
                results = await self._handle(query)
        finally:
            reset_usage_context(token)
        record_search(duration_s=time.perf_counter() - started, hits=len(results))
        return results

    async def _handle(self, query: SearchMemory) -> list[RankedHit]:
        candidates: list[GraphHit] = list(
            await self._engine.search(
                GraphQuery(
                    text=query.text,
                    group_ids=query.group_ids,
                    limit=max(query.limit * 4, 30),
                    as_of=query.as_of,
                )
            )
        )
        if not candidates:
            return []

        group_ids = [str(g) for g in query.group_ids]
        edge_uuids = [c.edge_uuid for c in candidates if c.edge_uuid]
        provenance = await self._read_model.enrich(group_ids=group_ids, edge_uuids=edge_uuids)
        feedback = await self._read_model.feedback_counts(group_ids=group_ids, refs=edge_uuids)

        with span("memory.rerank", candidates=len(candidates)):
            return self._rerank(candidates, provenance, feedback, limit=query.limit)

    def _rerank(
        self,
        candidates: list[GraphHit],
        provenance: dict[str, HitProvenance],
        feedback: dict[str, tuple[int, int]],
        *,
        limit: int,
    ) -> list[RankedHit]:
        now = utc_now()
        rel = _normalize([c.score for c in candidates])
        ranked: list[tuple[float, RankedHit]] = []
        for norm_rel, hit in zip(rel, candidates, strict=True):
            prov = provenance.get(hit.edge_uuid or "")
            authority = prov.authority if prov else 0.5
            verification = prov.verification if prov else None
            ver_score = _VERIFICATION_SCORE.get(verification or "", 0.5)
            recency = _recency(hit.valid_at, hit.invalid_at, now)
            up, down = feedback.get(hit.edge_uuid or "", (0, 0))
            fb_score = (up + 1) / (up + down + 2)  # Laplace-smoothed, defaults to 0.5
            score = (
                _W_REL * norm_rel
                + _W_AUTH * authority
                + _W_VER * ver_score
                + _W_TMP * recency
                + _W_FB * fb_score
            )
            ranked.append(
                (
                    score,
                    RankedHit(
                        fact=hit.fact,
                        score=round(score, 6),
                        source_id=prov.source_id if prov else None,
                        verification=verification,
                        authority=authority,
                        valid_at=hit.valid_at,
                        invalid_at=hit.invalid_at,
                    ),
                )
            )

        ranked.sort(key=lambda pair: pair[0], reverse=True)
        return [hit for _, hit in ranked[:limit]]
