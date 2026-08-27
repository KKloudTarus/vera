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
from collections.abc import Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from datetime import datetime

from vera.domain.ports.memory_engine import GraphHit, GraphQuery, MemoryEngine
from vera.domain.ports.reranker import Reranker
from vera.domain.ports.retrieval import HitProvenance, RetrievalReadModel
from vera.observability import span
from vera.observability.cost import UsageContext, reset_usage_context, set_usage_context
from vera.observability.metrics import record_search
from vera.shared.time import utc_now
from vera.shared.types import GroupId


def _empty_signals() -> dict[str, float]:
    return {}


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
    # The normalized stage-2 signal values behind this hit's score. Logged with any
    # feedback the caller gives so rerank weights can be calibrated from real labels.
    signals: Mapping[str, float] = field(default_factory=_empty_signals)


_VERIFICATION_SCORE = {"human_verified": 1.0, "auto": 0.8, "pending": 0.5}
_DAY_S = 24 * 3600.0


@dataclass(frozen=True, slots=True)
class RerankWeights:
    """Stage-2 blend weights and recency half-life. Tunable, not hard-coded."""

    relevance: float = 0.40
    authority: float = 0.18
    verification: float = 0.12
    recency: float = 0.12
    feedback: float = 0.08
    confidence: float = 0.10
    half_life_s: float = 30 * _DAY_S

    def normalized(self) -> RerankWeights:
        total = (
            self.relevance
            + self.authority
            + self.verification
            + self.recency
            + self.feedback
            + self.confidence
        ) or 1.0
        return RerankWeights(
            relevance=self.relevance / total,
            authority=self.authority / total,
            verification=self.verification / total,
            recency=self.recency / total,
            feedback=self.feedback / total,
            confidence=self.confidence / total,
            half_life_s=self.half_life_s,
        )


def _normalize(values: list[float]) -> list[float]:
    if not values:
        return []
    lo, hi = min(values), max(values)
    span = hi - lo + 1e-9  # epsilon guards divide-by-zero when all scores are equal
    return [(v - lo) / span for v in values]


def _recency(
    valid_at: datetime | None, invalid_at: datetime | None, now: datetime, half_life_s: float
) -> float:
    if invalid_at is not None:
        return 0.0
    if valid_at is None:
        return 0.5
    delta = max((now - valid_at).total_seconds(), 0.0)
    return math.exp(-math.log(2) * delta / half_life_s)


class SearchMemoryHandler:
    def __init__(
        self,
        engine: MemoryEngine,
        read_model: RetrievalReadModel,
        *,
        read_timeout_s: float | None = None,
        weights: RerankWeights | None = None,
        reranker: Reranker | None = None,
        cross_encoder_weight: float = 0.5,
        cross_encoder_top_n: int = 20,
    ) -> None:
        self._engine = engine
        self._read_model = read_model
        self._read_timeout_s = read_timeout_s
        self._weights = (weights or RerankWeights()).normalized()
        self._reranker = reranker
        self._ce_weight = cross_encoder_weight
        self._ce_top_n = cross_encoder_top_n

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
            if self._reranker is None:
                return self._rerank(candidates, provenance, feedback, limit=query.limit)
            head = self._rerank(candidates, provenance, feedback, limit=self._ce_top_n)
        reordered = await self._cross_encode(query.text, head)
        return reordered[: query.limit]

    async def _cross_encode(self, query_text: str, head: list[RankedHit]) -> list[RankedHit]:
        """Stage 3: blend a cross-encoder's query-fact relevance into the head's order."""
        if not head or self._reranker is None:
            return head
        with span("memory.cross_encode", head=len(head)):
            ce_scores = await self._reranker.rerank(query=query_text, facts=[h.fact for h in head])
        blends = [h.score for h in head]
        lo, hi = min(blends), max(blends)
        span_ = hi - lo + 1e-9
        w = self._ce_weight
        scored: list[tuple[float, RankedHit]] = []
        for hit, ce in zip(head, ce_scores, strict=True):
            final = (1 - w) * ((hit.score - lo) / span_) + w * ce
            scored.append((final, replace(hit, score=round(final, 6))))
        scored.sort(key=lambda pair: pair[0], reverse=True)
        return [hit for _, hit in scored]

    def _rerank(
        self,
        candidates: list[GraphHit],
        provenance: dict[str, HitProvenance],
        feedback: dict[str, tuple[int, int]],
        *,
        limit: int,
    ) -> list[RankedHit]:
        w = self._weights
        now = utc_now()
        rel = _normalize([c.score for c in candidates])
        ranked: list[tuple[float, RankedHit]] = []
        for norm_rel, hit in zip(rel, candidates, strict=True):
            prov = provenance.get(hit.edge_uuid or "")
            authority = prov.authority if prov else 0.5
            verification = prov.verification if prov else None
            ver_score = _VERIFICATION_SCORE.get(verification or "", 0.5)
            recency = _recency(hit.valid_at, hit.invalid_at, now, w.half_life_s)
            confidence = prov.confidence if prov else 1.0
            up, down = feedback.get(hit.edge_uuid or "", (0, 0))
            fb_score = (up + 1) / (up + down + 2)  # Laplace-smoothed, defaults to 0.5
            signals = {
                "relevance": norm_rel,
                "authority": authority,
                "verification": ver_score,
                "recency": recency,
                "feedback": fb_score,
                "confidence": confidence,
            }
            score = (
                w.relevance * norm_rel
                + w.authority * authority
                + w.verification * ver_score
                + w.recency * recency
                + w.feedback * fb_score
                + w.confidence * confidence
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
                        signals=signals,
                    ),
                )
            )

        ranked.sort(key=lambda pair: pair[0], reverse=True)
        return [hit for _, hit in ranked[:limit]]
