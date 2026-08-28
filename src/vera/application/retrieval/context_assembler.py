"""ContextAssembler: combined, explainable retrieval over the Knowledge Fabric (Phase 4).

Generates candidates in parallel from the fact store, the passage index, and the code index,
then deduplicates, scores each with a transparent signal vector, applies source-diversity so
one authoritative source is not drowned by duplicates, annotates conflicts, attaches
citations, and packs the result to a token budget. No LLM is used on this path (an optional
reranker is a later, opt-in stage); scoring is a deterministic weighted blend, and every hit
carries its signal vector and a reason, per docs/design/knowledge-fabric section 10.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime

from vera.domain.ports.retrieval_index import (
    CodeIndex,
    FactCandidateSource,
    FactHit,
    PassageHit,
    PassageIndex,
)
from vera.shared.time import utc_now


@dataclass(frozen=True, slots=True)
class RetrievalWeights:
    relevance: float = 0.40
    authority: float = 0.20
    verification: float = 0.15
    recency: float = 0.10
    confidence: float = 0.15
    half_life_s: float = 30 * 86400.0
    diversity_decay: float = 0.5  # score multiplier per additional hit from the same source


@dataclass(frozen=True, slots=True)
class Citation:
    kind: str  # 'fact' | 'passage' | 'code'
    ref: str  # fact_key or chunk_id
    excerpt: str | None = None
    heading_path: str | None = None
    artifact_version_id: str | None = None
    start_offset: int | None = None
    end_offset: int | None = None


@dataclass(frozen=True, slots=True)
class Signals:
    relevance: float
    authority: float
    verification: float
    recency: float
    confidence: float


@dataclass(frozen=True, slots=True)
class ScoredCandidate:
    kind: str
    ref: str
    text: str
    score: float
    signals: Signals
    citation: Citation
    source_key: str
    conflict: bool
    reason: str


@dataclass(frozen=True, slots=True)
class AssembledContext:
    query: str
    results: list[ScoredCandidate]
    omitted: int
    conflicts: int
    token_estimate: int
    freshness_warnings: int


@dataclass(slots=True)
class _Candidate:
    kind: str
    ref: str
    text: str
    relevance_raw: float
    authority: float
    verification: float
    confidence: float
    source_key: str
    conflict: bool
    citation: Citation
    recency_ts: datetime | None = None


def _estimate_tokens(text: str) -> int:
    return max(1, (len(text) + 3) // 4)


def _from_fact(hit: FactHit) -> _Candidate:
    verification = 1.0 if hit.lifecycle_state == "active" else 0.4
    source_key = (
        hit.supporting_source_ids[0] if hit.supporting_source_ids else f"fact:{hit.fact_key}"
    )
    return _Candidate(
        kind="fact",
        ref=hit.fact_key,
        text=hit.text,
        relevance_raw=hit.score,
        authority=hit.authority,
        verification=verification,
        confidence=hit.confidence,
        source_key=source_key,
        conflict=hit.lifecycle_state == "disputed",
        citation=Citation(kind="fact", ref=hit.fact_key, excerpt=hit.text),
        recency_ts=hit.valid_from,
    )


def _from_passage(hit: PassageHit, kind: str) -> _Candidate:
    excerpt = hit.text if len(hit.text) <= 280 else hit.text[:277] + "..."
    return _Candidate(
        kind=kind,
        ref=hit.chunk_id,
        text=hit.text,
        relevance_raw=hit.score,
        # Raw source text is not a verified fact: neutral authority and verification.
        authority=0.5,
        verification=0.7,
        confidence=0.6,
        source_key=hit.artifact_version_id,
        conflict=False,
        citation=Citation(
            kind=kind,
            ref=hit.chunk_id,
            excerpt=excerpt,
            heading_path=hit.heading_path,
            artifact_version_id=hit.artifact_version_id,
            start_offset=hit.start_offset,
            end_offset=hit.end_offset,
        ),
    )


def _dedup(candidates: list[_Candidate]) -> list[_Candidate]:
    seen: set[tuple[str, str]] = set()
    out: list[_Candidate] = []
    for c in candidates:
        key = (c.kind, c.ref)
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out


def _normalized_relevance(values: list[float]) -> list[float]:
    if not values:
        return []
    lo, hi = min(values), max(values)
    if hi <= lo:
        return [1.0 for _ in values]
    return [(v - lo) / (hi - lo) for v in values]


def _recency(ts: datetime | None, now: datetime, half_life_s: float) -> float:
    if ts is None:
        return 0.5  # unknown recency is neutral
    age = max(0.0, (now - ts).total_seconds())
    return math.exp(-math.log(2) * age / half_life_s)


def _score_all(
    candidates: list[_Candidate], weights: RetrievalWeights, *, now: datetime
) -> list[ScoredCandidate]:
    relevances = _normalized_relevance([c.relevance_raw for c in candidates])
    scored: list[ScoredCandidate] = []
    for c, relevance in zip(candidates, relevances, strict=True):
        recency = _recency(c.recency_ts, now, weights.half_life_s)
        signals = Signals(
            relevance=relevance,
            authority=c.authority,
            verification=c.verification,
            recency=recency,
            confidence=c.confidence,
        )
        score = (
            weights.relevance * relevance
            + weights.authority * c.authority
            + weights.verification * c.verification
            + weights.recency * recency
            + weights.confidence * c.confidence
        )
        reason = (
            f"{c.kind} match (relevance {relevance:.2f}, authority {c.authority:.2f}, "
            f"verification {c.verification:.2f})" + (" [conflict]" if c.conflict else "")
        )
        scored.append(
            ScoredCandidate(
                kind=c.kind,
                ref=c.ref,
                text=c.text,
                score=round(score, 6),
                signals=signals,
                citation=c.citation,
                source_key=c.source_key,
                conflict=c.conflict,
                reason=reason,
            )
        )
    return scored


def _diversify(scored: list[ScoredCandidate], decay: float) -> list[ScoredCandidate]:
    """Penalize repeated hits from the same source so one authoritative source is not drowned
    by duplicates: the nth hit from a source is scaled by ``decay**(n-1)``. Deterministic.
    """
    seen: dict[str, int] = {}
    adjusted: list[ScoredCandidate] = []
    for c in sorted(scored, key=lambda x: x.score, reverse=True):
        n = seen.get(c.source_key, 0)
        factor = decay**n
        seen[c.source_key] = n + 1
        adjusted.append(
            ScoredCandidate(
                kind=c.kind,
                ref=c.ref,
                text=c.text,
                score=round(c.score * factor, 6),
                signals=c.signals,
                citation=c.citation,
                source_key=c.source_key,
                conflict=c.conflict,
                reason=c.reason,
            )
        )
    return sorted(adjusted, key=lambda x: x.score, reverse=True)


def _pack(
    scored: list[ScoredCandidate], limit: int, token_budget: int
) -> tuple[list[ScoredCandidate], int, int]:
    packed: list[ScoredCandidate] = []
    tokens = 0
    for c in scored:
        if len(packed) >= limit:
            break
        cost = _estimate_tokens(c.text)
        if packed and tokens + cost > token_budget:
            break
        packed.append(c)
        tokens += cost
    return packed, len(scored) - len(packed), tokens


class ContextAssembler:
    def __init__(
        self,
        *,
        facts: FactCandidateSource,
        passages: PassageIndex,
        code: CodeIndex,
        weights: RetrievalWeights | None = None,
    ) -> None:
        self._facts = facts
        self._passages = passages
        self._code = code
        self._weights = weights or RetrievalWeights()

    async def assemble(
        self,
        *,
        query: str,
        group_id: str,
        limit: int = 10,
        token_budget: int = 2000,
        as_of: datetime | None = None,
        snapshot_fact_ids: set[str] | None = None,
    ) -> AssembledContext:
        k = max(limit * 4, 20)
        fact_hits, passage_hits, code_hits = await asyncio.gather(
            self._facts.search(
                group_id=group_id,
                query=query,
                limit=k,
                as_of=as_of,
                restrict_fact_ids=snapshot_fact_ids,
            ),
            self._passages.search(group_id=group_id, query=query, limit=k),
            self._code.search(group_id=group_id, query=query, limit=k),
        )
        candidates = [
            *(_from_fact(h) for h in fact_hits),
            *(_from_passage(h, "passage") for h in passage_hits),
            *(_from_passage(h, "code") for h in code_hits),
        ]
        candidates = _dedup(candidates)
        now = utc_now()
        scored = _diversify(
            _score_all(candidates, self._weights, now=now), self._weights.diversity_decay
        )
        packed, omitted, tokens = _pack(scored, limit, token_budget)
        return AssembledContext(
            query=query,
            results=packed,
            omitted=omitted,
            conflicts=sum(1 for c in packed if c.conflict),
            token_estimate=tokens,
            freshness_warnings=sum(1 for c in packed if c.signals.recency < 0.3),
        )
