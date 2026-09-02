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
from typing import Literal

from vera.domain.ports.retrieval_index import (
    CodeIndex,
    ContentAvailability,
    ContentAvailabilitySource,
    FactCandidateSource,
    FactHit,
    PassageHit,
    PassageIndex,
    RetrievalFilters,
    is_exact_fact_query_candidate,
)
from vera.shared.time import utc_now
from vera.shared.types import JsonDict


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
    evidence_id: str | None = None
    assertion_id: str | None = None
    source_id: str | None = None
    excerpt: str | None = None
    chunk_id: str | None = None
    heading_path: str | None = None
    artifact_version_id: str | None = None
    start_offset: int | None = None
    end_offset: int | None = None
    quote_hash: str | None = None
    content_hash: str | None = None
    extraction_run_id: str | None = None
    source_coordinates: JsonDict | None = None
    structured_record: JsonDict | None = None
    citation_uri: str | None = None


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
        citation=Citation(
            kind="fact",
            ref=hit.fact_key,
            evidence_id=hit.evidence_id,
            assertion_id=hit.evidence_assertion_id,
            source_id=hit.evidence_source_id,
            excerpt=hit.evidence_excerpt,
            chunk_id=hit.evidence_chunk_id,
            artifact_version_id=hit.evidence_artifact_version_id,
            start_offset=hit.evidence_start_offset,
            end_offset=hit.evidence_end_offset,
            quote_hash=hit.evidence_quote_hash,
            content_hash=hit.evidence_content_hash,
            extraction_run_id=hit.evidence_extraction_run_id,
            source_coordinates=hit.evidence_source_coordinates,
            structured_record=hit.evidence_structured_record,
            citation_uri=hit.evidence_citation_uri,
        ),
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
            chunk_id=hit.chunk_id,
            heading_path=hit.heading_path,
            artifact_version_id=hit.artifact_version_id,
            start_offset=hit.start_offset,
            end_offset=hit.end_offset,
            content_hash=hit.content_hash,
        ),
    )


def _dedup(candidates: list[_Candidate]) -> list[_Candidate]:
    seen: dict[tuple[str, str], int] = {}
    out: list[_Candidate] = []
    for c in candidates:
        key = ("fact" if c.kind == "fact" else "chunk", c.ref)
        index = seen.get(key)
        if index is not None:
            previous = out[index]
            if c.relevance_raw > previous.relevance_raw or (
                c.relevance_raw == previous.relevance_raw
                and c.kind == "code"
                and previous.kind == "passage"
            ):
                out[index] = c
            continue
        seen[key] = len(out)
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
    if half_life_s <= 0:
        return 1.0  # a non-positive half-life means no decay (guards divide-by-zero)
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
    for c in sorted(scored, key=lambda x: (-x.score, x.kind, x.ref)):
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
    return sorted(adjusted, key=lambda x: (-x.score, x.kind, x.ref))


def _pack(
    scored: list[ScoredCandidate],
    limit: int,
    token_budget: int,
    *,
    include_citation_excerpts: bool = True,
) -> tuple[list[ScoredCandidate], int, int]:
    packed: list[ScoredCandidate] = []
    tokens = 0
    for c in scored:
        if len(packed) >= limit:
            break
        content = c.text
        if include_citation_excerpts and c.citation.excerpt:
            content = f"{content}\n{c.citation.excerpt}"
        cost = _estimate_tokens(content)
        if tokens + cost > token_budget:
            continue
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
        content_availability: ContentAvailabilitySource | None = None,
        weights: RetrievalWeights | None = None,
    ) -> None:
        self._facts = facts
        self._passages = passages
        self._code = code
        self._content_availability = content_availability
        self._weights = weights or RetrievalWeights()

    async def assemble(
        self,
        *,
        query: str,
        group_id: str,
        limit: int = 10,
        token_budget: int = 2000,
        as_of: datetime | None = None,
        known_as_of: datetime | None = None,
        snapshot_fact_ids: set[str] | None = None,
        snapshot_id: str | None = None,
        passage_cutoff: datetime | None = None,
        filters: RetrievalFilters | None = None,
        citation_mode: Literal["full", "compact"] = "full",
    ) -> AssembledContext:
        k = max(limit * 4, 20)
        available = ContentAvailability(passages=True, code=True)
        passages_task: asyncio.Task[list[PassageHit]] | None = None
        code_task: asyncio.Task[list[PassageHit]] | None = None
        fact_hits: list[FactHit] | None = None
        exact_fact_match = False
        probe_exact_fact_first = (
            as_of is None
            and known_as_of is None
            and snapshot_fact_ids is None
            and snapshot_id is None
            and filters is None
            and is_exact_fact_query_candidate(query)
        )
        async with asyncio.TaskGroup() as group:
            facts_task = group.create_task(
                self._facts.search(
                    group_id=group_id,
                    query=query,
                    limit=k,
                    as_of=as_of,
                    known_as_of=known_as_of,
                    restrict_fact_ids=snapshot_fact_ids,
                    snapshot_id=snapshot_id,
                    filters=filters,
                )
            )
            if self._content_availability is not None and not probe_exact_fact_first:
                availability_task = group.create_task(
                    self._content_availability.get(group_id=group_id, snapshot_id=snapshot_id)
                )
                available = await availability_task
            if facts_task.done():
                fact_hits = facts_task.result()
                exact_fact_match = any(hit.exact_match for hit in fact_hits)
            elif probe_exact_fact_first:
                fact_hits = await facts_task
                exact_fact_match = any(hit.exact_match for hit in fact_hits)
                if self._content_availability is not None and not exact_fact_match:
                    availability_task = group.create_task(
                        self._content_availability.get(group_id=group_id, snapshot_id=snapshot_id)
                    )
                    available = await availability_task
            if available.passages and not exact_fact_match:
                passages_task = group.create_task(
                    self._passages.search(
                        group_id=group_id,
                        query=query,
                        limit=k,
                        created_before=passage_cutoff or known_as_of,
                        snapshot_id=snapshot_id,
                        filters=filters,
                    )
                )
            if available.code and not exact_fact_match:
                code_task = group.create_task(
                    self._code.search(
                        group_id=group_id,
                        query=query,
                        limit=k,
                        created_before=passage_cutoff or known_as_of,
                        snapshot_id=snapshot_id,
                        filters=filters,
                    )
                )
            if fact_hits is None:
                fact_hits = await facts_task
                exact_fact_match = any(hit.exact_match for hit in fact_hits)
                if exact_fact_match:
                    if passages_task is not None:
                        passages_task.cancel()
                    if code_task is not None:
                        code_task.cancel()
        passage_hits = (
            passages_task.result() if passages_task is not None and not exact_fact_match else []
        )
        code_hits = code_task.result() if code_task is not None and not exact_fact_match else []
        candidates = [
            *(_from_fact(h) for h in fact_hits),
            *(_from_passage(h, "passage") for h in passage_hits),
            *(_from_passage(h, "code") for h in code_hits),
        ]
        if filters is not None:
            if filters.min_authority is not None:
                candidates = [
                    candidate
                    for candidate in candidates
                    if candidate.authority >= filters.min_authority
                ]
            if filters.conflict_handling == "exclude":
                candidates = [candidate for candidate in candidates if not candidate.conflict]
            elif filters.conflict_handling == "only":
                candidates = [candidate for candidate in candidates if candidate.conflict]
        candidates = _dedup(candidates)
        now = passage_cutoff or utc_now()
        scored = _diversify(
            _score_all(candidates, self._weights, now=now), self._weights.diversity_decay
        )
        packed, omitted, tokens = _pack(
            scored,
            limit,
            token_budget,
            include_citation_excerpts=citation_mode == "full",
        )
        return AssembledContext(
            query=query,
            results=packed,
            omitted=omitted,
            conflicts=sum(1 for c in packed if c.conflict),
            token_estimate=tokens,
            freshness_warnings=sum(1 for c in packed if c.signals.recency < 0.3),
        )
