"""Combined-retrieval scoring, source-diversity, and packing (Phase 4, pure logic)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from vera.application.retrieval.context_assembler import (
    ContextAssembler,
    ScoredCandidate,
    Signals,
    _diversify,
    _normalized_relevance,
    _pack,
    _recency,
)
from vera.domain.ports.retrieval_index import FactHit, PassageHit
from vera.shared.time import utc_now


def _sc(ref: str, source: str, score: float) -> ScoredCandidate:
    sig = Signals(relevance=score, authority=1.0, verification=1.0, recency=0.5, confidence=0.5)
    from vera.application.retrieval.context_assembler import Citation

    return ScoredCandidate(
        kind="passage",
        ref=ref,
        text="x " * 10,
        score=score,
        signals=sig,
        citation=Citation(kind="passage", ref=ref),
        source_key=source,
        conflict=False,
        reason="",
    )


def test_normalized_relevance_scales_to_unit_range() -> None:
    assert _normalized_relevance([1.0, 3.0, 5.0]) == [0.0, 0.5, 1.0]
    assert _normalized_relevance([2.0, 2.0]) == [1.0, 1.0]  # flat batch -> all top


def test_recency_decays_with_age() -> None:
    now = utc_now()
    assert _recency(now, now, 30 * 86400.0) == pytest.approx(1.0, abs=1e-6)
    assert _recency(None, now, 30 * 86400.0) == 0.5  # unknown is neutral
    month_old = _recency(now - timedelta(days=30), now, 30 * 86400.0)
    assert month_old == pytest.approx(0.5, abs=0.02)  # one half-life


def test_diversity_penalizes_repeated_source() -> None:
    # Three hits from source A and one from B, all equal raw score: A's duplicates decay.
    cands = [_sc("a1", "A", 1.0), _sc("a2", "A", 1.0), _sc("a3", "A", 1.0), _sc("b1", "B", 0.9)]
    out = _diversify(cands, decay=0.5)
    by_ref = {c.ref: c.score for c in out}
    assert by_ref["a1"] == pytest.approx(1.0)
    assert by_ref["a2"] == pytest.approx(0.5)
    assert by_ref["a3"] == pytest.approx(0.25)
    # B (0.9) now outranks A's second hit (0.5), so one source cannot monopolize the top.
    assert out[1].ref == "b1"


def test_pack_respects_limit_and_token_budget() -> None:
    cands = [_sc(f"c{i}", "A", 1.0 - i * 0.01) for i in range(10)]
    packed, omitted, _ = _pack(cands, limit=3, token_budget=10_000)
    assert len(packed) == 3 and omitted == 7
    tiny, tiny_omitted, tokens = _pack(cands, limit=100, token_budget=10)
    assert len(tiny) >= 1 and tiny_omitted > 0 and tokens <= 10 + 100  # budget-bounded


# --------------------------------------------------------- assembler with fakes ---


class _FakeFacts:
    def __init__(self, hits: list[FactHit]) -> None:
        self._hits = hits

    async def search(self, *, group_id, query, limit, as_of=None):
        return self._hits


class _FakePassages:
    def __init__(self, hits: list[PassageHit]) -> None:
        self._hits = hits

    async def search(self, *, group_id, query, limit):
        return self._hits


def _fact(key: str, obj: str, lifecycle: str = "active", authority: float = 1.0) -> FactHit:
    return FactHit(
        fact_key=key,
        fact_id=key,
        subject_name="paymentapi",
        predicate="RUNS_ON",
        object_name=obj,
        text=f"paymentapi RUNS_ON {obj}",
        authority=authority,
        confidence=0.9,
        lifecycle_state=lifecycle,
        score=0.5,
        supporting_source_ids=("src-1",),
    )


def _passage(chunk: str, version: str, score: float) -> PassageHit:
    return PassageHit(
        chunk_id=chunk, artifact_version_id=version, text="deploys on eks cluster", score=score
    )


@pytest.mark.asyncio
async def test_assemble_combines_sources_annotates_conflict_and_cites() -> None:
    facts = _FakeFacts([_fact("f-active", "eks"), _fact("f-disp", "ecs", lifecycle="disputed")])
    # Five passages from ONE artifact version: diversity must stop them dominating.
    passages = _FakePassages([_passage(f"c{i}", "ver-1", 0.6 - i * 0.01) for i in range(5)])
    assembler = ContextAssembler(facts=facts, passages=passages, code=_FakePassages([]))

    result = await assembler.assemble(
        query="where does payment run", group_id="p:x", limit=6, token_budget=10_000
    )

    assert result.conflicts == 1  # the disputed fact is flagged
    assert all(r.citation.ref for r in result.results)  # every hit is cited
    assert all(r.reason for r in result.results)  # and carries a reason
    # The five same-version passages do not fill every slot: the facts still surface.
    kinds = [r.kind for r in result.results]
    assert "fact" in kinds
    assert kinds.count("passage") < 5
