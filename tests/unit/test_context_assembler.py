"""Combined-retrieval scoring, source-diversity, and packing (Phase 4, pure logic)."""

from __future__ import annotations

from datetime import datetime, timedelta

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
from vera.domain.ports.retrieval_index import ContentAvailability, FactHit, PassageHit
from vera.shared.time import utc_now


def _sc(ref: str, source: str, score: float, *, excerpt: str | None = None) -> ScoredCandidate:
    sig = Signals(relevance=score, authority=1.0, verification=1.0, recency=0.5, confidence=0.5)
    from vera.application.retrieval.context_assembler import Citation

    return ScoredCandidate(
        kind="passage",
        ref=ref,
        text="x " * 10,
        score=score,
        signals=sig,
        citation=Citation(kind="passage", ref=ref, excerpt=excerpt),
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


def test_diversity_breaks_score_ties_by_stable_identity() -> None:
    candidates = [_sc("c", "C", 1.0), _sc("a", "A", 1.0), _sc("b", "B", 1.0)]

    assert [candidate.ref for candidate in _diversify(candidates, decay=0.5)] == ["a", "b", "c"]


def test_pack_respects_limit_and_token_budget() -> None:
    cands = [_sc(f"c{i}", "A", 1.0 - i * 0.01) for i in range(10)]
    packed, omitted, _ = _pack(cands, limit=3, token_budget=10_000)
    assert len(packed) == 3 and omitted == 7
    tiny, tiny_omitted, tokens = _pack(cands, limit=100, token_budget=10)
    assert len(tiny) >= 1 and tiny_omitted > 0 and tokens <= 10


def test_pack_accounts_for_full_citation_excerpts() -> None:
    candidates = [_sc("quoted", "A", 1.0, excerpt="q" * 400), _sc("next", "B", 0.9)]

    full, omitted, full_tokens = _pack(candidates, limit=2, token_budget=50)
    compact, _, compact_tokens = _pack(
        candidates, limit=2, token_budget=50, include_citation_excerpts=False
    )

    assert [candidate.ref for candidate in full] == ["next"]
    assert omitted == 1 and full_tokens <= 50
    assert len(compact) == 2 and compact_tokens <= 50


# --------------------------------------------------------- assembler with fakes ---


class _FakeFacts:
    def __init__(self, hits: list[FactHit]) -> None:
        self._hits = hits
        self.known_as_of: datetime | None = None

    async def search(
        self,
        *,
        group_id,
        query,
        limit,
        as_of=None,
        known_as_of=None,
        restrict_fact_ids=None,
        snapshot_id=None,
        filters=None,
    ):
        self.known_as_of = known_as_of
        return self._hits


class _FakePassages:
    def __init__(self, hits: list[PassageHit]) -> None:
        self._hits = hits
        self.created_before: datetime | None = None
        self.calls = 0

    async def search(
        self, *, group_id, query, limit, created_before=None, snapshot_id=None, filters=None
    ):
        self.calls += 1
        self.created_before = created_before
        return self._hits


class _FakeContentAvailability:
    def __init__(self, *, passages: bool, code: bool) -> None:
        self._available = ContentAvailability(passages=passages, code=code)
        self.calls = 0

    async def get(self, *, group_id: str, snapshot_id: str | None = None) -> ContentAvailability:
        self.calls += 1
        return self._available


def _fact(
    key: str,
    obj: str,
    lifecycle: str = "active",
    authority: float = 1.0,
    *,
    exact_match: bool = False,
) -> FactHit:
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
        exact_match=exact_match,
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
    assert all(
        r.citation.excerpt is None for r in result.results if r.kind == "fact"
    )  # never synthesize evidence when no exact quote exists
    # The five same-version passages do not fill every slot: the facts still surface.
    kinds = [r.kind for r in result.results]
    assert "fact" in kinds
    assert kinds.count("passage") < 5


@pytest.mark.asyncio
async def test_assemble_uses_one_result_identity_per_chunk() -> None:
    passages = _FakePassages([_passage("shared", "ver-1", 0.6)])
    code = _FakePassages([_passage("shared", "ver-1", 0.8)])

    result = await ContextAssembler(facts=_FakeFacts([]), passages=passages, code=code).assemble(
        query="deploy", group_id="p:x"
    )

    assert [(candidate.kind, candidate.ref) for candidate in result.results] == [("code", "shared")]


@pytest.mark.asyncio
async def test_assemble_exact_fact_omits_passage_and_code_candidates() -> None:
    passages = _FakePassages([_passage("passage", "ver-1", 1.0)])
    code = _FakePassages([_passage("code", "ver-1", 1.0)])
    availability = _FakeContentAvailability(passages=True, code=True)

    result = await ContextAssembler(
        facts=_FakeFacts([_fact("exact", "eks", exact_match=True)]),
        passages=passages,
        code=code,
        content_availability=availability,
    ).assemble(query="paymentapi RUNS_ON eks", group_id="p:x")

    assert [(candidate.kind, candidate.ref) for candidate in result.results] == [("fact", "exact")]
    assert passages.calls == code.calls == availability.calls == 0


@pytest.mark.asyncio
async def test_valid_time_does_not_imply_a_transaction_boundary() -> None:
    boundary = utc_now()
    facts = _FakeFacts([])
    passages = _FakePassages([])
    code = _FakePassages([])

    await ContextAssembler(facts=facts, passages=passages, code=code).assemble(
        query="payment", group_id="p:x", as_of=boundary
    )

    assert facts.known_as_of is None
    assert passages.created_before is None
    assert code.created_before is None


@pytest.mark.asyncio
async def test_assemble_skips_indexes_for_unavailable_content() -> None:
    passages = _FakePassages([_passage("passage", "version", 1.0)])
    code = _FakePassages([_passage("code", "version", 1.0)])
    availability = _FakeContentAvailability(passages=False, code=False)
    assembler = ContextAssembler(
        facts=_FakeFacts([_fact("fact", "eks")]),
        passages=passages,
        code=code,
        content_availability=availability,
    )

    result = await assembler.assemble(query="payment RUNS_ON unknown", group_id="p:x")

    assert [candidate.kind for candidate in result.results] == ["fact"]
    assert passages.calls == code.calls == 0
    assert availability.calls == 1
