"""Retrieval-quality metrics: rank detection, hit@k, and MRR."""

from __future__ import annotations

from vera.application.queries.retrieval_eval import EvalReport, first_hit_rank, score


def test_first_hit_rank_is_case_insensitive_and_one_based() -> None:
    facts = ["paymentapi RUNS_ON prod-eks", "billing DEPENDS_ON POSTGRES"]
    assert first_hit_rank(facts, ["postgres"]) == 2  # matched at position 2
    assert first_hit_rank(facts, ["redis"]) is None  # no match


def test_score_computes_hit_rate_and_mrr() -> None:
    per_case = [
        (["a fact about X", "other"], ["X"]),  # hit at rank 1 -> rr 1.0
        (["nope", "has Y here"], ["Y"]),  # hit at rank 2 -> rr 0.5
        (["nothing relevant"], ["Z"]),  # miss -> rr 0
    ]
    report = score(per_case, k=1)
    assert report.cases == 3
    assert report.hits_at_k == 1  # only the rank-1 hit is within k=1
    assert abs(report.mrr - (1.0 + 0.5 + 0.0) / 3) < 1e-9
    assert abs(report.hit_rate - 1 / 3) < 1e-9


def test_hit_at_k_widens_with_k() -> None:
    per_case = [(["nope", "has Y here"], ["Y"])]  # hit at rank 2
    assert score(per_case, k=1).hits_at_k == 0
    assert score(per_case, k=2).hits_at_k == 1


def test_empty_set_is_zero() -> None:
    report = score([], k=5)
    assert report == EvalReport(cases=0, hits_at_k=0, k=5, mrr=0.0)
