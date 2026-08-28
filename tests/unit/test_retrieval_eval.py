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


def test_ndcg_rewards_higher_placement() -> None:
    from vera.application.queries.retrieval_eval import ndcg_at_k

    # A relevant item at rank 1 scores a perfect nDCG; pushed to rank 3 it scores less.
    assert ndcg_at_k([1.0, 0.0, 0.0], k=3) == 1.0
    lower = ndcg_at_k([0.0, 0.0, 1.0], k=3)
    assert 0.0 < lower < 1.0
    # No relevant item: nothing to normalize against, so 0.
    assert ndcg_at_k([0.0, 0.0, 0.0], k=3) == 0.0


def test_relevances_are_binary_and_case_insensitive() -> None:
    from vera.application.queries.retrieval_eval import relevances

    assert relevances(["paymentapi RUNS_ON EKS", "unrelated"], ["eks"]) == [1.0, 0.0]


def test_citation_rate() -> None:
    from vera.application.queries.retrieval_eval import citation_rate

    assert citation_rate([True, True, False, True]) == 0.75
    assert citation_rate([]) == 0.0


def test_score_reports_ndcg() -> None:
    from vera.application.queries.retrieval_eval import score

    report = score([(["paymentapi RUNS_ON eks", "noise"], ["eks"])], k=5)
    assert report.hit_rate == 1.0
    assert report.mrr == 1.0
    assert report.ndcg == 1.0
