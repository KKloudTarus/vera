"""Offline retrieval-quality metrics over a golden set.

A golden case is a query plus the substrings that a correct answer must contain. Running
each query yields a ranked list of results; the first result that contains any expected
substring is the hit, and its 1-based rank drives hit@k (was a correct result in the top k)
and MRR (mean reciprocal rank). nDCG@k adds position-weighted graded quality, and the
citation rate captures whether returned results are traceable. Keeping the scoring pure lets
a CI gate assert a baseline without a live graph.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass


def first_hit_rank(ranked_facts: Sequence[str], expected: Sequence[str]) -> int | None:
    """1-based rank of the first fact containing any expected substring, else None.
    Matching is case-insensitive.
    """
    needles = [e.lower() for e in expected if e]
    for index, fact in enumerate(ranked_facts, start=1):
        haystack = fact.lower()
        if any(n in haystack for n in needles):
            return index
    return None


def relevances(ranked_facts: Sequence[str], expected: Sequence[str]) -> list[float]:
    """Binary relevance per ranked position: 1 if the result contains any expected substring."""
    needles = [e.lower() for e in expected if e]
    return [1.0 if any(n in fact.lower() for n in needles) else 0.0 for fact in ranked_facts]


def ndcg_at_k(rels: Sequence[float], k: int) -> float:
    """Normalized discounted cumulative gain over the top k. 0 when no relevant item exists
    (an ideal ranking of all-zero relevances has no gain to normalize against).
    """
    top = list(rels[:k])
    dcg = sum(rel / math.log2(i + 2) for i, rel in enumerate(top))
    ideal = sorted(rels, reverse=True)[:k]
    idcg = sum(rel / math.log2(i + 2) for i, rel in enumerate(ideal))
    return dcg / idcg if idcg > 0 else 0.0


def citation_rate(cited_flags: Sequence[bool]) -> float:
    """Fraction of returned results that carry a citation reference (traceable output)."""
    return sum(1 for c in cited_flags if c) / len(cited_flags) if cited_flags else 0.0


@dataclass(frozen=True, slots=True)
class EvalReport:
    cases: int
    hits_at_k: int
    k: int

    @property
    def hit_rate(self) -> float:
        return self.hits_at_k / self.cases if self.cases else 0.0

    mrr: float = 0.0
    ndcg: float = 0.0


def score(per_case: Sequence[tuple[Sequence[str], Sequence[str]]], *, k: int) -> EvalReport:
    """Score cases, each a (ranked_facts, expected_substrings) pair."""
    if not per_case:
        return EvalReport(cases=0, hits_at_k=0, k=k, mrr=0.0, ndcg=0.0)
    hits = 0
    reciprocal_sum = 0.0
    ndcg_sum = 0.0
    for ranked_facts, expected in per_case:
        rank = first_hit_rank(ranked_facts, expected)
        if rank is not None:
            reciprocal_sum += 1.0 / rank
            if rank <= k:
                hits += 1
        ndcg_sum += ndcg_at_k(relevances(ranked_facts, expected), k)
    n = len(per_case)
    return EvalReport(cases=n, hits_at_k=hits, k=k, mrr=reciprocal_sum / n, ndcg=ndcg_sum / n)
