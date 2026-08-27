"""Offline retrieval-quality metrics over a golden set.

A golden case is a query plus the substrings that a correct answer must contain. Running
each query yields a ranked list of facts; the first fact that contains any expected
substring is the hit, and its 1-based rank drives the two standard metrics: hit@k (was a
correct fact in the top k) and MRR (mean reciprocal rank, rewarding a higher position).
Keeping the scoring pure lets a CI gate assert a baseline without a live graph.
"""

from __future__ import annotations

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


@dataclass(frozen=True, slots=True)
class EvalReport:
    cases: int
    hits_at_k: int
    k: int

    @property
    def hit_rate(self) -> float:
        return self.hits_at_k / self.cases if self.cases else 0.0

    mrr: float = 0.0


def score(per_case: Sequence[tuple[Sequence[str], Sequence[str]]], *, k: int) -> EvalReport:
    """Score cases, each a (ranked_facts, expected_substrings) pair."""
    if not per_case:
        return EvalReport(cases=0, hits_at_k=0, k=k, mrr=0.0)
    hits = 0
    reciprocal_sum = 0.0
    for ranked_facts, expected in per_case:
        rank = first_hit_rank(ranked_facts, expected)
        if rank is not None:
            reciprocal_sum += 1.0 / rank
            if rank <= k:
                hits += 1
    return EvalReport(cases=len(per_case), hits_at_k=hits, k=k, mrr=reciprocal_sum / len(per_case))
