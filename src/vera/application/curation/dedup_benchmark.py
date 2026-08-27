"""Measure the semantic-dedup threshold against labeled entity-name pairs.

Semantic linking merges two surface forms when their canonical-name embeddings have
cosine similarity at or above a threshold. That threshold trades precision (wrong
merges) against recall (missed synonyms and translations). This evaluates a labeled set
of pairs, where each pair is marked as the same real-world entity or not, so an operator
can pick the threshold from evidence rather than a guess. Feed it real names and a real
embedder to tune on production data, or vectors directly in a test.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from vera.application.curation.entity_resolver import cosine
from vera.domain.ports.embedder import Embedder

# A pair: the two embeddings and whether they are truly the same entity.
LabeledPair = tuple[list[float], list[float], bool]


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    threshold: float
    tp: int
    fp: int
    fn: int
    tn: int

    @property
    def precision(self) -> float:
        predicted = self.tp + self.fp
        return self.tp / predicted if predicted else 1.0

    @property
    def recall(self) -> float:
        actual = self.tp + self.fn
        return self.tp / actual if actual else 1.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0


def evaluate_pairs(pairs: Sequence[LabeledPair], threshold: float) -> BenchmarkResult:
    tp = fp = fn = tn = 0
    for a, b, same in pairs:
        linked = cosine(a, b) >= threshold
        if linked and same:
            tp += 1
        elif linked and not same:
            fp += 1
        elif not linked and same:
            fn += 1
        else:
            tn += 1
    return BenchmarkResult(threshold=threshold, tp=tp, fp=fp, fn=fn, tn=tn)


def sweep(pairs: Sequence[LabeledPair], thresholds: Sequence[float]) -> list[BenchmarkResult]:
    return [evaluate_pairs(pairs, t) for t in thresholds]


def best_by_f1(results: Sequence[BenchmarkResult]) -> BenchmarkResult:
    if not results:
        raise ValueError("no results to choose from")
    return max(results, key=lambda r: r.f1)


async def benchmark_names(
    embedder: Embedder,
    labeled: Sequence[tuple[str, str, bool]],
    thresholds: Sequence[float],
) -> list[BenchmarkResult]:
    """Embed each distinct name once, then sweep the thresholds over the labeled pairs."""
    vectors: dict[str, list[float]] = {}
    for left, right, _ in labeled:
        for name in (left, right):
            if name not in vectors:
                vectors[name] = await embedder.embed(name)
    pairs: list[LabeledPair] = [(vectors[a], vectors[b], same) for a, b, same in labeled]
    return sweep(pairs, thresholds)
