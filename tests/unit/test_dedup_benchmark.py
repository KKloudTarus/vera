"""The dedup benchmark scores a threshold's precision and recall on labeled pairs."""

from __future__ import annotations

import pytest

from vera.application.curation.dedup_benchmark import (
    benchmark_names,
    best_by_f1,
    evaluate_pairs,
    sweep,
)


class _FakeEmbedder:
    def __init__(self, vectors: dict[str, list[float]]) -> None:
        self._vectors = vectors

    async def embed(self, text: str) -> list[float]:
        return self._vectors[text]


# Three pairs: two synonyms (cos ~0.95), a cross-lingual match (~0.9), an unrelated pair (0).
_A = [1.0, 0.0, 0.0]
_SYN = [0.95, 0.31, 0.0]
_XLING = [0.9, 0.44, 0.0]
_OTHER = [0.0, 1.0, 0.0]
_NEAR = [0.7, 0.71, 0.0]  # cosine ~0.70 with _A: related-looking but a different entity


def test_a_strict_threshold_trades_recall_for_precision() -> None:
    pairs = [(_A, _SYN, True), (_A, _XLING, True), (_A, _OTHER, False)]
    strict = evaluate_pairs(pairs, threshold=0.97)  # links nothing
    assert strict.tp == 0 and strict.fp == 0 and strict.fn == 2
    assert strict.precision == 1.0  # no wrong merges
    assert strict.recall == 0.0  # but missed both true matches


def test_a_good_threshold_catches_all_true_pairs_without_false_merges() -> None:
    pairs = [(_A, _SYN, True), (_A, _XLING, True), (_A, _OTHER, False)]
    result = evaluate_pairs(pairs, threshold=0.86)
    assert result.tp == 2 and result.fp == 0 and result.fn == 0 and result.tn == 1
    assert result.precision == 1.0
    assert result.recall == 1.0
    assert result.f1 == 1.0


def test_sweep_and_best_by_f1_pick_the_strongest_threshold() -> None:
    # _NEAR is similar enough to be linked by a loose threshold but is a different entity,
    # so 0.5 makes a false merge, 0.97 misses true matches, and 0.86 separates cleanly.
    pairs = [(_A, _SYN, True), (_A, _XLING, True), (_A, _NEAR, False), (_A, _OTHER, False)]
    results = sweep(pairs, [0.5, 0.86, 0.97])
    assert evaluate_pairs(pairs, 0.5).fp == 1  # loose threshold wrongly merges _NEAR
    best = best_by_f1(results)
    assert best.threshold == 0.86


@pytest.mark.asyncio
async def test_benchmark_names_embeds_once_and_scores() -> None:
    embedder = _FakeEmbedder(
        {"paymentapi": _A, "payment service": _SYN, "dich vu thanh toan": _XLING, "billing": _OTHER}
    )
    labeled = [
        ("paymentapi", "payment service", True),
        ("paymentapi", "dich vu thanh toan", True),
        ("paymentapi", "billing", False),
    ]
    results = await benchmark_names(embedder, labeled, [0.86])
    assert results[0].precision == 1.0 and results[0].recall == 1.0
