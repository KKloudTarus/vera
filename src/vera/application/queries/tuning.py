"""Calibrate rerank weights from labeled feedback samples.

Each sample pairs a hit's signal values with a label (+1 for a helpful result, -1 for
an unhelpful one, from thumbs up/down). A signal that is higher on helpful hits than on
unhelpful ones is discriminative and earns weight; a signal that does not separate the
two earns none. Weights are the non-negative discriminability scores, normalized.

This is a transparent, deterministic calibration (mean-difference per signal), not a
black-box model, so an operator can read why a weight moved. Collect samples by logging
each returned hit's signals alongside the feedback later given on it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from vera.application.queries.search_memory import RerankWeights

_SIGNALS = ("relevance", "authority", "verification", "recency", "feedback", "confidence")


@dataclass(frozen=True, slots=True)
class RerankSample:
    relevance: float
    authority: float
    verification: float
    recency: float
    feedback: float
    confidence: float
    label: int  # +1 helpful, -1 unhelpful


def calibrate_weights(
    samples: Sequence[RerankSample], *, half_life_s: float, fallback: RerankWeights | None = None
) -> RerankWeights:
    """Return weights whose values are each signal's mean-difference (helpful minus
    unhelpful), floored at zero and normalized. Falls back to defaults if there is not
    at least one helpful and one unhelpful sample.
    """
    base = fallback or RerankWeights(half_life_s=half_life_s)
    positives = [s for s in samples if s.label > 0]
    negatives = [s for s in samples if s.label < 0]
    if not positives or not negatives:
        return base

    scores: dict[str, float] = {}
    for signal in _SIGNALS:
        pos = sum(getattr(s, signal) for s in positives) / len(positives)
        neg = sum(getattr(s, signal) for s in negatives) / len(negatives)
        scores[signal] = max(0.0, pos - neg)

    total = sum(scores.values())
    if total <= 0:
        return base  # nothing separates the classes; keep the prior
    return RerankWeights(
        relevance=scores["relevance"] / total,
        authority=scores["authority"] / total,
        verification=scores["verification"] / total,
        recency=scores["recency"] / total,
        feedback=scores["feedback"] / total,
        confidence=scores["confidence"] / total,
        half_life_s=half_life_s,
    )
