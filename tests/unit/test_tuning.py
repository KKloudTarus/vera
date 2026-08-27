"""Rerank weight calibration from labeled feedback samples."""

from __future__ import annotations

from vera.application.queries.search_memory import RerankWeights
from vera.application.queries.tuning import RerankSample, calibrate_weights

_HL = 30 * 24 * 3600.0


_FIELDS = ("relevance", "authority", "verification", "recency", "feedback", "confidence")


def _sample(label: int, **signals: float) -> RerankSample:
    base = dict.fromkeys(_FIELDS, 0.5)
    base.update(signals)
    return RerankSample(label=label, **base)  # type: ignore[arg-type]


def test_a_discriminative_signal_earns_the_weight() -> None:
    # Authority perfectly separates helpful from unhelpful; nothing else varies.
    samples = [
        _sample(+1, authority=1.0),
        _sample(+1, authority=0.9),
        _sample(-1, authority=0.0),
        _sample(-1, authority=0.1),
    ]
    w = calibrate_weights(samples, half_life_s=_HL)
    assert w.authority > 0.9  # dominates
    assert w.relevance == 0.0 and w.recency == 0.0
    assert (
        abs(
            w.relevance + w.authority + w.verification + w.recency + w.feedback + w.confidence - 1.0
        )
        < 1e-9
    )
    assert w.half_life_s == _HL


def test_falls_back_without_both_classes() -> None:
    default = RerankWeights(half_life_s=_HL)
    only_positive = [_sample(+1, authority=1.0)]
    assert calibrate_weights(only_positive, half_life_s=_HL) == default


def test_falls_back_when_nothing_separates() -> None:
    default = RerankWeights(half_life_s=_HL)
    samples = [_sample(+1), _sample(-1)]  # identical signals
    assert calibrate_weights(samples, half_life_s=_HL) == default


def test_two_signals_share_weight_proportionally() -> None:
    samples = [
        _sample(+1, relevance=1.0, confidence=1.0),
        _sample(-1, relevance=0.0, confidence=0.5),
    ]
    w = calibrate_weights(samples, half_life_s=_HL)
    # relevance separates by 1.0, confidence by 0.5 -> 2:1 split.
    assert abs(w.relevance - 2 / 3) < 1e-9
    assert abs(w.confidence - 1 / 3) < 1e-9
