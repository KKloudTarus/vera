"""CalibrationService turns logged feedback signal vectors into rerank weights."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import pytest

from vera.application.queries.calibration import CalibrationService, _to_sample
from vera.domain.ports.retrieval import LabeledSignals

_HL = 30 * 24 * 3600.0


def test_to_sample_defaults_missing_signals_to_neutral() -> None:
    sample = _to_sample(LabeledSignals(signals={"authority": 1.0}, label=1))
    assert sample.authority == 1.0
    assert sample.relevance == 0.5  # not logged -> neutral
    assert sample.label == 1


class _FakeReadModel:
    def __init__(self, labeled: list[LabeledSignals]) -> None:
        self._labeled = labeled

    async def calibration_samples(
        self, *, group_ids: Sequence[str], since: datetime | None = None
    ) -> list[LabeledSignals]:
        return self._labeled


@pytest.mark.asyncio
async def test_authority_signal_earns_weight_from_real_feedback() -> None:
    # Helpful results had high authority; unhelpful had low. Nothing else separates.
    labeled = [
        LabeledSignals({"authority": 1.0, "relevance": 0.5}, label=1),
        LabeledSignals({"authority": 0.9, "relevance": 0.5}, label=1),
        LabeledSignals({"authority": 0.1, "relevance": 0.5}, label=-1),
        LabeledSignals({"authority": 0.0, "relevance": 0.5}, label=-1),
    ]
    service = CalibrationService(_FakeReadModel(labeled))
    weights = await service.calibrate(group_ids=["p:demo"], half_life_s=_HL)
    assert weights.authority > 0.9
    assert weights.relevance == 0.0
    assert weights.half_life_s == _HL


@pytest.mark.asyncio
async def test_falls_back_when_no_labeled_feedback() -> None:
    service = CalibrationService(_FakeReadModel([]))
    weights = await service.calibrate(group_ids=["p:demo"], half_life_s=_HL)
    # No samples -> default weights preserved unchanged.
    assert weights.relevance == 0.40
    assert weights.half_life_s == _HL
