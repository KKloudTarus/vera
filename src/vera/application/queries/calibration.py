"""Calibrate rerank weights from real feedback.

The search handler logs each returned hit's signal vector; when a caller thumbs a result
up or down, that vector is stored with the label. This reads those labeled vectors back
and runs the transparent mean-difference calibration to propose new weights. It is the
bridge between logged feedback and the tunable ``RerankWeights`` the handler uses.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from vera.application.queries.search_memory import RerankWeights
from vera.application.queries.tuning import RerankSample, calibrate_weights
from vera.domain.ports.retrieval import LabeledSignals, RetrievalReadModel

_SIGNALS = ("relevance", "authority", "verification", "recency", "feedback", "confidence")


def _to_sample(labeled: LabeledSignals) -> RerankSample:
    signals = labeled.signals or {}
    # A missing signal defaults to the neutral 0.5 the handler uses when a hit lacks it.
    values = {name: float(signals.get(name, 0.5)) for name in _SIGNALS}
    return RerankSample(label=labeled.label, **values)


class CalibrationService:
    def __init__(self, read_model: RetrievalReadModel) -> None:
        self._read_model = read_model

    async def calibrate(
        self,
        *,
        group_ids: Sequence[str],
        half_life_s: float,
        fallback: RerankWeights | None = None,
        since: datetime | None = None,
    ) -> RerankWeights:
        labeled = await self._read_model.calibration_samples(group_ids=group_ids, since=since)
        samples = [_to_sample(item) for item in labeled]
        return calibrate_weights(samples, half_life_s=half_life_s, fallback=fallback)
