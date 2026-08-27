"""Propose rerank weights from logged feedback.

Reads the labeled signal vectors captured with each thumbs up/down and prints the
calibrated weights as the ``VERA_RERANK__*`` environment lines an operator sets to apply
them. Printing env, rather than writing a store, keeps the change explicit and reviewable
and matches how the weights are configured. With no group arguments it calibrates across
every group that has feedback; pass group_ids to scope it.
"""

from __future__ import annotations

import asyncio
import sys

from vera.application.queries.calibration import CalibrationService
from vera.application.queries.search_memory import RerankWeights
from vera.bootstrap import Container, build_container, build_rerank_weights, dispose_container
from vera.config.settings import get_settings
from vera.observability import configure_logging


def _env_lines(w: RerankWeights) -> str:
    days = w.half_life_s / 86400.0
    return "\n".join(
        (
            f"VERA_RERANK__W_RELEVANCE={w.relevance:.4f}",
            f"VERA_RERANK__W_AUTHORITY={w.authority:.4f}",
            f"VERA_RERANK__W_VERIFICATION={w.verification:.4f}",
            f"VERA_RERANK__W_RECENCY={w.recency:.4f}",
            f"VERA_RERANK__W_FEEDBACK={w.feedback:.4f}",
            f"VERA_RERANK__W_CONFIDENCE={w.confidence:.4f}",
            f"VERA_RERANK__RECENCY_HALF_LIFE_DAYS={days:.2f}",
        )
    )


async def calibrate(container: Container, group_ids: list[str]) -> RerankWeights:
    read_model = container.retrieval_read
    scope = group_ids or await read_model.feedback_groups()
    service = CalibrationService(read_model)
    current = build_rerank_weights(container.settings)
    return await service.calibrate(
        group_ids=scope, half_life_s=current.half_life_s, fallback=current
    )


async def _run(group_ids: list[str]) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    try:
        weights = await calibrate(container, group_ids)
    finally:
        await dispose_container(container)
    print(_env_lines(weights))  # operator-facing output


def main() -> None:
    asyncio.run(_run(sys.argv[1:]))


if __name__ == "__main__":
    main()
