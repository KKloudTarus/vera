"""Calibrate rerank weights from logged feedback.

Reads the labeled signal vectors captured with each thumbs up/down and prints the
calibrated weights as ``VERA_RERANK__*`` environment lines. With ``--apply`` it also
persists them as the active weight set (when there are enough samples), which the API and
MCP ranker load at startup, so the loop closes without editing config. With no group
arguments it calibrates across every group that has feedback; pass group_ids to scope it.

    python -m vera.entrypoints.calibrate            # print proposed weights
    python -m vera.entrypoints.calibrate --apply    # also persist them (cron-friendly)
"""

from __future__ import annotations

import asyncio
import sys

from vera.adapters.persistence.repositories.rerank_weights import (
    SqlAlchemyRerankWeightsRepository,
)
from vera.application.queries.calibration import CalibrationService, should_apply
from vera.application.queries.search_memory import RerankWeights
from vera.bootstrap import Container, build_container, build_rerank_weights, dispose_container
from vera.config.settings import get_settings
from vera.observability import configure_logging, get_logger

log = get_logger(__name__)


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


async def calibrate(container: Container, group_ids: list[str]) -> tuple[RerankWeights, int]:
    read_model = container.retrieval_read
    scope = group_ids or await read_model.feedback_groups()
    current = build_rerank_weights(container.settings)
    return await CalibrationService(read_model).calibrate_with_count(
        group_ids=scope, half_life_s=current.half_life_s, fallback=current
    )


async def _run(group_ids: list[str], *, apply: bool) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    try:
        weights, samples = await calibrate(container, group_ids)
        if apply:
            minimum = settings.rerank.min_calibration_samples
            if should_apply(samples, minimum):
                await SqlAlchemyRerankWeightsRepository(container.sessionmaker).save_active(
                    weights, sample_count=samples
                )
                log.info("calibrate.applied", samples=samples)
            else:
                log.warning("calibrate.skipped", samples=samples, minimum=minimum)
    finally:
        await dispose_container(container)
    print(_env_lines(weights))  # operator-facing output


def main() -> None:
    args = sys.argv[1:]
    apply = "--apply" in args
    group_ids = [a for a in args if not a.startswith("--")]
    asyncio.run(_run(group_ids, apply=apply))


if __name__ == "__main__":
    main()
