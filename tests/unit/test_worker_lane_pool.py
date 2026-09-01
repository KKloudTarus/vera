from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import cast

import pytest

from vera.bootstrap import Container
from vera.domain.ports.job_queue import QueuedJob
from vera.entrypoints.worker.lane_pool import LanePool


@pytest.mark.asyncio
async def test_community_build_has_per_episode_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = cast(
        "Container",
        SimpleNamespace(
            settings=SimpleNamespace(
                resilience=SimpleNamespace(per_episode_timeout_s=0.001),
                memory=SimpleNamespace(
                    semantic_dedup_threshold=0.9,
                    semantic_dedup_block_threshold=0.8,
                    semantic_dedup_enabled=False,
                ),
            ),
            embedder=None,
            entity_judge=None,
        ),
    )
    pool = LanePool(container, lanes=1, queue_maxsize=1)

    async def blocked(_job: QueuedJob) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(pool, "_process_community_build", blocked)
    job = cast("QueuedJob", SimpleNamespace(payload={"job_kind": "build_communities"}))

    with pytest.raises(TimeoutError):
        await pool._process(job)  # pyright: ignore[reportPrivateUsage]
