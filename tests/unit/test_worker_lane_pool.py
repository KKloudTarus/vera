from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pytest

from vera.bootstrap import Container
from vera.domain.ports.job_queue import QueuedJob
from vera.entrypoints.worker.lane_pool import LanePool
from vera.observability.cost import UsageAccountingError


@pytest.mark.asyncio
async def test_community_build_has_per_episode_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = cast(
        "Container",
        SimpleNamespace(
            settings=SimpleNamespace(
                worker=SimpleNamespace(visibility_timeout_s=300),
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


@pytest.mark.asyncio
async def test_accounting_failure_dead_letters_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminal = asyncio.Event()

    class Queue:
        def __init__(self) -> None:
            self.dead_letters: list[tuple[object, str]] = []
            self.failures: list[object] = []

        async def dead_letter(self, job_id: object, *, claim_token: object, error: str) -> None:
            self.dead_letters.append((job_id, error))
            terminal.set()

        async def fail(
            self, job_id: object, *, claim_token: object, error: str, retry_in_s: float
        ) -> None:
            self.failures.append(job_id)

    queue = Queue()
    container = cast(
        "Container",
        SimpleNamespace(
            queue=queue,
            settings=SimpleNamespace(
                worker=SimpleNamespace(visibility_timeout_s=300),
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

    async def accounting_failure(_job: QueuedJob) -> None:
        raise UsageAccountingError("durable usage unavailable")

    monkeypatch.setattr(pool, "_process", accounting_failure)
    job = cast(
        "QueuedJob",
        SimpleNamespace(
            id=uuid4(),
            claim_token=uuid4(),
            group_id="project",
            source_id="source",
            payload={},
            trace_context={},
            attempts=1,
        ),
    )

    pool.start()
    try:
        await pool.submit(job)
        await asyncio.wait_for(terminal.wait(), timeout=1)
    finally:
        await pool.stop()

    assert queue.dead_letters == [(job.id, "durable usage unavailable")]
    assert queue.failures == []


@pytest.mark.asyncio
async def test_lane_pool_renews_active_and_locally_queued_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    class Queue:
        def __init__(self) -> None:
            self.renewed: set[tuple[object, object]] = set()
            self.all_renewed = asyncio.Event()

        async def renew(self, job_id: object, *, claim_token: object) -> bool:
            self.renewed.add((job_id, claim_token))
            if len(self.renewed) == 2:
                self.all_renewed.set()
            return True

    queue = Queue()
    container = cast(
        "Container",
        SimpleNamespace(
            queue=queue,
            settings=SimpleNamespace(
                worker=SimpleNamespace(visibility_timeout_s=1),
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
    pool = LanePool(container, lanes=1, queue_maxsize=1, lease_renew_interval_s=0.01)
    jobs = [
        cast(
            "QueuedJob",
            SimpleNamespace(
                id=uuid4(),
                claim_token=uuid4(),
                group_id="project",
                source_id=f"source-{index}",
                payload={},
                trace_context={},
                attempts=1,
            ),
        )
        for index in range(2)
    ]

    async def process(job: QueuedJob) -> None:
        if job.id == jobs[0].id:
            first_started.set()
            await release_first.wait()

    monkeypatch.setattr(pool, "_process", process)
    pool.start()
    try:
        await pool.submit(jobs[0])
        await first_started.wait()
        await pool.submit(jobs[1])
        expected = {(job.id, job.claim_token) for job in jobs}
        await asyncio.wait_for(queue.all_renewed.wait(), timeout=1)
        release_first.set()
        await pool.join()
    finally:
        release_first.set()
        await pool.stop()

    assert expected.issubset(queue.renewed)


@pytest.mark.asyncio
async def test_lease_renewal_errors_fail_closed_before_visibility_expires() -> None:
    class Queue:
        def __init__(self) -> None:
            self.calls = 0

        async def renew(self, _job_id: object, *, claim_token: object) -> bool:
            del claim_token
            self.calls += 1
            raise RuntimeError("database unavailable")

    queue = Queue()
    container = cast(
        "Container",
        SimpleNamespace(
            queue=queue,
            settings=SimpleNamespace(
                worker=SimpleNamespace(visibility_timeout_s=0.03),
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
    pool = LanePool(container, lanes=1, queue_maxsize=1, lease_renew_interval_s=0.005)
    job = cast(
        "QueuedJob",
        SimpleNamespace(id=uuid4(), claim_token=uuid4()),
    )
    lost = asyncio.Event()

    await asyncio.wait_for(
        pool._renew_lease(job, lost),  # pyright: ignore[reportPrivateUsage]
        timeout=0.2,
    )

    assert lost.is_set()
    assert queue.calls >= 2


@pytest.mark.asyncio
async def test_hung_lease_renewal_fails_closed_before_visibility_expires() -> None:
    class Queue:
        async def renew(self, _job_id: object, *, claim_token: object) -> bool:
            del claim_token
            await asyncio.Event().wait()
            return True

    container = cast(
        "Container",
        SimpleNamespace(
            queue=Queue(),
            settings=SimpleNamespace(
                worker=SimpleNamespace(visibility_timeout_s=0.03),
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
    pool = LanePool(container, lanes=1, queue_maxsize=1, lease_renew_interval_s=0.005)
    job = cast("QueuedJob", SimpleNamespace(id=uuid4(), claim_token=uuid4()))
    lost = asyncio.Event()

    await asyncio.wait_for(
        pool._renew_lease(job, lost),  # pyright: ignore[reportPrivateUsage]
        timeout=0.1,
    )

    assert lost.is_set()
