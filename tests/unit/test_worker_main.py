from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest

from vera.bootstrap import Container
from vera.domain.ports.job_queue import QueuedJob
from vera.entrypoints.worker import main as worker_main
from vera.entrypoints.worker.lane_pool import LanePool
from vera.shared.types import GroupId, SourceId


@pytest.mark.asyncio
async def test_transient_poll_failure_does_not_stop_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    warnings: list[dict[str, str]] = []
    disposed = False

    class _StopEvent:
        def __init__(self) -> None:
            self.stopped = False

        def is_set(self) -> bool:
            return self.stopped

        def set(self) -> None:
            self.stopped = True

        async def wait(self) -> None:
            self.stopped = True

    class _Queue:
        calls = 0

        async def claim(self, *, batch_size: int) -> list[object]:
            assert batch_size == 1
            self.calls += 1
            raise ConnectionError

    class _Pool:
        started = False
        stopped = False
        joined = False

        def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

        async def join(self) -> None:
            self.joined = True

    settings = SimpleNamespace(
        log_json=False,
        log_level="INFO",
        observability=SimpleNamespace(metrics_enabled=False),
        worker=SimpleNamespace(
            lanes=1,
            batch_size=1,
            poll_interval_ms=1,
            queue_depth_alert_threshold=1,
            shutdown_grace_s=1,
            community_build_enabled=False,
            community_build_group_id=None,
            community_build_interval_cycles=1200,
        ),
    )
    queue = _Queue()
    container = cast("Container", SimpleNamespace(queue=queue))
    pool = _Pool()

    async def _verify(_container: Container) -> None:
        return None

    async def _dispose(_container: Container) -> None:
        nonlocal disposed
        disposed = True

    monkeypatch.setattr(worker_main, "get_settings", lambda: settings)
    monkeypatch.setattr(worker_main, "configure_logging", lambda **_values: None)
    monkeypatch.setattr(worker_main, "configure_tracing", lambda _settings: None)
    monkeypatch.setattr(worker_main, "instrument_worker", lambda: None)
    monkeypatch.setattr(worker_main, "build_container", lambda _settings: container)
    monkeypatch.setattr(worker_main, "verify_ontology", _verify)
    monkeypatch.setattr(worker_main, "_build_pool", lambda *_values: pool)
    monkeypatch.setattr(worker_main, "_build_scheduler", lambda _container: None)
    monkeypatch.setattr(worker_main, "dispose_container", _dispose)
    monkeypatch.setattr(worker_main.asyncio, "Event", _StopEvent)
    monkeypatch.setattr(
        worker_main.asyncio,
        "get_running_loop",
        lambda: SimpleNamespace(add_signal_handler=lambda *_values: None),
    )
    monkeypatch.setattr(
        worker_main,
        "log",
        SimpleNamespace(
            info=lambda *_args, **_values: None,
            warning=lambda _event, **values: warnings.append(values),
        ),
    )

    await worker_main.run()

    assert queue.calls == 1
    assert pool.started
    assert pool.joined
    assert pool.stopped
    assert disposed
    assert warnings == [{"error_type": "ConnectionError"}]


@pytest.mark.asyncio
async def test_shutdown_releases_jobs_blocked_by_lane_backpressure() -> None:
    released: list[UUID] = []
    submit_started = asyncio.Event()

    class _Queue:
        async def release(self, job_id: UUID, *, reason: str) -> None:
            assert reason == "worker shutdown"
            released.append(job_id)

    class _Pool:
        async def submit(self, _job: QueuedJob) -> None:
            submit_started.set()
            await asyncio.Future[None]()

    jobs = [
        QueuedJob(
            id=UUID(int=value),
            group_id=GroupId("group"),
            source_id=SourceId(f"source:{value}"),
            dedup_uuid=UUID(int=value + 10),
            payload={},
            attempts=0,
            created_at=datetime.now(UTC),
        )
        for value in (1, 2)
    ]
    stop = asyncio.Event()
    submitting = asyncio.create_task(
        worker_main._submit_claimed_jobs(
            cast("Container", SimpleNamespace(queue=_Queue())),
            cast("LanePool", _Pool()),
            jobs,
            stop,
        )
    )

    await submit_started.wait()
    stop.set()

    assert await asyncio.wait_for(submitting, timeout=1.0) is False
    assert released == [job.id for job in jobs]
