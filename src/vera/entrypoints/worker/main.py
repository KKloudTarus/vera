"""Ingestion worker: a dispatcher that claims jobs and feeds the lane pool.

The dispatcher claims a batch, routes each job to its group's lane, and blocks on a
full lane (backpressure). A reaper runs periodically to return timed-out in-flight
jobs to pending. Per-group ordering and the dedup-race guard live in the lane pool.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.connectors import SyncRegistration, SyncRunner, SyncScheduler
from vera.bootstrap import Container, build_container, dispose_container
from vera.config.settings import Settings, get_settings
from vera.entrypoints.worker.lane_pool import LanePool
from vera.observability import (
    configure_logging,
    configure_tracing,
    get_logger,
    instrument_worker,
)
from vera.observability.metrics import set_queue_depth, start_metrics_server

log = get_logger(__name__)

_RECLAIM_EVERY_CYCLES = 20
_SYNC_EVERY_CYCLES = 10


def build_sync_registrations(container: Container) -> list[SyncRegistration]:
    """Connector registrations for scheduled sync.

    These are environment-specific (repositories, base URLs, credentials), so they are
    wired here when configured. Empty by default, so the worker runs with no connectors.
    """
    _ = container
    return []


def _build_scheduler(container: Container) -> SyncScheduler | None:
    registrations = build_sync_registrations(container)
    if not registrations:
        return None
    runner = SyncRunner(
        uow_factory=lambda: SqlAlchemyUnitOfWork(container.sessionmaker),
        extractor=container.extractor,
        state=container.sync_state,
        object_store=container.object_store,
    )
    return SyncScheduler(runner=runner, state=container.sync_state, registrations=registrations)


def _build_pool(container: Container, settings: Settings) -> LanePool:
    lanes = max(1, settings.worker.lanes)
    per_lane = max(1, settings.worker.batch_size // lanes + 1)
    return LanePool(container, lanes=lanes, queue_maxsize=per_lane)


async def run_until_empty(container: Container, pool: LanePool, *, batch_size: int) -> int:
    """Claim and process until the queue is drained. Returns the number processed.

    Used by tests and one-shot runs. The long-running worker uses ``run`` instead.
    """
    processed = 0
    while True:
        jobs = await container.queue.claim(batch_size=batch_size)
        if jobs:
            for job in jobs:
                await pool.submit(job)
            processed += len(jobs)
            continue
        # claim() skips groups with an in-flight job (per-group serialization), so a
        # single-group backlog can look empty while jobs remain pending. Drain the
        # in-flight batch, then stop only once nothing is pending.
        await pool.join()
        depth = await container.queue.depth_by_status()
        if depth.get("pending", 0) == 0:
            break
    await pool.join()
    return processed


async def run() -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    configure_tracing(settings)
    instrument_worker()
    container = build_container(settings)
    if settings.observability.metrics_enabled:
        start_metrics_server(settings.observability.worker_metrics_port)
    pool = _build_pool(container, settings)
    pool.start()
    scheduler = _build_scheduler(container)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    log.info("worker.startup", lanes=settings.worker.lanes, batch=settings.worker.batch_size)
    cycles = 0
    try:
        while not stop.is_set():
            cycles += 1
            if cycles % _RECLAIM_EVERY_CYCLES == 0:
                reclaimed = await container.queue.reclaim_stuck()
                if reclaimed:
                    log.info("worker.reclaimed", count=reclaimed)
                set_queue_depth(await container.queue.depth_by_status())
            if scheduler is not None and cycles % _SYNC_EVERY_CYCLES == 0:
                outcomes = await scheduler.run_due()
                if outcomes:
                    log.info("worker.sync", sources=len(outcomes))
            jobs = await container.queue.claim(batch_size=settings.worker.batch_size)
            if not jobs:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(), timeout=settings.worker.poll_interval_ms / 1000
                    )
                continue
            for job in jobs:
                await pool.submit(job)
    finally:
        await pool.stop()
        await dispose_container(container)
        log.info("worker.shutdown")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
