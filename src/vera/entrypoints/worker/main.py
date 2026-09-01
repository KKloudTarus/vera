"""Ingestion worker: a dispatcher that claims jobs and feeds the lane pool.

The dispatcher claims a batch, routes each job to its group's lane, and blocks on a
full lane (backpressure). A reaper runs periodically to return timed-out in-flight
jobs to pending. Per-group ordering and the dedup-race guard live in the lane pool.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal

from sqlalchemy import text

from vera.adapters.persistence.repositories import (
    SqlAlchemyFactExpiryRepository,
    SqlAlchemyKnowledgeEventLog,
    SqlAlchemyOutboxRepository,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.connectors import SyncRegistration, SyncRunner, SyncScheduler
from vera.application.curation.reconciliation import FactExpiryService
from vera.bootstrap import Container, build_container, dispose_container, verify_ontology
from vera.config.settings import Settings, active_embedding, get_settings
from vera.entrypoints.worker.lane_pool import LanePool
from vera.observability import (
    configure_logging,
    configure_tracing,
    get_logger,
    instrument_worker,
)
from vera.observability.metrics import note_backpressure, set_queue_depth, start_metrics_server
from vera.shared.errors import VeraError
from vera.shared.ids import uuid7

log = get_logger(__name__)

_RECLAIM_EVERY_CYCLES = 20
_SYNC_EVERY_CYCLES = 10


def build_sync_registrations(container: Container) -> list[SyncRegistration]:
    """Connector registrations for scheduled sync, from ``settings.connectors.specs``.

    Empty by default, so the worker runs with no connectors until configured. A spec that
    is malformed or whose secret is missing is skipped and logged (never with the secret),
    so one bad connector does not stop the worker from serving the rest.
    """
    from uuid import UUID

    from vera.adapters.connectors.registry import build_connector

    registrations: list[SyncRegistration] = []
    for index, spec in enumerate(container.settings.connectors.specs):
        kind = str(spec.get("kind", "?"))
        try:
            registration = SyncRegistration(
                source_id=UUID(str(spec["source_id"])),
                group_id=str(spec["group_id"]),
                connector=build_connector(spec),
                interval_s=float(str(spec.get("interval_s", 3600))),
            )
        except (KeyError, ValueError, VeraError) as exc:
            # Redacted: log the position and kind, never the spec (it may carry a token).
            log.warning("connector.spec_skipped", index=index, kind=kind, reason=str(exc))
            continue
        registrations.append(registration)
        log.info(
            "connector.registered",
            kind=kind,
            source_id=str(registration.source_id),
            group_id=registration.group_id,
            interval_s=registration.interval_s,
        )
    return registrations


def _build_scheduler(container: Container) -> SyncScheduler | None:
    registrations = build_sync_registrations(container)
    if not registrations:
        return None
    embedding_model, embedding_dimension = active_embedding(container.settings)
    runner = SyncRunner(
        uow_factory=lambda: SqlAlchemyUnitOfWork(container.sessionmaker),
        extractor=container.extractor,
        state=container.sync_state,
        object_store=container.object_store,
        judge=container.judge,
        embedder=(container.embedder if container.settings.memory.vector_search_enabled else None),
        embedding_provider=container.settings.memory.embedder,
        embedding_model=embedding_model,
        embedding_model_version=container.settings.memory.embedding_model_version,
        embedding_dimension=embedding_dimension,
    )
    return SyncScheduler(
        runner=runner,
        state=container.sync_state,
        uow_factory=lambda: SqlAlchemyUnitOfWork(container.sessionmaker),
        registrations=registrations,
    )


def _build_pool(container: Container, settings: Settings) -> LanePool:
    lanes = max(1, settings.worker.lanes)
    per_lane = max(1, settings.worker.batch_size // lanes + 1)
    return LanePool(container, lanes=lanes, queue_maxsize=per_lane)


async def expire_due_facts(container: Container) -> int:
    """Expire stale governed facts and transactionally enqueue their graph cleanup."""
    async with container.workers() as session, session.begin():
        report = await FactExpiryService(
            facts=SqlAlchemyFactExpiryRepository(session),
            events=SqlAlchemyKnowledgeEventLog(session),
        ).run()
        if container.fact_projection is not None:
            outbox = SqlAlchemyOutboxRepository(session)
            for group_id in report.group_ids:
                await outbox.add(
                    group_id=group_id,
                    source_id=f"ttl:{group_id}",
                    dedup_uuid=uuid7(),
                    payload={"job_kind": "project_facts", "group_id": group_id},
                )
        return report.expired


async def delete_expired_context_packs(container: Container) -> int:
    """Delete expired context packs through the worker-only erasure function."""
    async with container.workers() as session, session.begin():
        removed = await session.scalar(text("SELECT delete_all_expired_context_packs()"))
        return int(removed or 0)


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
    # Fail fast if code and the persisted ontology disagree before the worker reconciles.
    await verify_ontology(container)
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
                depths = await container.queue.depth_by_status()
                set_queue_depth(depths)
                threshold = settings.worker.queue_depth_alert_threshold
                if note_backpressure(depths, threshold):
                    log.warning(
                        "worker.queue_backpressure",
                        pending=depths.get("pending", 0),
                        threshold=threshold,
                    )
                expired = await expire_due_facts(container)
                if expired:
                    log.info("worker.facts_expired", count=expired)
                deleted_packs = await delete_expired_context_packs(container)
                if deleted_packs:
                    log.info("worker.context_packs_deleted", count=deleted_packs)
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
