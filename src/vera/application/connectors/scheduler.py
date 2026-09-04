"""SyncScheduler: run each registered connector when its interval is due.

A source is due when it has never synced or its last cursor is older than its interval.
The worker calls ``run_due`` periodically; because each run is incremental and ingestion
is content-idempotent, a due run that finds no changes updates nothing.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from vera.application.connectors.service import SyncRunner, UnitOfWorkFactory
from vera.domain.ports.connectors import SourceConnector, SyncOutcome, SyncStateStore
from vera.observability import get_logger
from vera.shared.ids import uuid7
from vera.shared.time import utc_now

log = get_logger(__name__)

_DEFAULT_LEASE_DURATION_S = 300.0


@dataclass(frozen=True, slots=True)
class SyncRegistration:
    source_id: UUID
    group_id: str
    connector: SourceConnector
    interval_s: float


class SyncScheduler:
    def __init__(
        self,
        *,
        runner: SyncRunner,
        state: SyncStateStore,
        uow_factory: UnitOfWorkFactory,
        registrations: list[SyncRegistration],
        lease_duration_s: float = _DEFAULT_LEASE_DURATION_S,
    ) -> None:
        if lease_duration_s <= 0:
            raise ValueError("lease_duration_s must be positive")
        self._runner = runner
        self._state = state
        self._uow_factory = uow_factory
        self._registrations = registrations
        self._lease_duration_s = lease_duration_s

    async def _is_due(self, registration: SyncRegistration, at: datetime) -> bool:
        last = await self._state.last_synced_at(registration.source_id)
        if last is None:
            return True  # never synced: run a full backfill
        return (at - last).total_seconds() >= registration.interval_s

    async def _claim_lease(self, source_id: UUID, owner_token: UUID) -> bool:
        async with self._uow_factory() as uow:
            claimed = await uow.sources.claim_sync_lease(
                source_id=source_id,
                owner_token=owner_token,
                lease_duration_s=self._lease_duration_s,
            )
            await uow.commit()
        return claimed

    async def _renew_lease(self, source_id: UUID, owner_token: UUID) -> bool:
        async with self._uow_factory() as uow:
            renewed = await uow.sources.renew_sync_lease(
                source_id=source_id,
                owner_token=owner_token,
                lease_duration_s=self._lease_duration_s,
            )
            await uow.commit()
        return renewed

    async def _release_lease(self, source_id: UUID, owner_token: UUID) -> None:
        async with self._uow_factory() as uow:
            await uow.sources.release_sync_lease(
                source_id=source_id,
                owner_token=owner_token,
            )
            await uow.commit()

    async def _renew_until_done(
        self,
        *,
        source_id: UUID,
        owner_token: UUID,
        stop: asyncio.Event,
        lease_lost: asyncio.Event,
        sync_task: asyncio.Task[SyncOutcome],
    ) -> None:
        renewal_interval_s = self._lease_duration_s / 3
        while True:
            try:
                async with asyncio.timeout(renewal_interval_s):
                    await stop.wait()
                return
            except TimeoutError:
                pass
            try:
                async with asyncio.timeout(renewal_interval_s):
                    renewed = await self._renew_lease(source_id, owner_token)
            except asyncio.CancelledError:
                raise
            except Exception:
                renewed = False
            if not renewed:
                lease_lost.set()
                sync_task.cancel()
                return

    async def _sync_with_lease(
        self, registration: SyncRegistration, owner_token: UUID
    ) -> SyncOutcome:
        stop = asyncio.Event()
        lease_lost = asyncio.Event()
        async with asyncio.TaskGroup() as tasks:
            sync_task = tasks.create_task(
                self._runner.sync(
                    source_id=registration.source_id,
                    group_id=registration.group_id,
                    connector=registration.connector,
                )
            )
            tasks.create_task(
                self._renew_until_done(
                    source_id=registration.source_id,
                    owner_token=owner_token,
                    stop=stop,
                    lease_lost=lease_lost,
                    sync_task=sync_task,
                )
            )
            try:
                outcome = await sync_task
            except asyncio.CancelledError:
                if lease_lost.is_set():
                    raise RuntimeError(
                        f"connector lease lost for source {registration.source_id}"
                    ) from None
                raise
            finally:
                stop.set()
        return outcome

    async def _run_if_claimed(
        self, registration: SyncRegistration, due_at: datetime
    ) -> SyncOutcome | None:
        owner_token = uuid7()
        if not await self._claim_lease(registration.source_id, owner_token):
            return None
        try:
            # A competing worker may have completed after the first due check but before
            # this claim. Its checkpoint is newer than due_at, so skip the stale attempt.
            if not await self._is_due(registration, due_at):
                return None
            # The due check may block past the lease deadline. Renewing also proves this
            # worker still owns the source before synchronization can create side effects.
            try:
                async with asyncio.timeout(self._lease_duration_s / 3):
                    renewed = await self._renew_lease(registration.source_id, owner_token)
            except TimeoutError:
                return None
            if not renewed:
                return None
            return await self._sync_with_lease(registration, owner_token)
        finally:
            try:
                await self._release_lease(registration.source_id, owner_token)
            except Exception:
                log.exception(
                    "connector.lease_release_failed",
                    source_id=str(registration.source_id),
                )

    async def run_due(self, *, at: datetime | None = None) -> list[SyncOutcome]:
        moment = at or utc_now()
        outcomes: list[SyncOutcome] = []
        for registration in self._registrations:
            if await self._is_due(registration, moment):
                try:
                    outcome = await self._run_if_claimed(registration, moment)
                except Exception:
                    log.exception(
                        "connector.sync_failed",
                        source_id=str(registration.source_id),
                        kind=registration.connector.kind,
                    )
                    continue
                if outcome is not None:
                    outcomes.append(outcome)
        return outcomes
