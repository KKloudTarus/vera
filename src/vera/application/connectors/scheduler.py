"""SyncScheduler: run each registered connector when its interval is due.

A source is due when it has never synced or its last cursor is older than its interval.
The worker calls ``run_due`` periodically; because each run is incremental and ingestion
is content-idempotent, a due run that finds no changes updates nothing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from vera.application.connectors.service import SyncRunner
from vera.domain.ports.connectors import SourceConnector, SyncOutcome, SyncStateStore
from vera.observability import get_logger
from vera.shared.time import utc_now

log = get_logger(__name__)


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
        registrations: list[SyncRegistration],
    ) -> None:
        self._runner = runner
        self._state = state
        self._registrations = registrations

    async def _is_due(self, registration: SyncRegistration, at: datetime) -> bool:
        last = await self._state.last_synced_at(registration.source_id)
        if last is None:
            return True  # never synced: run a full backfill
        return (at - last).total_seconds() >= registration.interval_s

    async def run_due(self, *, at: datetime | None = None) -> list[SyncOutcome]:
        moment = at or utc_now()
        outcomes: list[SyncOutcome] = []
        for registration in self._registrations:
            if await self._is_due(registration, moment):
                try:
                    outcome = await self._runner.sync(
                        source_id=registration.source_id,
                        group_id=registration.group_id,
                        connector=registration.connector,
                    )
                except Exception:
                    log.exception(
                        "connector.sync_failed",
                        source_id=str(registration.source_id),
                        kind=registration.connector.kind,
                    )
                    continue
                outcomes.append(outcome)
        return outcomes
