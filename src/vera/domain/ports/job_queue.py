"""The ``JobQueue`` port: a vendor-neutral async ingestion queue.

Default adapter is Postgres-native (transactional outbox + ``FOR UPDATE SKIP
LOCKED``); no broker, no vendor lock-in. If throughput ever demands a dedicated
broker, an open, self-hostable one (Redpanda/Kafka with ``partition key =
group_id``, or NATS JetStream) implements this same port, never a single-cloud
service.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from uuid import UUID

from vera.shared.types import GroupId, JsonDict, SourceId, empty_json


@dataclass(frozen=True, slots=True)
class QueuedJob:
    id: UUID
    group_id: GroupId
    source_id: SourceId
    dedup_uuid: UUID
    payload: JsonDict
    attempts: int
    created_at: datetime
    trace_context: JsonDict = field(default_factory=empty_json)


class JobQueue(Protocol):
    async def enqueue(
        self,
        *,
        group_id: GroupId,
        source_id: SourceId,
        dedup_uuid: UUID,
        payload: JsonDict,
        trace_context: JsonDict | None = None,
    ) -> bool:
        """Enqueue a job idempotently. Returns False if ``dedup_uuid`` already exists."""
        ...

    async def claim(self, *, batch_size: int) -> Sequence[QueuedJob]:
        """Claim a batch of ready jobs (oldest-pending, skipping groups in flight)."""
        ...

    async def complete(self, job_id: UUID) -> None:
        """Mark a job done."""
        ...

    async def fail(self, job_id: UUID, *, error: str, retry_in_s: float) -> None:
        """Reschedule a transient failure, or dead-letter once attempts are exhausted."""
        ...

    async def release(self, job_id: UUID, *, reason: str) -> None:
        """Return a claimed job to pending without consuming an attempt."""
        ...

    async def reclaim_stuck(self) -> int:
        """Return timed-out in-flight jobs to pending. Returns how many were reclaimed."""
        ...

    async def depth_by_status(self) -> dict[str, int]:
        """Count jobs by status (pending, inflight, dead, done) for the queue-depth gauge."""
        ...
