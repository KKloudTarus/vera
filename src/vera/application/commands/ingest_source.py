"""IngestSource: admit a source for asynchronous, idempotent ingestion.

Demonstrates the command shape: a use case that depends only on ports, derives the
deterministic idempotency key, and enqueues durably. The heavy work (Graphiti
add_episode) happens later in the worker; the request path just records intent.
"""

from __future__ import annotations

from dataclasses import dataclass

from vera.domain.ports.job_queue import JobQueue
from vera.shared.errors import Conflict, Err, Ok, Result
from vera.shared.ids import deterministic_id
from vera.shared.types import GroupId, JsonDict, SourceId


@dataclass(frozen=True, slots=True)
class IngestSource:
    """Command input."""

    source_id: SourceId
    group_id: GroupId
    payload: JsonDict
    trace_context: JsonDict | None = None


@dataclass(frozen=True, slots=True)
class IngestAccepted:
    """Command output: the deterministic dedup id the job and episode will carry."""

    dedup_uuid: str


class IngestSourceHandler:
    def __init__(self, queue: JobQueue) -> None:
        self._queue = queue

    async def handle(self, cmd: IngestSource) -> Result[IngestAccepted, Conflict]:
        dedup_uuid = deterministic_id(str(cmd.source_id))
        admitted = await self._queue.enqueue(
            group_id=cmd.group_id,
            source_id=cmd.source_id,
            dedup_uuid=dedup_uuid,
            payload=cmd.payload,
            trace_context=cmd.trace_context,
        )
        if not admitted:
            return Err(Conflict(f"source {cmd.source_id} already ingested"))
        return Ok(IngestAccepted(dedup_uuid=str(dedup_uuid)))
