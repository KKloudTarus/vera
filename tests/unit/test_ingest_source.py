"""IngestSource idempotency, using a fake queue that tracks dedup keys."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

import pytest

from vera.application.commands.ingest_source import IngestSource, IngestSourceHandler
from vera.domain.ports.job_queue import QueuedJob
from vera.shared.errors import is_err, is_ok
from vera.shared.types import GroupId, SourceId


class FakeQueue:
    def __init__(self) -> None:
        self.seen: set[UUID] = set()

    async def enqueue(
        self,
        *,
        group_id: GroupId,
        source_id: SourceId,
        dedup_uuid: UUID,
        payload: dict[str, Any],
        trace_context: dict[str, Any] | None = None,
    ) -> bool:
        if dedup_uuid in self.seen:
            return False
        self.seen.add(dedup_uuid)
        return True

    async def claim(self, *, batch_size: int) -> Sequence[QueuedJob]:
        return []

    async def complete(self, job_id: UUID, *, claim_token: UUID) -> None: ...

    async def fail(
        self, job_id: UUID, *, claim_token: UUID, error: str, retry_in_s: float
    ) -> None: ...

    async def dead_letter(self, job_id: UUID, *, claim_token: UUID, error: str) -> None: ...


@pytest.mark.asyncio
async def test_first_ingest_accepted_second_is_duplicate() -> None:
    handler = IngestSourceHandler(FakeQueue())
    command = IngestSource(SourceId("git:repo:abc:v1"), GroupId("proj:demo"), {})

    first = await handler.handle(command)
    second = await handler.handle(command)

    assert is_ok(first)
    assert is_err(second)
    assert second.error.code == "conflict"
