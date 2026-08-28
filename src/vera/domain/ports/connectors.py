"""Connector ports: pulling records from external systems into the pipeline.

A connector maps a source system's records to ``ConnectorRecord``s with a stable
external id, and supports incremental sync: given the cursor from the last run it
returns only what changed. The sync state store persists cursors and job outcomes, so
a scheduled run resumes where the last one stopped and never reprocesses unchanged data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from uuid import UUID

from vera.shared.types import JsonDict, empty_json


@dataclass(frozen=True, slots=True)
class ConnectorRecord:
    external_id: str  # stable within the source, so re-sync is idempotent
    body: str
    knowledge_type: str = "text"
    title: str | None = None
    metadata: JsonDict = field(default_factory=empty_json)
    reference_time: datetime | None = None
    source_revision: int | None = None
    source_updated_at: datetime | None = None
    source_version_id: str | None = None


@dataclass(frozen=True, slots=True)
class ConnectorBatch:
    records: tuple[ConnectorRecord, ...]
    next_cursor: JsonDict
    has_more: bool = False


class SourceConnector(Protocol):
    @property
    def kind(self) -> str: ...

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        """Return records changed since ``cursor`` (all records when cursor is None)."""
        ...


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    processed: int
    unchanged: int
    cursor: JsonDict


class SyncStateStore(Protocol):
    async def get_cursor(self, source_id: UUID) -> JsonDict | None: ...

    async def save_cursor(self, source_id: UUID, cursor: JsonDict) -> None: ...

    async def start_job(self, source_id: UUID) -> UUID: ...

    async def finish_job(self, job_id: UUID, *, processed: int, unchanged: int) -> None: ...

    async def fail_job(self, job_id: UUID, *, error: str) -> None: ...

    async def last_synced_at(self, source_id: UUID) -> datetime | None:
        """When this source last had a saved cursor, for scheduling due syncs."""
        ...
