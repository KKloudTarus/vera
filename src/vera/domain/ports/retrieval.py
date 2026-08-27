"""Retrieval read-path ports.

The read model enriches stage-1 graph hits with VERA provenance (verification,
authority, source) and feedback counts, in batches. The feedback repository records
a viewer's up/down signal on a result.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from vera.shared.types import JsonDict


@dataclass(frozen=True, slots=True)
class HitProvenance:
    edge_uuid: str
    verification: str
    authority: float
    source_id: str
    confidence: float = 1.0


@dataclass(frozen=True, slots=True)
class EpisodeProvenance:
    source_id: str
    group_id: str
    knowledge_type: str
    verification: str
    authority: float
    reference_time: datetime
    payload: JsonDict


@dataclass(frozen=True, slots=True)
class RecentChange:
    source_id: str
    group_id: str
    knowledge_type: str
    verification: str
    reference_time: datetime


class RetrievalReadModel(Protocol):
    async def enrich(
        self, *, group_ids: Sequence[str], edge_uuids: Sequence[str]
    ) -> dict[str, HitProvenance]:
        """Provenance per edge_uuid. One query, no N+1."""
        ...

    async def feedback_counts(
        self, *, group_ids: Sequence[str], refs: Sequence[str]
    ) -> dict[str, tuple[int, int]]:
        """(upvotes, downvotes) per result ref, aggregated in one query."""
        ...

    async def recent_changes(self, *, group_ids: Sequence[str], limit: int) -> list[RecentChange]:
        """Most recently published episodes across the given scopes."""
        ...

    async def get_source(
        self, *, group_ids: Sequence[str], source_id: str
    ) -> EpisodeProvenance | None:
        """Provenance of one published episode, if it is in an allowed scope."""
        ...


class RetrievalFeedbackRepository(Protocol):
    async def record(
        self,
        *,
        group_id: str,
        principal_id: UUID | None,
        query: str,
        result_ref: str,
        signal: str,
        weight: float = 1.0,
    ) -> None: ...
