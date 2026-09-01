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
class LabeledSignals:
    """A logged rerank signal vector paired with the label feedback later gave it."""

    signals: JsonDict
    label: int  # +1 for an up vote, -1 for a down vote


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

    async def calibration_samples(
        self, *, group_ids: Sequence[str], since: datetime | None = None
    ) -> list[LabeledSignals]:
        """Feedback rows that carry a logged signal vector, for rerank calibration."""
        ...

    async def feedback_groups(self) -> list[str]:
        """Distinct group_ids that have signal-bearing feedback, so a global calibration
        run can discover its own scope.
        """
        ...


class RetrievalFeedbackRepository(Protocol):
    async def lock_attribution(
        self, *, principal_id: UUID, context_pack_id: UUID, result_ref: str
    ) -> None: ...

    async def attributed_signal(
        self, *, principal_id: UUID, context_pack_id: UUID, result_ref: str
    ) -> str | None: ...

    async def record(
        self,
        *,
        group_id: str,
        principal_id: UUID | None,
        query: str,
        result_ref: str,
        context_pack_id: UUID | None = None,
        signal: str,
        weight: float = 1.0,
        signals: JsonDict | None = None,
        rank: int | None = None,
    ) -> None: ...
