"""Ports for authoritative lineage behind derived graph communities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True, slots=True)
class CommunityFact:
    fact_id: UUID
    fact_key: str
    subject_name: str
    predicate: str
    object_name: str


@dataclass(frozen=True, slots=True)
class CommunityLineageItem:
    community_id: UUID
    derivation_run_id: UUID
    fact_id: UUID
    fact_key: str
    subject_name: str
    predicate: str
    object_name: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class CommunityLineagePage:
    items: tuple[CommunityLineageItem, ...]
    next_cursor: str | None


class CommunityLineageRepository(Protocol):
    async def active_facts(self, *, group_id: str) -> tuple[CommunityFact, ...]: ...

    async def record(
        self,
        *,
        group_id: str,
        community_id: UUID,
        derivation_run_id: UUID,
        fact_ids: tuple[UUID, ...],
    ) -> None: ...

    async def page(
        self,
        *,
        group_ids: tuple[str, ...],
        community_id: UUID,
        derivation_run_id: UUID | None,
        cursor: UUID | None,
        limit: int,
    ) -> CommunityLineagePage: ...
