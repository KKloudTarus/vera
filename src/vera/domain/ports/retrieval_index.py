"""Ports for the passage, code, and fact candidate sources used by combined retrieval
(Phase 4).

These are the swappable candidate generators the ContextAssembler fans out to. The first
adapters are Postgres full-text search over the rebuildable ``search_vector`` columns; a
vector backend implements the same ports later without touching the application layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol


@dataclass(frozen=True, slots=True)
class RetrievalFilters:
    repository: str | None = None
    branch: str | None = None
    code_path: str | None = None
    document_type: str | None = None
    source_type: str | None = None
    include_predicates: tuple[str, ...] = field(default_factory=tuple)
    exclude_predicates: tuple[str, ...] = field(default_factory=tuple)
    min_authority: float | None = None
    max_trust_tier: int | None = None
    conflict_handling: Literal["include", "exclude", "only"] = "include"


@dataclass(frozen=True, slots=True)
class PassageHit:
    chunk_id: str
    artifact_version_id: str
    text: str
    score: float
    heading_path: str | None = None
    symbol_name: str | None = None
    start_offset: int | None = None
    end_offset: int | None = None
    page_number: int | None = None
    start_line: int | None = None
    end_line: int | None = None


@dataclass(frozen=True, slots=True)
class FactHit:
    fact_key: str
    fact_id: str
    subject_name: str
    predicate: str
    object_name: str
    text: str
    authority: float
    confidence: float
    lifecycle_state: str
    score: float
    valid_from: datetime | None = None
    supporting_source_ids: tuple[str, ...] = field(default_factory=tuple)


class PassageIndex(Protocol):
    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        """Search passages. When ``created_before`` is given (a snapshot's freeze time), only
        chunks ingested at or before it are returned, so a pack against a snapshot reproduces
        the passages that existed when it was taken.
        """
        ...


class CodeIndex(Protocol):
    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]: ...


class FactCandidateSource(Protocol):
    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        as_of: datetime | None = None,
        restrict_fact_ids: set[str] | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[FactHit]:
        """Search active facts, or, when ``restrict_fact_ids`` is given (a snapshot's frozen
        membership), only those facts regardless of their current lifecycle, for reproducible
        snapshot retrieval.
        """
        ...
