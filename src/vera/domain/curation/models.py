"""Curation domain records returned by repositories."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from vera.domain.knowledge.models import ClaimType, VerificationStatus
from vera.shared.types import JsonDict, empty_json


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    artifact_id: UUID
    version_id: UUID
    version: int
    source_revision: int | None = None
    source_updated_at: datetime | None = None
    source_version_id: str | None = None
    observed_at: datetime | None = None
    predecessor_version_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class ArtifactHead:
    """The current state of an artifact, used to make re-ingestion idempotent."""

    artifact_id: UUID
    version_id: UUID
    version: int
    content_hash: str
    source_revision: int | None = None
    source_updated_at: datetime | None = None
    source_version_id: str | None = None
    observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ClaimRecord:
    id: UUID
    group_id: str
    artifact_version_id: UUID
    statement: str
    claim_type: ClaimType
    status: VerificationStatus
    version_id: int
    subject: str | None
    predicate: str | None
    object: str | None
    subject_entity_type: str | None = None
    object_entity_type: str | None = None
    qualifiers: JsonDict = field(default_factory=empty_json)
    confidence: float | None = None
    extraction_run_id: UUID | None = None
    chunk_id: UUID | None = None
    source_quote: str | None = None
    quote_start: int | None = None
    quote_end: int | None = None
    quote_hash: str | None = None
    needs_review: bool = False
