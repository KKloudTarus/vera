"""Curation domain records returned by repositories."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from vera.domain.knowledge.models import ClaimType, VerificationStatus


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    artifact_id: UUID
    version_id: UUID
    version: int


@dataclass(frozen=True, slots=True)
class ArtifactHead:
    """The current state of an artifact, used to make re-ingestion idempotent."""

    artifact_id: UUID
    version_id: UUID
    version: int
    content_hash: str


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
    confidence: float | None = None
