"""Knowledge-context domain models (example scaffolding).

These are plain, framework-free domain objects. The SQLAlchemy tables that persist
them live in ``vera.adapters.persistence.models`` and are mapped imperatively, so
these classes stay pure (no ``DeclarativeBase``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from vera.shared.time import utc_now
from vera.shared.types import GroupId, SourceId


class SourceKind(StrEnum):
    GIT = "git"
    CONFLUENCE = "confluence"
    JIRA = "jira"
    CMDB = "cmdb"
    SLACK = "slack"
    PDF = "pdf"
    FILESYSTEM = "filesystem"
    AGENT = "agent"


class ReviewDecision(StrEnum):
    APPROVE = "approve"
    REJECT = "reject"
    NEEDS_CHANGES = "needs_changes"


class ClaimType(StrEnum):
    FACT = "fact"
    REQUIREMENT = "requirement"
    DECISION = "decision"
    PROCEDURE = "procedure"
    HYPOTHESIS = "hypothesis"
    PROPOSAL = "proposal"


class VerificationStatus(StrEnum):
    UNVERIFIED = "unverified"
    PENDING = "pending"
    VERIFIED = "verified"
    DISPUTED = "disputed"


@dataclass(slots=True)
class CandidateClaim:
    """A claim extracted from an artifact, not yet promoted to the graph.

    Curation moves it ``unverified``/``pending`` -> ``verified`` before it becomes
    a published episode. Epistemic status lives here in Postgres, never in Graphiti.
    """

    source_id: SourceId
    group_id: GroupId
    statement: str
    claim_type: ClaimType = ClaimType.FACT
    status: VerificationStatus = VerificationStatus.UNVERIFIED
    reference_time: datetime = field(default_factory=utc_now)
    verified_by: str | None = None

    def verify(self, *, by: str) -> None:
        if self.status is VerificationStatus.VERIFIED:
            return
        self.status = VerificationStatus.VERIFIED
        self.verified_by = by

    @property
    def is_publishable(self) -> bool:
        return self.status is VerificationStatus.VERIFIED


@dataclass(frozen=True, slots=True)
class CanonicalEntity:
    """A VERA-owned identity that unifies graph fragments across scopes."""

    id: UUID
    group_id: str
    entity_type: str
    canonical_name: str
