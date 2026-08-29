"""Curation ports: claim extraction and the curation repositories.

The application curation service depends only on these; concrete implementations live
in the adapters layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from uuid import UUID

from vera.domain.curation.models import ArtifactHead, ArtifactRef, ClaimRecord
from vera.domain.knowledge.models import ClaimType, VerificationStatus
from vera.shared.types import JsonDict, empty_json


@dataclass(frozen=True, slots=True)
class ExtractedClaim:
    statement: str
    claim_type: ClaimType = ClaimType.FACT
    subject: str | None = None
    predicate: str | None = None
    object: str | None = None
    subject_entity_type: str | None = None
    object_entity_type: str | None = None
    qualifiers: JsonDict = field(default_factory=empty_json)
    confidence: float | None = None
    source_quote: str | None = None
    quote_start: int | None = None
    quote_end: int | None = None


@dataclass(frozen=True, slots=True)
class SourceRecord:
    id: UUID
    trust_tier: int
    kind: str


class ClaimExtractor(Protocol):
    @property
    def provider(self) -> str: ...

    @property
    def model(self) -> str: ...

    async def extract(
        self, *, body: str, knowledge_type: str, metadata: JsonDict
    ) -> list[ExtractedClaim]:
        """Turn an artifact into candidate claims."""
        ...


class ContradictionJudge(Protocol):
    async def contradictions(
        self, *, subject: str, predicate: str, new_object: str, existing_objects: list[str]
    ) -> set[str]:
        """Return the existing objects that the new (subject, predicate, object) truly
        contradicts (semantic judgement, for non-functional predicates)."""
        ...


class EntityResolutionJudge(Protocol):
    async def same_entity(
        self, *, name: str, entity_type: str, candidates: list[str]
    ) -> str | None:
        """Which candidate canonical name refers to the same real-world entity as ``name``,
        or None. Resolves synonyms, abbreviations, and cross-lingual names that embedding
        similarity over bare names cannot separate on its own."""
        ...


class KnowledgeSourceRepository(Protocol):
    async def create(
        self,
        *,
        workspace_id: UUID,
        project_id: UUID | None,
        kind: str,
        name: str,
        trust_tier: int,
    ) -> UUID: ...

    async def get(self, source_id: UUID) -> SourceRecord | None: ...

    async def get_or_create_agent(self, *, workspace_id: UUID) -> UUID:
        """The tier-4 agent source for a workspace, creating it on first use."""
        ...


class ArtifactRepository(Protocol):
    async def create_with_version(
        self,
        *,
        source_id: UUID,
        external_id: str,
        title: str | None,
        content_hash: str,
        s3_key: str,
        reference_time: datetime,
        source_revision: int | None = None,
        source_updated_at: datetime | None = None,
        source_version_id: str | None = None,
        observed_at: datetime | None = None,
    ) -> ArtifactRef: ...

    async def get_head(self, *, source_id: UUID, external_id: str) -> ArtifactHead | None:
        """The current artifact for this external id, or None if never ingested."""
        ...

    async def add_version(
        self,
        *,
        artifact_id: UUID,
        content_hash: str,
        s3_key: str,
        reference_time: datetime,
        source_revision: int | None = None,
        source_updated_at: datetime | None = None,
        source_version_id: str | None = None,
        observed_at: datetime | None = None,
    ) -> ArtifactRef:
        """Append a new version to an existing artifact and make it current."""
        ...


class CandidateClaimRepository(Protocol):
    async def create(
        self,
        *,
        artifact_version_id: UUID,
        group_id: str,
        claim: ExtractedClaim,
        extraction_run_id: UUID | None = None,
        chunk_id: UUID | None = None,
        quote_hash: str | None = None,
        needs_review: bool = False,
    ) -> ClaimRecord: ...

    async def get(self, claim_id: UUID) -> ClaimRecord | None: ...

    async def transition(
        self, *, claim_id: UUID, expected_version: int, to_status: VerificationStatus
    ) -> bool:
        """Optimistically move a claim to a new status. False if the version is stale."""
        ...

    async def find_verified_conflicts(
        self, *, group_id: str, subject: str, predicate: str, obj: str, exclude_id: UUID
    ) -> list[ClaimRecord]:
        """Verified claims with the same subject and predicate but a different object."""
        ...

    async def source_trust_tier(self, claim_id: UUID) -> int | None:
        """The trust tier of the knowledge source behind a claim, via its artifact."""
        ...


class ReviewRepository(Protocol):
    async def add(
        self,
        *,
        candidate_claim_id: UUID,
        reviewer_principal_id: UUID | None,
        decision: str,
        authority: str | None,
        notes: str | None,
    ) -> None: ...


class PublishedEpisodeRepository(Protocol):
    async def publish(
        self,
        *,
        source_id: str,
        group_id: str,
        knowledge_type: str,
        verification: str,
        authority: float,
        reference_time: datetime,
        payload: JsonDict,
        dedup_uuid: UUID,
        ontology_version_id: UUID | None = None,
        pipeline: JsonDict | None = None,
        confidence: float = 1.0,
    ) -> bool:
        """Insert a published episode. False if this dedup_uuid was already published."""
        ...

    async def invalidate(
        self, *, group_id: str, source_id: str, invalid_at: datetime, superseded_by_source: str
    ) -> None:
        """Mark an episode superseded (bi-temporal): set invalid_at and who replaced it."""
        ...
