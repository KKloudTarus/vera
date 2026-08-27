"""CurationService: the raw-to-graph pipeline.

Ingest stores an artifact version, extracts and classifies claims, and applies the
source's trust tier: authoritative/curated auto-publish, informational needs review,
unverified only proposes. Publishing enforces the contamination guard and a conflict
check, then writes a published episode and enqueues an ingestion job so the worker
sends it to the graph. All writes share the caller's Unit of Work transaction.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

from vera.domain.curation.policy import may_publish_to
from vera.domain.curation.state import can_transition
from vera.domain.curation.trust import TrustAction, action_for_tier, authority_for_tier
from vera.domain.knowledge.models import ReviewDecision, VerificationStatus
from vera.domain.ontology import CURRENT_PIPELINE_VERSIONS
from vera.domain.ports.curation import ClaimExtractor
from vera.domain.ports.unit_of_work import UnitOfWork
from vera.shared.errors import Conflict, DomainError, Err, NotFound, Ok, PolicyRejected, Result
from vera.shared.ids import deterministic_id
from vera.shared.time import utc_now
from vera.shared.types import JsonDict


@dataclass(frozen=True, slots=True)
class IngestArtifact:
    source_id: UUID
    group_id: str
    external_id: str
    body: str
    knowledge_type: str = "text"
    title: str | None = None
    metadata: JsonDict | None = None


@dataclass(frozen=True, slots=True)
class IngestResult:
    artifact_version_id: str
    claim_ids: tuple[str, ...]
    published: int
    action: str


@dataclass(frozen=True, slots=True)
class PublishOutcome:
    status: str  # "published" or "flagged"


def _payload_for(
    subject: str | None, predicate: str | None, obj: str | None, statement: str
) -> JsonDict:
    if subject and predicate and obj:
        return {"triples": [{"subject": subject, "predicate": predicate, "object": obj}]}
    return {"body": statement}


class CurationService:
    def __init__(self, uow: UnitOfWork, extractor: ClaimExtractor) -> None:
        self._uow = uow
        self._extractor = extractor

    async def ingest_artifact(self, cmd: IngestArtifact) -> Result[IngestResult, DomainError]:
        uow = self._uow
        metadata = cmd.metadata or {}
        source = await uow.sources.get(cmd.source_id)
        if source is None:
            return Err(NotFound(f"knowledge source {cmd.source_id} not found"))

        # Re-ingestion is idempotent by content: an unchanged record is a no-op, changed
        # content appends a version. This keeps a re-run of a sync from duplicating work.
        content_hash = "sha256:" + hashlib.sha256(cmd.body.encode()).hexdigest()
        head = await uow.artifacts.get_head(source_id=cmd.source_id, external_id=cmd.external_id)
        if head is not None and head.content_hash == content_hash:
            return Ok(
                IngestResult(
                    artifact_version_id=str(head.version_id),
                    claim_ids=(),
                    published=0,
                    action="unchanged",
                )
            )
        s3_key = f"artifacts/{cmd.source_id}/{cmd.external_id}/v{(head.version + 1) if head else 1}"
        if head is None:
            ref = await uow.artifacts.create_with_version(
                source_id=cmd.source_id,
                external_id=cmd.external_id,
                title=cmd.title,
                content_hash=content_hash,
                s3_key=s3_key,
                reference_time=utc_now(),
            )
        else:
            ref = await uow.artifacts.add_version(
                artifact_id=head.artifact_id,
                content_hash=content_hash,
                s3_key=s3_key,
                reference_time=utc_now(),
            )

        extracted = await self._extractor.extract(
            body=cmd.body, knowledge_type=cmd.knowledge_type, metadata=metadata
        )
        action = action_for_tier(source.trust_tier)
        claim_ids: list[str] = []
        published = 0
        for item in extracted:
            claim = await uow.claims.create(
                artifact_version_id=ref.version_id, group_id=cmd.group_id, claim=item
            )
            claim_ids.append(str(claim.id))
            if action == TrustAction.AUTO_PUBLISH:
                await uow.claims.transition(
                    claim_id=claim.id,
                    expected_version=claim.version_id,
                    to_status=VerificationStatus.VERIFIED,
                )
                result = await self.publish_claim(claim.id)
                if isinstance(result, Ok) and result.value.status == "published":
                    published += 1
            elif action == TrustAction.REVIEW_REQUIRED:
                await uow.claims.transition(
                    claim_id=claim.id,
                    expected_version=claim.version_id,
                    to_status=VerificationStatus.PENDING,
                )

        return Ok(
            IngestResult(
                artifact_version_id=str(ref.version_id),
                claim_ids=tuple(claim_ids),
                published=published,
                action=action.value,
            )
        )

    async def review_claim(
        self,
        *,
        claim_id: UUID,
        reviewer_principal_id: UUID | None,
        approve: bool,
        authority: str | None = None,
        notes: str | None = None,
    ) -> Result[PublishOutcome, DomainError]:
        uow = self._uow
        claim = await uow.claims.get(claim_id)
        if claim is None:
            return Err(NotFound(f"claim {claim_id} not found"))

        target = VerificationStatus.VERIFIED if approve else VerificationStatus.DISPUTED
        if not can_transition(claim.status, target):
            return Err(Conflict(f"cannot move claim from {claim.status} to {target}"))

        decision = ReviewDecision.APPROVE if approve else ReviewDecision.REJECT
        await uow.reviews.add(
            candidate_claim_id=claim_id,
            reviewer_principal_id=reviewer_principal_id,
            decision=decision.value,
            authority=authority,
            notes=notes,
        )
        moved = await uow.claims.transition(
            claim_id=claim_id, expected_version=claim.version_id, to_status=target
        )
        if not moved:
            return Err(Conflict("claim changed since it was read"))
        if approve:
            return await self.publish_claim(claim_id)
        return Ok(PublishOutcome(status="flagged"))

    async def publish_claim(self, claim_id: UUID) -> Result[PublishOutcome, DomainError]:
        uow = self._uow
        claim = await uow.claims.get(claim_id)
        if claim is None:
            return Err(NotFound(f"claim {claim_id} not found"))

        if not may_publish_to(claim.group_id, claim.status):
            return Err(PolicyRejected("only verified knowledge may enter a shared scope"))
        if claim.status != VerificationStatus.VERIFIED:
            return Err(PolicyRejected("claim must be verified before publishing"))

        if claim.subject and claim.predicate and claim.object:
            conflicts = await uow.claims.find_verified_conflicts(
                group_id=claim.group_id,
                subject=claim.subject,
                predicate=claim.predicate,
                obj=claim.object,
                exclude_id=claim.id,
            )
            if conflicts:
                await uow.reviews.add(
                    candidate_claim_id=claim.id,
                    reviewer_principal_id=None,
                    decision=ReviewDecision.NEEDS_CHANGES.value,
                    authority="conflict-policy",
                    notes="conflicts with a verified fact on the same subject and predicate",
                )
                await uow.claims.transition(
                    claim_id=claim.id,
                    expected_version=claim.version_id,
                    to_status=VerificationStatus.DISPUTED,
                )
                return Ok(PublishOutcome(status="flagged"))

        durable_source = f"{claim.group_id}:{claim.id}"
        dedup = deterministic_id(durable_source)
        payload = _payload_for(claim.subject, claim.predicate, claim.object, claim.statement)
        tier = await uow.claims.source_trust_tier(claim.id)
        authority = authority_for_tier(tier) if tier is not None else 0.5
        # Stamp the episode with the ontology and pipeline versions that produced it, so
        # it is reproducible and a reprocess knows exactly what to rebuild under.
        ontology_id = await uow.ontology.get_active_id()
        inserted = await uow.episodes.publish(
            source_id=durable_source,
            group_id=claim.group_id,
            knowledge_type=claim.claim_type.value,
            verification="human_verified",
            authority=authority,
            reference_time=utc_now(),
            payload=payload,
            dedup_uuid=dedup,
            ontology_version_id=ontology_id,
            pipeline=CURRENT_PIPELINE_VERSIONS.as_dict(),
        )
        if inserted:
            await uow.outbox.add(
                group_id=claim.group_id,
                source_id=durable_source,
                dedup_uuid=dedup,
                payload=payload,
            )
        return Ok(PublishOutcome(status="published"))
