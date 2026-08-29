"""CurationService: the raw-to-graph pipeline.

Ingest stores an artifact version, extracts and classifies claims, and applies the
source's trust tier: authoritative/curated auto-publish, informational needs review,
unverified only proposes. Publishing enforces the contamination guard and a conflict
check, then writes a published episode and enqueues an ingestion job so the worker
sends it to the graph. All writes share the caller's Unit of Work transaction.
"""

from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass, replace
from datetime import datetime
from uuid import UUID

from vera.application.curation.chunking import chunk_artifact
from vera.application.curation.supersede import SupersedePolicy
from vera.domain.curation.models import ArtifactHead, ArtifactRef, ClaimRecord
from vera.domain.curation.policy import may_publish_to
from vera.domain.curation.state import can_transition
from vera.domain.curation.trust import (
    TrustAction,
    TrustTier,
    action_for_tier,
    authority_for_tier,
)
from vera.domain.knowledge.fabric import Chunk, ChunkEmbedding, ExtractionRun
from vera.domain.knowledge.models import ReviewDecision, VerificationStatus
from vera.domain.ontology import CURRENT_PIPELINE_VERSIONS
from vera.domain.ports.curation import ClaimExtractor, ContradictionJudge, ExtractedClaim
from vera.domain.ports.embedder import Embedder
from vera.domain.ports.object_store import ObjectStore
from vera.domain.ports.unit_of_work import UnitOfWork
from vera.shared.errors import Conflict, DomainError, Err, NotFound, Ok, PolicyRejected, Result
from vera.shared.ids import deterministic_id, uuid7
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
    reference_time: datetime | None = None
    source_revision: int | None = None
    source_updated_at: datetime | None = None
    source_version_id: str | None = None
    tombstone: bool = False


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
    subject: str | None,
    predicate: str | None,
    obj: str | None,
    statement: str,
    *,
    subject_entity_type: str | None = None,
    object_entity_type: str | None = None,
    qualifiers: JsonDict | None = None,
    supersede_objects: list[str] | None = None,
    source_quote: str | None = None,
    quote_start: int | None = None,
    quote_end: int | None = None,
) -> JsonDict:
    if subject and predicate and obj:
        triple: JsonDict = {"subject": subject, "predicate": predicate, "object": obj}
        if subject_entity_type:
            triple["entity_type"] = subject_entity_type
        if object_entity_type:
            triple["object_type"] = object_entity_type
        if qualifiers:
            triple["qualifiers"] = qualifiers
        if supersede_objects:
            # The worker closes the prior edges' valid time for these specific objects.
            triple["supersede_objects"] = supersede_objects
        if source_quote is not None and quote_start is not None and quote_end is not None:
            triple.update(
                source_quote=source_quote,
                quote_start=quote_start,
                quote_end=quote_end,
            )
        return {"triples": [triple]}
    return {"body": statement}


def _exact_quote_hash(claim: ExtractedClaim, chunk: Chunk) -> str | None:
    start, end, quote = claim.quote_start, claim.quote_end, claim.source_quote
    if start is None or end is None or not quote or start < 0 or end <= start:
        return None
    if end > len(chunk.text) or chunk.text[start:end] != quote:
        return None
    return hashlib.sha256(quote.encode("utf-8")).hexdigest()


def _align_document_quote(
    claim: ExtractedClaim, chunks: list[Chunk], body: str
) -> tuple[ExtractedClaim, Chunk | None]:
    start, end, quote = claim.quote_start, claim.quote_end, claim.source_quote
    if (
        start is None
        or end is None
        or not quote
        or start < 0
        or end <= start
        or end > len(body)
        or body[start:end] != quote
    ):
        return claim, None
    for chunk in chunks:
        chunk_start = body.find(chunk.text)
        while chunk_start >= 0:
            chunk_end = chunk_start + len(chunk.text)
            if chunk_start <= start and end <= chunk_end:
                return (
                    replace(
                        claim,
                        quote_start=start - chunk_start,
                        quote_end=end - chunk_start,
                    ),
                    chunk,
                )
            chunk_start = body.find(chunk.text, chunk_start + 1)
    return claim, None


def _is_stale_version(head: ArtifactHead, cmd: IngestArtifact) -> bool:
    if cmd.source_revision is not None and head.source_revision is not None:
        return cmd.source_revision <= head.source_revision
    if cmd.source_updated_at is not None and head.source_updated_at is not None:
        incoming = (cmd.source_updated_at, cmd.source_version_id or "")
        current = (head.source_updated_at, head.source_version_id or "")
        return incoming <= current
    return False


class CurationService:
    def __init__(
        self,
        uow: UnitOfWork,
        extractor: ClaimExtractor,
        object_store: ObjectStore | None = None,
        judge: ContradictionJudge | None = None,
        embedder: Embedder | None = None,
        embedding_provider: str = "unknown",
        embedding_model: str = "unknown",
        embedding_model_version: str = "1",
        embedding_dimension: int | None = None,
    ) -> None:
        self._uow = uow
        self._extractor = extractor
        self._object_store = object_store
        self._supersede = SupersedePolicy(judge)
        self._embedder = embedder
        self._embedding_provider = embedding_provider
        self._embedding_model = embedding_model
        self._embedding_model_version = embedding_model_version
        self._embedding_dimension = embedding_dimension

    async def _contradicted(self, claim: ClaimRecord) -> list[ClaimRecord]:
        """Existing verified claims the new claim replaces, per the single supersede
        policy shared by the structured and free-text ingestion paths.
        """
        subject, predicate, obj = claim.subject, claim.predicate, claim.object
        if not (subject and predicate and obj):
            return []
        conflicts = await self._uow.claims.find_verified_conflicts(
            group_id=claim.group_id,
            subject=subject,
            predicate=predicate,
            obj=obj,
            exclude_id=claim.id,
        )
        return await self._supersede.contradicted(
            subject=subject, predicate=predicate, new_object=obj, conflicts=conflicts
        )

    async def ingest_artifact(self, cmd: IngestArtifact) -> Result[IngestResult, DomainError]:
        uow = self._uow
        metadata = cmd.metadata or {}
        source = await uow.sources.get(cmd.source_id)
        if source is None:
            return Err(NotFound(f"knowledge source {cmd.source_id} not found"))

        # Re-ingestion is idempotent by content: an unchanged record is a no-op, changed
        # content appends a version. This keeps a re-run of a sync from duplicating work.
        hash_input = (b"tombstone\0" + cmd.body.encode()) if cmd.tombstone else cmd.body.encode()
        content_hash = "sha256:" + hashlib.sha256(hash_input).hexdigest()
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
        if head is not None and _is_stale_version(head, cmd):
            return Ok(
                IngestResult(
                    artifact_version_id=str(head.version_id),
                    claim_ids=(),
                    published=0,
                    action="stale",
                )
            )
        observed_at = utc_now()
        reference_time = cmd.reference_time or observed_at
        s3_key = f"artifacts/{cmd.source_id}/{cmd.external_id}/v{(head.version + 1) if head else 1}"
        # Persist the raw artifact bytes so the graph stays rebuildable from Postgres + S3.
        if self._object_store is not None and cmd.body:
            await self._object_store.put(
                key=s3_key, data=cmd.body.encode("utf-8"), content_type="text/plain"
            )
        if head is None:
            ref = await uow.artifacts.create_with_version(
                source_id=cmd.source_id,
                external_id=cmd.external_id,
                title=cmd.title,
                content_hash=content_hash,
                s3_key=s3_key,
                reference_time=reference_time,
                source_revision=cmd.source_revision,
                source_updated_at=cmd.source_updated_at,
                source_version_id=cmd.source_version_id,
                observed_at=observed_at,
            )
        else:
            ref = await uow.artifacts.add_version(
                artifact_id=head.artifact_id,
                content_hash=content_hash,
                s3_key=s3_key,
                reference_time=reference_time,
                source_revision=cmd.source_revision,
                source_updated_at=cmd.source_updated_at,
                source_version_id=cmd.source_version_id,
                observed_at=observed_at,
            )

        if cmd.tombstone:
            # Source deletion is lifecycle metadata, not a new knowledge assertion. An empty
            # authoritative version withdraws every assertion from the previous artifact version.
            await self._queue_fabric_version(ref, source.trust_tier, cmd.group_id)
            return Ok(
                IngestResult(
                    artifact_version_id=str(ref.version_id),
                    claim_ids=(),
                    published=0,
                    action="tombstone",
                )
            )

        # Structure-aware chunking (persisted before extraction) so retrieval has citable
        # passages and free-text extraction never sends an unbounded document to the LLM: each
        # chunk is bounded, and extraction runs per chunk. Structured triples come from the
        # metadata, so they are extracted once and need no chunk loop.
        normalized_body = unicodedata.normalize("NFC", cmd.body)
        content_type = str(metadata.get("content_type") or "text/markdown")
        chunks = (
            chunk_artifact(
                text=normalized_body,
                content_type=content_type,
                artifact_version_id=ref.version_id,
                group_id=cmd.group_id,
            )
            if normalized_body.strip()
            else []
        )
        chunks = [await uow.chunks.upsert(chunk) for chunk in chunks]
        if self._embedder is not None:
            for chunk in chunks:
                vector = await self._embedder.embed(chunk.text)
                dimension = self._embedding_dimension or len(vector)
                if len(vector) != dimension:
                    raise ValueError(
                        f"embedding dimension mismatch: expected {dimension}, got {len(vector)}"
                    )
                await uow.chunk_embeddings.upsert(
                    ChunkEmbedding(
                        id=uuid7(),
                        group_id=cmd.group_id,
                        chunk_id=chunk.id,
                        provider=self._embedding_provider,
                        model=self._embedding_model,
                        model_version=self._embedding_model_version,
                        dimension=dimension,
                        embedding=vector,
                        content_hash=chunk.content_hash,
                        created_at=utc_now(),
                    )
                )
        extraction_run = await uow.extraction_runs.add(
            ExtractionRun(
                id=uuid7(),
                group_id=cmd.group_id,
                artifact_version_id=ref.version_id,
                model=self._extractor.model,
                provider=self._extractor.provider,
                prompt_version=CURRENT_PIPELINE_VERSIONS.prompt,
                pipeline_version=CURRENT_PIPELINE_VERSIONS.as_dict(),
                started_at=utc_now(),
            )
        )

        has_structured = bool(metadata.get("triples") or metadata.get("claims"))
        if has_structured or not chunks:
            extracted = await self._extractor.extract(
                body=normalized_body, knowledge_type=cmd.knowledge_type, metadata=metadata
            )
            extracted_with_chunks = (
                [_align_document_quote(claim, chunks, normalized_body) for claim in extracted]
                if chunks
                else [(claim, None) for claim in extracted]
            )
        else:
            extracted_with_chunks: list[tuple[ExtractedClaim, Chunk | None]] = []
            for chunk in chunks:
                extracted_with_chunks.extend(
                    (claim, chunk)
                    for claim in await self._extractor.extract(
                        body=chunk.text, knowledge_type=cmd.knowledge_type, metadata={}
                    )
                )
        action = action_for_tier(source.trust_tier)
        claim_ids: list[str] = []
        published = 0
        for item, chunk in extracted_with_chunks:
            quote_hash = _exact_quote_hash(item, chunk) if chunk is not None else None
            has_quote = any(
                value is not None for value in (item.source_quote, item.quote_start, item.quote_end)
            )
            needs_review = (chunk is not None and quote_hash is None) or (
                chunk is None and has_quote
            )
            claim = await uow.claims.create(
                artifact_version_id=ref.version_id,
                group_id=cmd.group_id,
                claim=item,
                extraction_run_id=extraction_run.id,
                chunk_id=chunk.id if chunk is not None else None,
                quote_hash=quote_hash,
                needs_review=needs_review,
            )
            claim_ids.append(str(claim.id))
            if needs_review:
                await uow.claims.transition(
                    claim_id=claim.id,
                    expected_version=claim.version_id,
                    to_status=VerificationStatus.PENDING,
                )
                await self._queue_fabric_review(claim, source.trust_tier)
                continue
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

        has_fabric_claim = any(
            item.subject and item.predicate and item.object for item, _ in extracted_with_chunks
        )
        if action is TrustAction.AUTO_PUBLISH and not has_fabric_claim:
            await self._queue_fabric_version(ref, source.trust_tier, cmd.group_id)

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
        if approve and claim.needs_review:
            return Err(PolicyRejected("claim provenance must be corrected before approval"))

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
        if claim.needs_review:
            return Err(PolicyRejected("claim provenance must be corrected before publishing"))

        durable_source = f"{claim.group_id}:{claim.id}"
        tier = await uow.claims.source_trust_tier(claim.id)
        supersede_objects: list[str] = []
        if claim.subject and claim.predicate and claim.object:
            contradicted = await self._contradicted(claim)
            if contradicted and (tier is None or tier > TrustTier.CURATED):
                # Not trusted enough to overwrite verified memory: hold for review.
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
            if contradicted:
                # A trusted, newer value supersedes the old ones: invalidate the prior
                # episodes (bi-temporal) so the current view updates, history stays queryable.
                now = utc_now()
                for old in contradicted:
                    await uow.episodes.invalidate(
                        group_id=old.group_id,
                        source_id=f"{old.group_id}:{old.id}",
                        invalid_at=now,
                        superseded_by_source=durable_source,
                    )
                    await uow.claims.transition(
                        claim_id=old.id,
                        expected_version=old.version_id,
                        to_status=VerificationStatus.DISPUTED,
                    )
                    await uow.reviews.add(
                        candidate_claim_id=old.id,
                        reviewer_principal_id=None,
                        decision=ReviewDecision.NEEDS_CHANGES.value,
                        authority="superseded",
                        notes=f"superseded by claim {claim.id}",
                    )
                supersede_objects = [old.object for old in contradicted if old.object]

        dedup = deterministic_id(durable_source)
        payload = _payload_for(
            claim.subject,
            claim.predicate,
            claim.object,
            claim.statement,
            subject_entity_type=claim.subject_entity_type,
            object_entity_type=claim.object_entity_type,
            qualifiers=claim.qualifiers,
            supersede_objects=supersede_objects,
            source_quote=claim.source_quote,
            quote_start=claim.quote_start,
            quote_end=claim.quote_end,
        )
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
            confidence=claim.confidence if claim.confidence is not None else 1.0,
        )
        if inserted:
            # Carry the real provenance the worker's Fabric reconciliation needs, so it never
            # has to guess authority or trust. Namespaced under `_fabric`; the legacy graph
            # path and the triple payload are unchanged (extra keys are ignored there).
            outbox_payload = {
                **payload,
                "_fabric": {
                    "trust_tier": int(tier) if tier is not None else int(TrustTier.UNVERIFIED),
                    "authority": authority,
                    "confidence": claim.confidence if claim.confidence is not None else 1.0,
                    "verification": "human_verified",
                    "ontology_version_id": str(ontology_id) if ontology_id is not None else None,
                    "artifact_version_id": str(claim.artifact_version_id),
                    "extraction_run_id": (
                        str(claim.extraction_run_id)
                        if claim.extraction_run_id is not None
                        else None
                    ),
                    "chunk_id": str(claim.chunk_id) if claim.chunk_id is not None else None,
                    "quote_hash": claim.quote_hash,
                    "needs_review": False,
                },
            }
            await uow.outbox.add(
                group_id=claim.group_id,
                source_id=durable_source,
                dedup_uuid=dedup,
                payload=outbox_payload,
            )
        return Ok(PublishOutcome(status="published"))

    async def _queue_fabric_review(self, claim: ClaimRecord, trust_tier: int) -> None:
        if not (claim.subject and claim.predicate and claim.object):
            return
        source_id = f"review:{claim.group_id}:{claim.id}"
        ontology_id = await self._uow.ontology.get_active_id()
        payload = _payload_for(
            claim.subject,
            claim.predicate,
            claim.object,
            claim.statement,
            subject_entity_type=claim.subject_entity_type,
            object_entity_type=claim.object_entity_type,
            qualifiers=claim.qualifiers,
            source_quote=claim.source_quote,
            quote_start=claim.quote_start,
            quote_end=claim.quote_end,
        )
        payload["_fabric"] = {
            "trust_tier": int(trust_tier),
            "authority": authority_for_tier(trust_tier),
            "confidence": claim.confidence if claim.confidence is not None else 0.0,
            "verification": "pending",
            "ontology_version_id": str(ontology_id) if ontology_id is not None else None,
            "artifact_version_id": str(claim.artifact_version_id),
            "extraction_run_id": (
                str(claim.extraction_run_id) if claim.extraction_run_id is not None else None
            ),
            "chunk_id": str(claim.chunk_id) if claim.chunk_id is not None else None,
            "quote_hash": None,
            "needs_review": True,
        }
        await self._uow.outbox.add(
            group_id=claim.group_id,
            source_id=source_id,
            dedup_uuid=deterministic_id(source_id),
            payload=payload,
        )

    async def _queue_fabric_version(self, ref: ArtifactRef, trust_tier: int, group_id: str) -> None:
        source_id = f"fabric-version:{group_id}:{ref.version_id}"
        ontology_id = await self._uow.ontology.get_active_id()
        await self._uow.outbox.add(
            group_id=group_id,
            source_id=source_id,
            dedup_uuid=deterministic_id(source_id),
            payload={
                "job_kind": "fabric_reconcile_version",
                "triples": [],
                "_fabric": {
                    "trust_tier": int(trust_tier),
                    "authority": authority_for_tier(trust_tier),
                    "confidence": 1.0,
                    "verification": "human_verified",
                    "ontology_version_id": str(ontology_id) if ontology_id is not None else None,
                    "artifact_version_id": str(ref.version_id),
                    "extraction_run_id": None,
                    "chunk_id": None,
                    "quote_hash": None,
                    "needs_review": False,
                },
            },
        )
