"""Concrete curation repositories: sources, artifacts, candidate claims, reviews,
published episodes. Each maps ORM rows to domain records at its boundary.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import CursorResult, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.knowledge import (
    ArtifactRow,
    ArtifactVersionRow,
    CandidateClaimRow,
    KnowledgeSourceRow,
    PublishedEpisodeRow,
    ReviewRow,
)
from vera.domain.curation.models import ArtifactHead, ArtifactRef, ClaimRecord
from vera.domain.knowledge.models import ClaimType, VerificationStatus
from vera.domain.ports.curation import ExtractedClaim, SourceRecord
from vera.shared.types import JsonDict


def _to_claim(row: CandidateClaimRow) -> ClaimRecord:
    return ClaimRecord(
        id=row.id,
        group_id=row.group_id,
        artifact_version_id=row.artifact_version_id,
        statement=row.statement,
        claim_type=ClaimType(row.claim_type),
        status=VerificationStatus(row.verification_status),
        version_id=row.version_id,
        subject=row.subject,
        predicate=row.predicate,
        object=row.object,
    )


class SqlAlchemyKnowledgeSourceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        workspace_id: UUID,
        project_id: UUID | None,
        kind: str,
        name: str,
        trust_tier: int,
    ) -> UUID:
        row = KnowledgeSourceRow(
            workspace_id=workspace_id,
            project_id=project_id,
            kind=kind,
            name=name,
            trust_tier=trust_tier,
        )
        self._session.add(row)
        await self._session.flush()
        return row.id

    async def get(self, source_id: UUID) -> SourceRecord | None:
        row = await self._session.get(KnowledgeSourceRow, source_id)
        if row is None:
            return None
        return SourceRecord(id=row.id, trust_tier=row.trust_tier, kind=row.kind)

    async def get_or_create_agent(self, *, workspace_id: UUID) -> UUID:
        existing = await self._session.scalar(
            select(KnowledgeSourceRow.id).where(
                KnowledgeSourceRow.workspace_id == workspace_id,
                KnowledgeSourceRow.kind == "agent",
            )
        )
        if existing is not None:
            return existing
        return await self.create(
            workspace_id=workspace_id,
            project_id=None,
            kind="agent",
            name="agent proposals",
            trust_tier=4,
        )


class SqlAlchemyArtifactRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_with_version(
        self,
        *,
        source_id: UUID,
        external_id: str,
        title: str | None,
        content_hash: str,
        s3_key: str,
        reference_time: datetime,
    ) -> ArtifactRef:
        artifact = ArtifactRow(
            source_id=source_id,
            external_id=external_id,
            title=title,
            content_hash=content_hash,
            s3_key=s3_key,
            reference_time=reference_time,
            current_version=1,
        )
        self._session.add(artifact)
        await self._session.flush()
        version = ArtifactVersionRow(
            artifact_id=artifact.id,
            version=1,
            content_hash=content_hash,
            s3_key=s3_key,
            reference_time=reference_time,
        )
        self._session.add(version)
        await self._session.flush()
        return ArtifactRef(artifact_id=artifact.id, version_id=version.id, version=1)

    async def get_head(self, *, source_id: UUID, external_id: str) -> ArtifactHead | None:
        row = (
            await self._session.execute(
                select(ArtifactRow).where(
                    ArtifactRow.source_id == source_id,
                    ArtifactRow.external_id == external_id,
                )
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        version_id = await self._session.scalar(
            select(ArtifactVersionRow.id).where(
                ArtifactVersionRow.artifact_id == row.id,
                ArtifactVersionRow.version == row.current_version,
            )
        )
        if version_id is None:  # a current version should always exist
            raise ValueError(f"artifact {row.id} has no row for version {row.current_version}")
        return ArtifactHead(
            artifact_id=row.id,
            version_id=version_id,
            version=row.current_version,
            content_hash=row.content_hash,
        )

    async def add_version(
        self,
        *,
        artifact_id: UUID,
        content_hash: str,
        s3_key: str,
        reference_time: datetime,
    ) -> ArtifactRef:
        artifact = await self._session.get(ArtifactRow, artifact_id)
        if artifact is None:
            raise ValueError(f"artifact {artifact_id} not found")
        next_version = artifact.current_version + 1
        artifact.current_version = next_version
        artifact.content_hash = content_hash
        artifact.s3_key = s3_key
        artifact.reference_time = reference_time
        version = ArtifactVersionRow(
            artifact_id=artifact_id,
            version=next_version,
            content_hash=content_hash,
            s3_key=s3_key,
            reference_time=reference_time,
        )
        self._session.add(version)
        await self._session.flush()
        return ArtifactRef(artifact_id=artifact_id, version_id=version.id, version=next_version)


class SqlAlchemyCandidateClaimRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self, *, artifact_version_id: UUID, group_id: str, claim: ExtractedClaim
    ) -> ClaimRecord:
        row = CandidateClaimRow(
            artifact_version_id=artifact_version_id,
            group_id=group_id,
            statement=claim.statement,
            claim_type=claim.claim_type.value,
            verification_status=VerificationStatus.UNVERIFIED.value,
            subject=claim.subject,
            predicate=claim.predicate,
            object=claim.object,
            confidence=claim.confidence,
        )
        self._session.add(row)
        await self._session.flush()
        return _to_claim(row)

    async def get(self, claim_id: UUID) -> ClaimRecord | None:
        row = await self._session.get(CandidateClaimRow, claim_id)
        return _to_claim(row) if row is not None else None

    async def transition(
        self, *, claim_id: UUID, expected_version: int, to_status: VerificationStatus
    ) -> bool:
        result = await self._session.execute(
            update(CandidateClaimRow)
            .where(
                CandidateClaimRow.id == claim_id,
                CandidateClaimRow.version_id == expected_version,
            )
            .values(
                verification_status=to_status.value,
                version_id=CandidateClaimRow.version_id + 1,
            )
        )
        return cast("CursorResult[Any]", result).rowcount > 0

    async def find_verified_conflicts(
        self, *, group_id: str, subject: str, predicate: str, obj: str, exclude_id: UUID
    ) -> list[ClaimRecord]:
        stmt = select(CandidateClaimRow).where(
            CandidateClaimRow.group_id == group_id,
            CandidateClaimRow.verification_status == VerificationStatus.VERIFIED.value,
            CandidateClaimRow.subject == subject,
            CandidateClaimRow.predicate == predicate,
            CandidateClaimRow.object != obj,
            CandidateClaimRow.id != exclude_id,
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [_to_claim(row) for row in rows]

    async def source_trust_tier(self, claim_id: UUID) -> int | None:
        stmt = (
            select(KnowledgeSourceRow.trust_tier)
            .select_from(CandidateClaimRow)
            .join(
                ArtifactVersionRow,
                ArtifactVersionRow.id == CandidateClaimRow.artifact_version_id,
            )
            .join(ArtifactRow, ArtifactRow.id == ArtifactVersionRow.artifact_id)
            .join(KnowledgeSourceRow, KnowledgeSourceRow.id == ArtifactRow.source_id)
            .where(CandidateClaimRow.id == claim_id)
        )
        return await self._session.scalar(stmt)


class SqlAlchemyReviewRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        *,
        candidate_claim_id: UUID,
        reviewer_principal_id: UUID | None,
        decision: str,
        authority: str | None,
        notes: str | None,
    ) -> None:
        self._session.add(
            ReviewRow(
                candidate_claim_id=candidate_claim_id,
                reviewer_principal_id=reviewer_principal_id,
                decision=decision,
                authority=authority,
                notes=notes,
            )
        )
        await self._session.flush()


class SqlAlchemyPublishedEpisodeRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

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
    ) -> bool:
        stmt = (
            pg_insert(PublishedEpisodeRow)
            .values(
                source_id=source_id,
                group_id=group_id,
                knowledge_type=knowledge_type,
                verification=verification,
                authority=authority,
                reference_time=reference_time,
                payload=payload,
                dedup_uuid=dedup_uuid,
                ontology_version_id=ontology_version_id,
                pipeline=pipeline or {},
            )
            .on_conflict_do_nothing(constraint="uq_episode_dedup")
            .returning(PublishedEpisodeRow.id)
        )
        result = await self._session.execute(stmt)
        return result.first() is not None

    async def invalidate(
        self, *, group_id: str, source_id: str, invalid_at: datetime, superseded_by_source: str
    ) -> None:
        await self._session.execute(
            update(PublishedEpisodeRow)
            .where(
                PublishedEpisodeRow.group_id == group_id,
                PublishedEpisodeRow.source_id == source_id,
            )
            .values(invalid_at=invalid_at, superseded_by_source=superseded_by_source)
        )
