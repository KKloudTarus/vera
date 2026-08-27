"""Knowledge tables: the raw-to-verified pipeline plus its provenance.

Flow: knowledge_source produces artifacts, each artifact keeps immutable versions,
a version yields candidate_claims, reviews move a claim to verified, and a verified
claim becomes a published_episode that the ingestion worker sends to the graph.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from vera.adapters.persistence.base import Base
from vera.adapters.persistence.models._mixins import UUIDPK, Timestamps
from vera.domain.knowledge.models import (
    ClaimType,
    ReviewDecision,
    SourceKind,
    VerificationStatus,
)

_SOURCE_KINDS = ", ".join(f"'{k.value}'" for k in SourceKind)
_CLAIM_TYPES = ", ".join(f"'{k.value}'" for k in ClaimType)
_VERIF = ", ".join(f"'{k.value}'" for k in VerificationStatus)
_DECISIONS = ", ".join(f"'{k.value}'" for k in ReviewDecision)


class KnowledgeSourceRow(Base, UUIDPK, Timestamps):
    __tablename__ = "knowledge_sources"

    workspace_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False
    )
    project_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=True
    )
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    trust_tier: Mapped[int] = mapped_column(Integer, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))

    __table_args__ = (
        CheckConstraint(f"kind IN ({_SOURCE_KINDS})", name="ck_source_kind"),
        CheckConstraint("trust_tier BETWEEN 1 AND 4", name="ck_source_trust_tier"),
    )


class ArtifactRow(Base, UUIDPK, Timestamps):
    __tablename__ = "artifacts"

    source_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("knowledge_sources.id", ondelete="CASCADE"), nullable=False
    )
    external_id: Mapped[str] = mapped_column(String(512), nullable=False)
    title: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    s3_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    reference_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    current_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")

    __table_args__ = (UniqueConstraint("source_id", "external_id", name="uq_artifact_external"),)


class ArtifactVersionRow(Base, UUIDPK):
    __tablename__ = "artifact_versions"

    artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifacts.id", ondelete="CASCADE"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    s3_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    reference_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (UniqueConstraint("artifact_id", "version", name="uq_artifact_version"),)


class CandidateClaimRow(Base, UUIDPK, Timestamps):
    __tablename__ = "candidate_claims"

    artifact_version_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("artifact_versions.id", ondelete="CASCADE"),
        nullable=False,
    )
    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    statement: Mapped[str] = mapped_column(Text, nullable=False)
    claim_type: Mapped[str] = mapped_column(String(32), nullable=False)
    verification_status: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=VerificationStatus.UNVERIFIED.value
    )
    subject: Mapped[str | None] = mapped_column(String(512), nullable=True)
    predicate: Mapped[str | None] = mapped_column(String(256), nullable=True)
    object: Mapped[str | None] = mapped_column(String(512), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    version_id: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")

    __mapper_args__ = {"version_id_col": version_id}  # noqa: RUF012  SQLAlchemy config dict
    __table_args__ = (
        CheckConstraint(f"claim_type IN ({_CLAIM_TYPES})", name="ck_claim_type"),
        CheckConstraint(f"verification_status IN ({_VERIF})", name="ck_claim_status"),
    )


class ReviewRow(Base, UUIDPK):
    __tablename__ = "reviews"

    candidate_claim_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("candidate_claims.id", ondelete="CASCADE"), nullable=False
    )
    reviewer_principal_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("principals.id", ondelete="SET NULL"), nullable=True
    )
    decision: Mapped[str] = mapped_column(String(32), nullable=False)
    authority: Mapped[str | None] = mapped_column(String(128), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (CheckConstraint(f"decision IN ({_DECISIONS})", name="ck_review_decision"),)


class PublishedEpisodeRow(Base, UUIDPK):
    __tablename__ = "published_episodes"

    source_id: Mapped[str] = mapped_column(String(512), nullable=False)
    artifact_version_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifact_versions.id", ondelete="SET NULL"), nullable=True
    )
    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    knowledge_type: Mapped[str] = mapped_column(String(64), nullable=False)
    verification: Mapped[str] = mapped_column(String(64), nullable=False)
    authority: Mapped[float] = mapped_column(Float, nullable=False, server_default="0.5")
    confidence: Mapped[float] = mapped_column(Float, nullable=False, server_default="1.0")
    reference_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    ontology_version_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("ontology_versions.id", ondelete="SET NULL"), nullable=True
    )
    pipeline: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    dedup_uuid: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    # Bi-temporal invalidation: set when a newer, contradicting fact supersedes this one.
    invalid_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    superseded_by_source: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("dedup_uuid", name="uq_episode_dedup"),
        UniqueConstraint("group_id", "source_id", name="uq_episode_group_source"),
    )
