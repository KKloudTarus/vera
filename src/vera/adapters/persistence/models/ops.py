"""Operational tables: sync jobs and cursors, ontology versions, and the two
append-heavy tables (audit_events, retrieval_feedback) that are range-partitioned
by month so retention is a partition drop rather than a bulk delete.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from vera.adapters.persistence.base import Base
from vera.adapters.persistence.models._mixins import UUIDPK


class SyncJobRow(Base, UUIDPK):
    __tablename__ = "sync_jobs"

    source_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("knowledge_sources.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="pending")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    stats: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','running','succeeded','failed')", name="ck_sync_status"
        ),
        Index("ix_sync_jobs_source", "source_id", "created_at"),
    )


class SyncCursorRow(Base, UUIDPK):
    __tablename__ = "sync_cursors"

    source_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("knowledge_sources.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    cursor: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class OntologyVersionRow(Base, UUIDPK):
    __tablename__ = "ontology_versions"

    version: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    entity_types: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    edge_types: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    # Per-predicate governance (cardinality, absence, conflict) versioned with the ontology,
    # so reconciliation reads the same rules the version was published under.
    predicate_policies: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class LlmUsageRow(Base, UUIDPK):
    """One provider call's token usage and estimated cost, attributed to a request.

    ``request_kind`` is 'ingest' or 'search'; ``ref`` is the source_id for ingest, so
    cost per episode and per query is a simple aggregate over this table.
    """

    __tablename__ = "llm_usage"

    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    model: Mapped[str] = mapped_column(String(128), nullable=False)
    operation: Mapped[str] = mapped_column(String(16), nullable=False)
    request_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    group_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    ref: Mapped[str | None] = mapped_column(String(512), nullable=True)
    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    cost_usd: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")

    __table_args__ = (
        CheckConstraint("operation IN ('llm','embedding')", name="ck_llm_usage_operation"),
        Index("ix_llm_usage_group_time", "group_id", "occurred_at"),
        Index("ix_llm_usage_ref", "ref"),
    )


class AuditEventRow(Base):
    """Append-only audit log, range-partitioned by ``occurred_at`` (monthly)."""

    __tablename__ = "audit_events"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), server_default=text("uuidv7()"))
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    actor: Mapped[str | None] = mapped_column(String(256), nullable=True)
    group_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    target: Mapped[str | None] = mapped_column(String(512), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )

    __table_args__ = (
        PrimaryKeyConstraint("id", "occurred_at", name="pk_audit_events"),
        Index("ix_audit_group_time", "group_id", "occurred_at"),
        {"postgresql_partition_by": "RANGE (occurred_at)"},
    )


class RetrievalFeedbackRow(Base):
    """User feedback on retrieval results, range-partitioned by ``created_at``."""

    __tablename__ = "retrieval_feedback"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), server_default=text("uuidv7()"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    principal_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    result_ref: Mapped[str] = mapped_column(String(512), nullable=False)
    signal: Mapped[str] = mapped_column(String(8), nullable=False)
    weight: Mapped[float] = mapped_column(Float, nullable=False, server_default="1.0")
    # Rerank signal vector shown for this result, logged at feedback time for calibration.
    signals: Mapped[dict[str, float] | None] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint("id", "created_at", name="pk_retrieval_feedback"),
        CheckConstraint("signal IN ('up','down')", name="ck_feedback_signal"),
        Index("ix_feedback_group_time", "group_id", "created_at"),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )


class RerankWeightsRow(Base, UUIDPK):
    """A calibrated rerank weight set. The latest active row is loaded at startup and used
    in place of the configured defaults, so feedback-driven calibration takes effect.
    """

    __tablename__ = "rerank_weights"

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    w_relevance: Mapped[float] = mapped_column(Float, nullable=False)
    w_authority: Mapped[float] = mapped_column(Float, nullable=False)
    w_verification: Mapped[float] = mapped_column(Float, nullable=False)
    w_recency: Mapped[float] = mapped_column(Float, nullable=False)
    w_feedback: Mapped[float] = mapped_column(Float, nullable=False)
    w_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    half_life_s: Mapped[float] = mapped_column(Float, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    active: Mapped[bool] = mapped_column(nullable=False, server_default=text("true"))

    __table_args__ = (Index("ix_rerank_weights_active", "active", "created_at"),)


class GroupEmbeddingStateRow(Base):
    """The embedding model and dimension a group's vectors were built with.

    Neo4j vectors must share one dimension per group or similarity breaks, so ingestion
    records the fingerprint on first write and refuses a later write under a different
    model or dimension until the group is reprocessed (re-embedded) under the new config.
    """

    __tablename__ = "group_embedding_state"

    group_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    embedding_model: Mapped[str] = mapped_column(String(128), nullable=False)
    embedding_dim: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
