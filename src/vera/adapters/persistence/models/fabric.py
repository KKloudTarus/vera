"""Knowledge Fabric tables (Phase 1): chunks, facts, assertions, evidence, fact_relations,
and the append-only knowledge_events ledger.

All are tenant-scoped by ``group_id`` and take the same ``tenant_isolation`` RLS policy as
the existing knowledge tables (added in the Phase 1 migration). These are additive: no
existing table changes. See docs/design/knowledge-fabric.md and docs/adr/0001, 0004.
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
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from vera.adapters.persistence.base import Base
from vera.adapters.persistence.models._mixins import UUIDPK, Timestamps
from vera.domain.knowledge.fabric import (
    AssertionState,
    FactLifecycle,
    KnowledgeEventType,
    ObjectType,
    Polarity,
    RelationType,
)

_LIFECYCLE = ", ".join(f"'{s.value}'" for s in FactLifecycle)
_POLARITY = ", ".join(f"'{s.value}'" for s in Polarity)
_OBJECT_TYPE = ", ".join(f"'{s.value}'" for s in ObjectType)
_ASSERTION_STATE = ", ".join(f"'{s.value}'" for s in AssertionState)
_RELATION_TYPE = ", ".join(f"'{s.value}'" for s in RelationType)
_EVENT_TYPE = ", ".join(f"'{s.value}'" for s in KnowledgeEventType)


class ChunkRow(Base, UUIDPK):
    __tablename__ = "chunks"

    artifact_version_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("artifact_versions.id", ondelete="CASCADE"),
        nullable=False,
    )
    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    chunk_key: Mapped[str] = mapped_column(String(128), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    token_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    heading_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_offset: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_offset: Mapped[int | None] = mapped_column(Integer, nullable=True)
    page_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    symbol_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    start_line: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_line: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parent_chunk_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("chunks.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("group_id", "chunk_key", name="uq_chunk_key"),
        UniqueConstraint("artifact_version_id", "ordinal", name="uq_chunk_ordinal"),
        Index("ix_chunks_version", "artifact_version_id"),
    )


class ExtractionRunRow(Base, UUIDPK):
    __tablename__ = "extraction_runs"

    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    artifact_version_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("artifact_versions.id", ondelete="CASCADE"),
        nullable=False,
    )
    model: Mapped[str] = mapped_column(String(256), nullable=False)
    provider: Mapped[str] = mapped_column(String(128), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(64), nullable=False)
    pipeline_version: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_extraction_runs_version", "artifact_version_id"),)


class FactRow(Base, UUIDPK, Timestamps):
    __tablename__ = "facts"

    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    fact_key: Mapped[str] = mapped_column(String(128), nullable=False)
    slot_key: Mapped[str] = mapped_column(String(128), nullable=False)
    subject_entity_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("canonical_entities.id"), nullable=False
    )
    predicate: Mapped[str] = mapped_column(String(256), nullable=False)
    object_type: Mapped[str] = mapped_column(String(16), nullable=False)
    normalized_object: Mapped[str] = mapped_column(Text, nullable=False)
    object_entity_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("canonical_entities.id"), nullable=True
    )
    object_scalar: Mapped[str | None] = mapped_column(Text, nullable=True)
    qualifiers: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    lifecycle_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=FactLifecycle.PROPOSED.value
    )
    authority: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    confidence: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    system_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    system_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ontology_version_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("ontology_versions.id", ondelete="SET NULL"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("id", "group_id", name="uq_facts_tenant_snapshot"),
        CheckConstraint(f"lifecycle_state IN ({_LIFECYCLE})", name="ck_fact_lifecycle"),
        CheckConstraint(f"object_type IN ({_OBJECT_TYPE})", name="ck_fact_object_type"),
        CheckConstraint(
            "(object_type = 'entity' AND object_entity_id IS NOT NULL) "
            "OR (object_type = 'scalar' AND object_scalar IS NOT NULL)",
            name="ck_fact_object_present",
        ),
        # At most one active fact per logical proposition.
        Index(
            "uq_fact_active_key",
            "group_id",
            "fact_key",
            unique=True,
            postgresql_where=text("lifecycle_state = 'active'"),
        ),
        Index("ix_facts_slot", "group_id", "slot_key"),
        Index("ix_facts_subject", "subject_entity_id"),
        Index(
            "ix_facts_expiry",
            "expires_at",
            postgresql_where=text("lifecycle_state = 'active' AND expires_at IS NOT NULL"),
        ),
    )


class AssertionRow(Base, UUIDPK):
    __tablename__ = "assertions"

    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    fact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("facts.id", ondelete="CASCADE"), nullable=False
    )
    polarity: Mapped[str] = mapped_column(String(16), nullable=False)
    knowledge_source_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("knowledge_sources.id", ondelete="SET NULL"), nullable=True
    )
    artifact_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifacts.id", ondelete="SET NULL"), nullable=True
    )
    artifact_version_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifact_versions.id", ondelete="SET NULL"), nullable=True
    )
    extractor_confidence: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    source_authority: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    verification_state: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default="unverified"
    )
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    extraction_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("extraction_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    run_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=AssertionState.ACTIVE.value
    )
    withdrawn_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(f"polarity IN ({_POLARITY})", name="ck_assertion_polarity"),
        CheckConstraint(f"state IN ({_ASSERTION_STATE})", name="ck_assertion_state"),
        # One assertion per (fact, source version, polarity): re-ingest reaffirms in place.
        UniqueConstraint("fact_id", "artifact_version_id", "polarity", name="uq_assertion_source"),
        Index(
            "uq_assertion_run_key",
            "fact_id",
            "run_key",
            "polarity",
            unique=True,
            postgresql_where=text("run_key IS NOT NULL"),
        ),
        Index("ix_assertions_fact", "group_id", "fact_id"),
    )


class EvidenceRow(Base, UUIDPK):
    __tablename__ = "evidence"

    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    assertion_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("assertions.id", ondelete="CASCADE"), nullable=False
    )
    chunk_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("chunks.id", ondelete="SET NULL"), nullable=True
    )
    artifact_version_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("artifact_versions.id", ondelete="SET NULL"), nullable=True
    )
    structured_record: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    excerpt: Mapped[str | None] = mapped_column(Text, nullable=True)
    citation_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    quote_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quote_end: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quote_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    citation_override: Mapped[str | None] = mapped_column(Text, nullable=True)
    extraction_run_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("extraction_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    source_coordinates: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    confidentiality: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default="internal"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("assertion_id", "content_hash", name="uq_evidence_hash"),
        CheckConstraint(
            "(quote_start IS NULL AND quote_end IS NULL AND quote_hash IS NULL) OR "
            "(quote_start >= 0 AND quote_end > quote_start AND quote_hash IS NOT NULL)",
            name="ck_evidence_quote_offsets",
        ),
        Index("ix_evidence_assertion", "group_id", "assertion_id"),
    )


class FactRelationRow(Base, UUIDPK):
    __tablename__ = "fact_relations"

    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    from_fact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("facts.id", ondelete="CASCADE"), nullable=False
    )
    to_fact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("facts.id", ondelete="CASCADE"), nullable=False
    )
    relation_type: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(f"relation_type IN ({_RELATION_TYPE})", name="ck_relation_type"),
        UniqueConstraint("from_fact_id", "to_fact_id", "relation_type", name="uq_fact_relation"),
        Index("ix_relations_from", "group_id", "from_fact_id"),
    )


class KnowledgeEventRow(Base):
    """Append-only semantic change ledger, range-partitioned by ``occurred_at`` monthly
    (the pattern used by ``audit_events``). No cross-partition FKs by design; subject ids are
    plain UUIDs. See docs/adr/0004.
    """

    __tablename__ = "knowledge_events"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), server_default=text("uuidv7()"))
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    event_type: Mapped[str] = mapped_column(String(48), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(256), nullable=True)
    source_id: Mapped[str | None] = mapped_column(String(512), nullable=True)
    fact_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    assertion_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    artifact_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    entity_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    previous_state: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    next_state: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    policy_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    trace_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint("id", "occurred_at", name="pk_knowledge_events"),
        CheckConstraint(f"event_type IN ({_EVENT_TYPE})", name="ck_knowledge_event_type"),
        Index("ix_knowledge_events_group_time", "group_id", "occurred_at"),
        Index("ix_knowledge_events_fact", "fact_id"),
        {"postgresql_partition_by": "RANGE (occurred_at)"},
    )
