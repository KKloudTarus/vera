"""knowledge fabric phase 1: chunks, facts, assertions, evidence, relations, events

Additive only. Introduces the authoritative fact model (docs/adr/0001, 0002, 0004, 0005)
alongside the existing knowledge tables; nothing existing is altered. All group-scoped tables
get the same tenant_isolation RLS policy as the current knowledge tables, and knowledge_events
is range-partitioned by month like audit_events.

Revision ID: f3a1b2c4d5e6
Revises: d6e7f8a9bacb
Create Date: 2026-08-28
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from vera.domain.knowledge.fabric import (
    AssertionState,
    FactLifecycle,
    KnowledgeEventType,
    ObjectType,
    Polarity,
    RelationType,
)

revision: str = "f3a1b2c4d5e6"
down_revision: str | None = "d6e7f8a9bacb"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

APP_ROLE = "vera_app"

# group-scoped tables that need the tenant_isolation RLS policy.
_RLS_TABLES = ("chunks", "facts", "assertions", "evidence", "fact_relations", "knowledge_events")


def _in_list(enum: type) -> str:
    return ", ".join(f"'{m.value}'" for m in enum)


def upgrade() -> None:
    op.create_table(
        "chunks",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuidv7()")),
        sa.Column(
            "artifact_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("artifact_versions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("chunk_key", sa.String(128), nullable=False),
        sa.Column("ordinal", sa.Integer, nullable=False),
        sa.Column("text", sa.Text, nullable=False),
        sa.Column("content_hash", sa.String(128), nullable=False),
        sa.Column("token_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("heading_path", sa.Text, nullable=True),
        sa.Column("start_offset", sa.Integer, nullable=True),
        sa.Column("end_offset", sa.Integer, nullable=True),
        sa.Column("page_number", sa.Integer, nullable=True),
        sa.Column("symbol_name", sa.String(512), nullable=True),
        sa.Column("start_line", sa.Integer, nullable=True),
        sa.Column("end_line", sa.Integer, nullable=True),
        sa.Column(
            "parent_chunk_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("chunks.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_id", "chunk_key", name="uq_chunk_key"),
        sa.UniqueConstraint("artifact_version_id", "ordinal", name="uq_chunk_ordinal"),
    )
    op.create_index("ix_chunks_version", "chunks", ["artifact_version_id"])

    op.create_table(
        "facts",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuidv7()")),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("fact_key", sa.String(128), nullable=False),
        sa.Column("slot_key", sa.String(128), nullable=False),
        sa.Column(
            "subject_entity_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("canonical_entities.id"),
            nullable=False,
        ),
        sa.Column("predicate", sa.String(256), nullable=False),
        sa.Column("object_type", sa.String(16), nullable=False),
        sa.Column("normalized_object", sa.Text, nullable=False),
        sa.Column(
            "object_entity_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("canonical_entities.id"),
            nullable=True,
        ),
        sa.Column("object_scalar", sa.Text, nullable=True),
        sa.Column(
            "qualifiers", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column(
            "lifecycle_state",
            sa.String(16),
            nullable=False,
            server_default=FactLifecycle.PROPOSED.value,
        ),
        sa.Column("authority", sa.Float, nullable=False, server_default="0"),
        sa.Column("confidence", sa.Float, nullable=False, server_default="0"),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "system_from", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("system_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "ontology_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("ontology_versions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            f"lifecycle_state IN ({_in_list(FactLifecycle)})", name="ck_fact_lifecycle"
        ),
        sa.CheckConstraint(f"object_type IN ({_in_list(ObjectType)})", name="ck_fact_object_type"),
        sa.CheckConstraint(
            "(object_type = 'entity' AND object_entity_id IS NOT NULL) "
            "OR (object_type = 'scalar' AND object_scalar IS NOT NULL)",
            name="ck_fact_object_present",
        ),
    )
    op.create_index(
        "uq_fact_active_key",
        "facts",
        ["group_id", "fact_key"],
        unique=True,
        postgresql_where=sa.text("lifecycle_state = 'active'"),
    )
    op.create_index("ix_facts_slot", "facts", ["group_id", "slot_key"])
    op.create_index("ix_facts_subject", "facts", ["subject_entity_id"])

    op.create_table(
        "assertions",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuidv7()")),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column(
            "fact_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("facts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("polarity", sa.String(16), nullable=False),
        sa.Column(
            "knowledge_source_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("knowledge_sources.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "artifact_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("artifacts.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "artifact_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("artifact_versions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("extractor_confidence", sa.Float, nullable=False, server_default="0"),
        sa.Column("source_authority", sa.Float, nullable=False, server_default="0"),
        sa.Column("verification_state", sa.String(32), nullable=False, server_default="unverified"),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("extraction_run_id", sa.String(128), nullable=True),
        sa.Column(
            "state", sa.String(16), nullable=False, server_default=AssertionState.ACTIVE.value
        ),
        sa.Column("withdrawn_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(f"polarity IN ({_in_list(Polarity)})", name="ck_assertion_polarity"),
        sa.CheckConstraint(f"state IN ({_in_list(AssertionState)})", name="ck_assertion_state"),
        sa.UniqueConstraint(
            "fact_id", "artifact_version_id", "polarity", name="uq_assertion_source"
        ),
    )
    op.create_index("ix_assertions_fact", "assertions", ["group_id", "fact_id"])

    op.create_table(
        "evidence",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuidv7()")),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column(
            "assertion_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("assertions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "chunk_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("chunks.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "artifact_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("artifact_versions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("structured_record", postgresql.JSONB, nullable=True),
        sa.Column("excerpt", sa.Text, nullable=True),
        sa.Column("citation_uri", sa.Text, nullable=True),
        sa.Column("content_hash", sa.String(128), nullable=False),
        sa.Column(
            "source_coordinates",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("confidentiality", sa.String(32), nullable=False, server_default="internal"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("assertion_id", "content_hash", name="uq_evidence_hash"),
    )
    op.create_index("ix_evidence_assertion", "evidence", ["group_id", "assertion_id"])

    op.create_table(
        "fact_relations",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuidv7()")),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column(
            "from_fact_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("facts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "to_fact_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("facts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("relation_type", sa.String(32), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(f"relation_type IN ({_in_list(RelationType)})", name="ck_relation_type"),
        sa.UniqueConstraint("from_fact_id", "to_fact_id", "relation_type", name="uq_fact_relation"),
    )
    op.create_index("ix_relations_from", "fact_relations", ["group_id", "from_fact_id"])

    # Append-only ledger, range-partitioned monthly like audit_events.
    op.execute(
        f"""
        CREATE TABLE knowledge_events (
            id uuid NOT NULL DEFAULT uuidv7(),
            occurred_at timestamptz NOT NULL DEFAULT now(),
            group_id varchar(256) NOT NULL,
            event_type varchar(48) NOT NULL,
            actor varchar(256),
            source_id varchar(512),
            fact_id uuid,
            assertion_id uuid,
            artifact_id uuid,
            entity_id uuid,
            previous_state jsonb,
            next_state jsonb,
            reason text,
            policy_version varchar(64),
            trace_id varchar(128),
            CONSTRAINT pk_knowledge_events PRIMARY KEY (id, occurred_at),
            CONSTRAINT ck_knowledge_event_type CHECK (event_type IN ({_in_list(KnowledgeEventType)}))
        ) PARTITION BY RANGE (occurred_at)
        """
    )
    op.execute("CREATE TABLE knowledge_events_default PARTITION OF knowledge_events DEFAULT")
    op.execute(
        "CREATE INDEX ix_knowledge_events_group_time ON knowledge_events (group_id, occurred_at)"
    )
    op.execute("CREATE INDEX ix_knowledge_events_fact ON knowledge_events (fact_id)")

    # Tenant isolation (same policy as the existing knowledge tables) and app-role grants.
    for table in _RLS_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY tenant_isolation ON {table} "
            "USING (group_id = current_setting('vera.group_id', true)) "
            "WITH CHECK (group_id = current_setting('vera.group_id', true))"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {APP_ROLE}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON knowledge_events_default TO {APP_ROLE}")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS knowledge_events CASCADE")
    op.drop_table("fact_relations")
    op.drop_table("evidence")
    op.drop_table("assertions")
    op.drop_table("facts")
    op.drop_table("chunks")
