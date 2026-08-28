"""knowledge fabric phase 5: knowledge snapshots and context packs

Adds immutable snapshots (a frozen set of active fact revisions plus the ontology/policy
versions and source-revision boundaries) and persisted context packs. All group-scoped and
under the tenant_isolation RLS policy. Additive only.

Revision ID: b8d0f1a2c3e4
Revises: a7c9e1f2b3d4
Create Date: 2026-08-28
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b8d0f1a2c3e4"
down_revision: str | None = "a7c9e1f2b3d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

APP_ROLE = "vera_app"
_RLS_TABLES = ("knowledge_snapshots", "snapshot_facts", "context_packs")


def upgrade() -> None:
    op.create_table(
        "knowledge_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuidv7()")),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("as_of_valid_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("as_of_system_time", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column(
            "ontology_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("ontology_versions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("fact_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("source_boundaries", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "snapshot_facts",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("knowledge_snapshots.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "fact_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("facts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.PrimaryKeyConstraint("snapshot_id", "fact_id", name="pk_snapshot_facts"),
    )
    op.create_index("ix_snapshot_facts_group", "snapshot_facts", ["group_id"])

    op.create_table(
        "context_packs",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuidv7()")),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("knowledge_snapshots.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("query", sa.Text, nullable=False),
        sa.Column("hints", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("token_estimate", sa.Integer, nullable=False, server_default="0"),
        sa.Column("result_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("omitted", sa.Integer, nullable=False, server_default="0"),
        sa.Column("conflicts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("freshness_warnings", sa.Integer, nullable=False, server_default="0"),
        sa.Column("results", postgresql.JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.PrimaryKeyConstraint("id"),
        sa.Index("ix_context_packs_group", "group_id", "created_at"),
    )

    for table in _RLS_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY tenant_isolation ON {table} "
            "USING (group_id = current_setting('vera.group_id', true)) "
            "WITH CHECK (group_id = current_setting('vera.group_id', true))"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {APP_ROLE}")


def downgrade() -> None:
    op.drop_table("context_packs")
    op.drop_table("snapshot_facts")
    op.drop_table("knowledge_snapshots")
