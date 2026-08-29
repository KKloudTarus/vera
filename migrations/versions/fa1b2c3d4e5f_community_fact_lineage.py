"""governed community fact lineage

Revision ID: fa1b2c3d4e5f
Revises: e9f0a1b2c3d4
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "fa1b2c3d4e5f"
down_revision: str | None = "e9f0a1b2c3d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "community_fact_lineage",
        sa.Column("community_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "fact_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("facts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("derivation_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint(
            "community_id", "fact_id", "derivation_run_id", name="pk_community_fact_lineage"
        ),
    )
    op.create_index(
        "ix_community_lineage_group_community_run",
        "community_fact_lineage",
        ["group_id", "community_id", "derivation_run_id"],
    )
    op.create_index("ix_community_lineage_fact", "community_fact_lineage", ["fact_id"])
    op.execute("ALTER TABLE community_fact_lineage ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE community_fact_lineage FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON community_fact_lineage "
        "USING (group_id = current_setting('vera.group_id', true)) "
        "WITH CHECK (group_id = current_setting('vera.group_id', true))"
    )
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON community_fact_lineage TO vera_app")
    op.execute("GRANT SELECT ON community_fact_lineage TO vera_trusted")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON community_fact_lineage TO vera_worker")


def downgrade() -> None:
    op.drop_table("community_fact_lineage")
