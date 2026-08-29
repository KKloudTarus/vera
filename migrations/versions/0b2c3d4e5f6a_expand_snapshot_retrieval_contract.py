"""expand snapshot retrieval contract

Revision ID: 0b2c3d4e5f6a
Revises: fa1b2c3d4e5f
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0b2c3d4e5f6a"
down_revision: str | None = "fa1b2c3d4e5f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "knowledge_snapshots",
        sa.Column("retrieval_frozen", sa.Boolean, nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "knowledge_snapshots",
        sa.Column(
            "assembler_version",
            sa.String(64),
            nullable=False,
            server_default="context-assembler-v2",
        ),
    )
    op.add_column(
        "context_packs",
        sa.Column(
            "request",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    for column in (
        sa.Column("fact_key", sa.String(128), nullable=True),
        sa.Column("subject_name", sa.Text, nullable=True),
        sa.Column("predicate", sa.String(256), nullable=True),
        sa.Column("object_name", sa.Text, nullable=True),
        sa.Column("normalized_object", sa.Text, nullable=True),
        sa.Column("object_scalar", sa.Text, nullable=True),
        sa.Column("authority", sa.Float, nullable=True),
        sa.Column("confidence", sa.Float, nullable=True),
        sa.Column("lifecycle_state", sa.String(16), nullable=True),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=True),
    ):
        op.add_column("snapshot_facts", column)


def downgrade() -> None:
    for column in (
        "valid_from",
        "lifecycle_state",
        "confidence",
        "authority",
        "object_name",
        "object_scalar",
        "normalized_object",
        "predicate",
        "subject_name",
        "fact_key",
    ):
        op.drop_column("snapshot_facts", column)
    op.drop_column("context_packs", "request")
    op.drop_column("knowledge_snapshots", "assembler_version")
    op.drop_column("knowledge_snapshots", "retrieval_frozen")
