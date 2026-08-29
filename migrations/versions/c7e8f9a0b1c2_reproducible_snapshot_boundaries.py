"""reproducible snapshot boundaries

Separates valid time from the system freeze boundary and pins retrieval and graph projection
versions on each snapshot.

Revision ID: c7e8f9a0b1c2
Revises: b6d7e8f9a0b1
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c7e8f9a0b1c2"
down_revision: str | None = "b6d7e8f9a0b1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "knowledge_snapshots",
        "as_of_system_time",
        new_column_name="frozen_at_system_time",
    )
    op.execute(
        "UPDATE knowledge_snapshots SET as_of_valid_time = frozen_at_system_time "
        "WHERE as_of_valid_time IS NULL"
    )
    op.alter_column("knowledge_snapshots", "as_of_valid_time", nullable=False)
    op.add_column(
        "knowledge_snapshots",
        sa.Column(
            "embedding_version",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "knowledge_snapshots",
        sa.Column(
            "retrieval_index_version",
            sa.String(64),
            nullable=False,
            server_default="fts-v1",
        ),
    )
    op.add_column(
        "knowledge_snapshots",
        sa.Column("graph_projection_checkpoint", postgresql.UUID(as_uuid=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("knowledge_snapshots", "graph_projection_checkpoint")
    op.drop_column("knowledge_snapshots", "retrieval_index_version")
    op.drop_column("knowledge_snapshots", "embedding_version")
    op.alter_column("knowledge_snapshots", "as_of_valid_time", nullable=True)
    op.alter_column(
        "knowledge_snapshots",
        "frozen_at_system_time",
        new_column_name="as_of_system_time",
    )
