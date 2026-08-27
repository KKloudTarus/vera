"""calibrated rerank weights

Revision ID: c5d6e7f8a9ba
Revises: b4c5d6e7f8a9
Create Date: 2026-08-28

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c5d6e7f8a9ba"
down_revision: str | None = "b4c5d6e7f8a9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "rerank_weights",
        sa.Column("id", sa.UUID(as_uuid=True), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("w_relevance", sa.Float(), nullable=False),
        sa.Column("w_authority", sa.Float(), nullable=False),
        sa.Column("w_verification", sa.Float(), nullable=False),
        sa.Column("w_recency", sa.Float(), nullable=False),
        sa.Column("w_feedback", sa.Float(), nullable=False),
        sa.Column("w_confidence", sa.Float(), nullable=False),
        sa.Column("half_life_s", sa.Float(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_rerank_weights"),
    )
    op.create_index("ix_rerank_weights_active", "rerank_weights", ["active", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_rerank_weights_active", table_name="rerank_weights")
    op.drop_table("rerank_weights")
