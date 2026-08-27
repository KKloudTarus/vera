"""published_episodes bi-temporal invalidation columns

Adds invalid_at and superseded_by_source so a newer, contradicting fact can supersede
an older one (temporal update) while history remains queryable.

Revision ID: d9a2b4c6e8f1
Revises: c8e1f0a9b2d3
Create Date: 2026-08-27 14:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d9a2b4c6e8f1"
down_revision: str | None = "c8e1f0a9b2d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "published_episodes",
        sa.Column("invalid_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "published_episodes",
        sa.Column("superseded_by_source", sa.String(length=512), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("published_episodes", "superseded_by_source")
    op.drop_column("published_episodes", "invalid_at")
