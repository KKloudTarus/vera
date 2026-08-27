"""published episode retraction

Revision ID: d6e7f8a9bacb
Revises: c5d6e7f8a9ba
Create Date: 2026-08-28

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d6e7f8a9bacb"
down_revision: str | None = "c5d6e7f8a9ba"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "published_episodes",
        sa.Column("retracted_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("published_episodes", "retracted_at")
