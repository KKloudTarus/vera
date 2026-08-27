"""published_episodes.confidence

Extraction confidence carried onto the episode so the retrieval rerank can weigh it.

Revision ID: e1f2a3b4c5d6
Revises: d9a2b4c6e8f1
Create Date: 2026-08-27 15:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e1f2a3b4c5d6"
down_revision: str | None = "d9a2b4c6e8f1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "published_episodes",
        sa.Column("confidence", sa.Float(), server_default="1.0", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("published_episodes", "confidence")
