"""feedback signal snapshot for rerank calibration

Revision ID: a3b4c5d6e7f8
Revises: f2b3c4d5e6a7
Create Date: 2026-08-27

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a3b4c5d6e7f8"
down_revision: str | None = "f2b3c4d5e6a7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Snapshot of the rerank signal vector shown for this result, logged when feedback is
    # given so weights can be calibrated later from real thumbs up/down (feature logging).
    op.add_column(
        "retrieval_feedback",
        sa.Column("signals", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("retrieval_feedback", "signals")
