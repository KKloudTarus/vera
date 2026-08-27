"""ingestion trace_context

Revision ID: 549e6556e266
Revises: 4a6e79e1e912
Create Date: 2026-08-27 07:50:50.546064+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "549e6556e266"
down_revision: str | None = "4a6e79e1e912"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "ingestion_jobs",
        sa.Column(
            "trace_context",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("ingestion_jobs", "trace_context")
