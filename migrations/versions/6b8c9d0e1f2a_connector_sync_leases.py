"""connector sync leases

Revision ID: 6b8c9d0e1f2a
Revises: 5a7b8c9d0e1f
Create Date: 2026-08-31
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "6b8c9d0e1f2a"
down_revision: str | None = "5a7b8c9d0e1f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "knowledge_sources",
        sa.Column("sync_lease_owner", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "knowledge_sources",
        sa.Column("sync_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("knowledge_sources", "sync_lease_expires_at")
    op.drop_column("knowledge_sources", "sync_lease_owner")
