"""artifact source watermark

Revision ID: 2d4e5f6a7b8c
Revises: 1c3d4e5f6a7b
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "2d4e5f6a7b8c"
down_revision: str | None = "1c3d4e5f6a7b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("artifacts", sa.Column("source_revision", sa.BigInteger(), nullable=True))
    op.add_column(
        "artifacts", sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("artifacts", sa.Column("source_version_id", sa.String(256), nullable=True))
    op.execute(
        "UPDATE artifacts a SET source_revision = av.source_revision, "
        "source_updated_at = av.source_updated_at, source_version_id = av.source_version_id "
        "FROM artifact_versions av "
        "WHERE av.artifact_id = a.id AND av.version = a.current_version"
    )


def downgrade() -> None:
    op.drop_column("artifacts", "source_version_id")
    op.drop_column("artifacts", "source_updated_at")
    op.drop_column("artifacts", "source_revision")
