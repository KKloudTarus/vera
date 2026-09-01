"""add the context-pack expiry index without blocking writes

Revision ID: 9e1f2a3b4c5d
Revises: 8d0e1f2a3b4c
Create Date: 2026-08-31
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "9e1f2a3b4c5d"
down_revision: str | None = "8d0e1f2a3b4c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # A failed concurrent build leaves an invalid index behind. Drop first so rerunning this
    # unstamped revision repairs either a valid or invalid partial application.
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_context_packs_expires_at")
        op.execute(
            "CREATE INDEX CONCURRENTLY ix_context_packs_expires_at ON context_packs (expires_at)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_context_packs_expires_at")
