"""tenant-scoped legal holds

Revision ID: af2a3b4c5d6e
Revises: c8d4e2f1a3b5
Create Date: 2026-08-31
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "af2a3b4c5d6e"
down_revision: str | None = "c8d4e2f1a3b5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "legal_holds",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            server_default=sa.text("uuidv7()"),
        ),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("source_id", sa.String(512), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_id", "source_id", name="uq_legal_hold_group_source"),
    )
    op.execute("ALTER TABLE legal_holds ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE legal_holds FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON legal_holds "
        "USING (group_id = current_setting('vera.group_id', true)) "
        "WITH CHECK (group_id = current_setting('vera.group_id', true))"
    )
    op.execute("REVOKE ALL ON legal_holds FROM vera_app, vera_trusted, vera_worker")
    op.execute("GRANT SELECT, INSERT, UPDATE ON legal_holds TO vera_app")
    op.execute("GRANT SELECT ON legal_holds TO vera_trusted")
    op.execute("GRANT SELECT, INSERT, UPDATE ON legal_holds TO vera_worker")


def downgrade() -> None:
    op.drop_table("legal_holds")
