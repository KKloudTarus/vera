"""immutable context pack contract

Adds stable request/result metadata, expiry, and database-enforced immutability to persisted
context packs.

Revision ID: d8f9a0b1c2d3
Revises: c7e8f9a0b1c2
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d8f9a0b1c2d3"
down_revision: str | None = "c7e8f9a0b1c2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("context_packs", sa.Column("request_hash", sa.String(64), nullable=True))
    op.add_column(
        "context_packs",
        sa.Column(
            "result_references",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        "context_packs", sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("context_packs", sa.Column("assembler_version", sa.String(64), nullable=True))
    op.execute(
        "UPDATE context_packs SET request_hash = encode(sha256(id::text::bytea), 'hex'), "
        "result_references = COALESCE((SELECT jsonb_agg(item->>'ref') "
        "FROM jsonb_array_elements(results) item WHERE item ? 'ref'), '[]'::jsonb), "
        "expires_at = created_at + interval '30 days', "
        "assembler_version = 'context-assembler-v1'"
    )
    op.alter_column("context_packs", "request_hash", nullable=False)
    op.alter_column("context_packs", "expires_at", nullable=False)
    op.alter_column("context_packs", "assembler_version", nullable=False)
    op.execute(
        """
        CREATE FUNCTION reject_context_pack_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'context packs are immutable';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        "CREATE TRIGGER context_packs_immutable BEFORE UPDATE OR DELETE ON context_packs "
        "FOR EACH ROW EXECUTE FUNCTION reject_context_pack_mutation()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER context_packs_immutable ON context_packs")
    op.execute("DROP FUNCTION reject_context_pack_mutation()")
    op.drop_column("context_packs", "assembler_version")
    op.drop_column("context_packs", "expires_at")
    op.drop_column("context_packs", "result_references")
    op.drop_column("context_packs", "request_hash")
