"""knowledge fabric phase 4: full-text search indexes on chunks and facts

Adds a generated ``search_vector`` tsvector column plus a GIN index to both ``chunks`` and
``facts``. The columns are GENERATED ALWAYS from stored text, so the index is a rebuildable
projection of the authoritative rows (ADR-0003): dropping and recreating it changes nothing.
Additive only; no existing column changes.

Revision ID: a7c9e1f2b3d4
Revises: f3a1b2c4d5e6
Create Date: 2026-08-28
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "a7c9e1f2b3d4"
down_revision: str | None = "f3a1b2c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE chunks ADD COLUMN search_vector tsvector "
        "GENERATED ALWAYS AS (to_tsvector('english', "
        "coalesce(heading_path,'') || ' ' || coalesce(symbol_name,'') || ' ' || text)) STORED"
    )
    op.execute("CREATE INDEX ix_chunks_fts ON chunks USING gin (search_vector)")
    op.execute(
        "ALTER TABLE facts ADD COLUMN search_vector tsvector "
        "GENERATED ALWAYS AS (to_tsvector('english', "
        "predicate || ' ' || normalized_object || ' ' || coalesce(object_scalar,''))) STORED"
    )
    op.execute("CREATE INDEX ix_facts_fts ON facts USING gin (search_vector)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_facts_fts")
    op.execute("ALTER TABLE facts DROP COLUMN IF EXISTS search_vector")
    op.execute("DROP INDEX IF EXISTS ix_chunks_fts")
    op.execute("ALTER TABLE chunks DROP COLUMN IF EXISTS search_vector")
