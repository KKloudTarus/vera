"""alias normalization keeps diacritics: make alias_norm an app-written Unicode column

``entity_aliases.alias_norm`` was a generated column using ``[^a-zA-Z0-9]``, which deletes
Vietnamese (and other) diacritics and mangles non-ASCII names (``"Đội"`` became ``"i"``).
The database cannot fold accents in an IMMUTABLE expression, and folding would be wrong for
Vietnamese anyway. So the column becomes a plain column written by the app from
``normalize_name`` (Unicode-aware, diacritics preserved), and existing rows are backfilled
with the same function so the exact-match index keeps matching app-side lookups.

Revision ID: c8d4e2f1a3b5
Revises: b7f3c1a2d4e5
Create Date: 2026-09-01
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from vera.shared.text import normalize_name

revision: str = "c8d4e2f1a3b5"
down_revision: str | None = "b7f3c1a2d4e5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_alias_norm_trgm")
    op.execute("ALTER TABLE entity_aliases DROP CONSTRAINT IF EXISTS uq_alias_norm")
    op.execute("ALTER TABLE entity_aliases DROP COLUMN IF EXISTS alias_norm")
    op.execute("ALTER TABLE entity_aliases ADD COLUMN alias_norm varchar(512)")

    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT id, alias FROM entity_aliases")).fetchall()
    for row in rows:
        bind.execute(
            sa.text("UPDATE entity_aliases SET alias_norm = :norm WHERE id = :id"),
            {"norm": normalize_name(row.alias), "id": row.id},
        )

    op.execute("ALTER TABLE entity_aliases ALTER COLUMN alias_norm SET NOT NULL")
    op.execute(
        "ALTER TABLE entity_aliases ADD CONSTRAINT uq_alias_norm UNIQUE (group_id, alias_norm)"
    )
    op.execute(
        "CREATE INDEX ix_alias_norm_trgm ON entity_aliases USING gin (alias_norm gin_trgm_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_alias_norm_trgm")
    op.execute("ALTER TABLE entity_aliases DROP CONSTRAINT IF EXISTS uq_alias_norm")
    op.execute("ALTER TABLE entity_aliases DROP COLUMN IF EXISTS alias_norm")
    op.execute(
        "ALTER TABLE entity_aliases ADD COLUMN alias_norm varchar(512) "
        "GENERATED ALWAYS AS (lower(btrim(regexp_replace(alias, '[^a-zA-Z0-9]+', ' ', 'g')))) "
        "STORED"
    )
    op.execute(
        "ALTER TABLE entity_aliases ADD CONSTRAINT uq_alias_norm UNIQUE (group_id, alias_norm)"
    )
    op.execute(
        "CREATE INDEX ix_alias_norm_trgm ON entity_aliases USING gin (alias_norm gin_trgm_ops)"
    )
