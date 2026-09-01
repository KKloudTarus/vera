"""multilingual full-text search: rebuild chunk/fact search_vector with the 'simple' config

The generated ``search_vector`` columns used the ``'english'`` text-search configuration,
which stems and stopword-filters English and mangles non-English (e.g. Vietnamese) tokens.
Switch them to ``'simple'`` (no stemming or stopwords, diacritics preserved), so tokens in
any language index and match by their exact form. The columns are GENERATED and a rebuildable
projection of the stored text (ADR-0003), so dropping and recreating them changes no
authoritative data; the query side switches to ``'simple'`` in the same change.

Revision ID: b7f3c1a2d4e5
Revises: 9e1f2a3b4c5d
Create Date: 2026-09-01
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "b7f3c1a2d4e5"
down_revision: str | None = "9e1f2a3b4c5d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CHUNKS_EXPR = (
    "coalesce(heading_path,'') || ' ' || coalesce(symbol_name,'') || ' ' || text"
)
_FACTS_EXPR = "predicate || ' ' || normalized_object || ' ' || coalesce(object_scalar,'')"


def _rebuild(table: str, index: str, config: str, expr: str) -> None:
    op.execute(f"DROP INDEX IF EXISTS {index}")
    op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS search_vector")
    op.execute(
        f"ALTER TABLE {table} ADD COLUMN search_vector tsvector "
        f"GENERATED ALWAYS AS (to_tsvector('{config}', {expr})) STORED"
    )
    op.execute(f"CREATE INDEX {index} ON {table} USING gin (search_vector)")


def upgrade() -> None:
    _rebuild("chunks", "ix_chunks_fts", "simple", _CHUNKS_EXPR)
    _rebuild("facts", "ix_facts_fts", "simple", _FACTS_EXPR)


def downgrade() -> None:
    _rebuild("chunks", "ix_chunks_fts", "english", _CHUNKS_EXPR)
    _rebuild("facts", "ix_facts_fts", "english", _FACTS_EXPR)
