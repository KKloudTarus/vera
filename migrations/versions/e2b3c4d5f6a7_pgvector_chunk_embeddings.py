"""pgvector: optional dense-embedding column and ANN index on chunks

Adds a ``vector`` embedding column and an HNSW cosine index to ``chunks`` so passage retrieval
can use approximate nearest-neighbor search behind the existing PassageIndex port, alongside
the full-text default. The whole step is conditional on the ``vector`` extension being
available: on a stock postgres image it is a no-op, so ``make migrate`` still succeeds and
ingestion is unaffected; on the pgvector image it enables the column and index. The column is
nullable, so ingestion that does not compute embeddings keeps working; a backfill fills them.

The dimension is frozen at 1024 to match the platform default embedder; a deployment using a
different embedding dimension adjusts this migration and the embedding backfill together.

Revision ID: e2b3c4d5f6a7
Revises: d1a2b3c4e5f6
Create Date: 2026-08-28
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "e2b3c4d5f6a7"
down_revision: str | None = "d1a2b3c4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_VECTOR_DIM = 1024

_UPGRADE = f"""
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') THEN
        CREATE EXTENSION IF NOT EXISTS vector;
        ALTER TABLE chunks ADD COLUMN IF NOT EXISTS embedding vector({_VECTOR_DIM});
        CREATE INDEX IF NOT EXISTS ix_chunks_embedding
            ON chunks USING hnsw (embedding vector_cosine_ops);
    ELSE
        RAISE NOTICE 'pgvector not available; skipping chunk embedding column (FTS remains)';
    END IF;
END $$;
"""

_DOWNGRADE = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'chunks' AND column_name = 'embedding') THEN
        DROP INDEX IF EXISTS ix_chunks_embedding;
        ALTER TABLE chunks DROP COLUMN embedding;
    END IF;
END $$;
"""


def upgrade() -> None:
    op.execute(_UPGRADE)


def downgrade() -> None:
    op.execute(_DOWNGRADE)
