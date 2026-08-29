"""multi-model chunk embeddings

Moves vectors from the single-model chunks column into a provider-neutral table that retains
multiple model versions concurrently.

Revision ID: b6d7e8f9a0b1
Revises: a5c6d7e8f9b0
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b6d7e8f9a0b1"
down_revision: str | None = "a5c6d7e8f9b0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_vector() -> bool:
    return bool(
        op.get_bind().scalar(
            sa.text("SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')")
        )
    )


def upgrade() -> None:
    if not _has_vector():
        return
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute(
        """
        CREATE TABLE chunk_embeddings (
            id uuid PRIMARY KEY DEFAULT uuidv7(),
            group_id varchar(256) NOT NULL,
            chunk_id uuid NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
            provider varchar(128) NOT NULL,
            model varchar(256) NOT NULL,
            model_version varchar(128) NOT NULL,
            dimension integer NOT NULL CHECK (dimension > 0),
            embedding vector NOT NULL,
            content_hash varchar(128) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            active boolean NOT NULL DEFAULT true,
            CONSTRAINT uq_chunk_embedding_model
                UNIQUE (chunk_id, provider, model, model_version)
        )
        """
    )
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY ix_chunk_embeddings_group_active "
            "ON chunk_embeddings(group_id, active)"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY ix_chunk_embeddings_ann_1024 ON chunk_embeddings "
            "USING hnsw ((embedding::vector(1024)) vector_cosine_ops) "
            "WHERE active AND dimension = 1024"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY ix_chunk_embeddings_ann_1536 ON chunk_embeddings "
            "USING hnsw ((embedding::vector(1536)) vector_cosine_ops) "
            "WHERE active AND dimension = 1536"
        )
    has_legacy_column = bool(
        op.get_bind().scalar(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_name = 'chunks' AND column_name = 'embedding')"
            )
        )
    )
    if has_legacy_column:
        op.execute(
            """
            INSERT INTO chunk_embeddings (
                group_id, chunk_id, provider, model, model_version, dimension,
                embedding, content_hash, active
            )
            SELECT group_id, id, 'legacy', 'legacy-1024', '1', 1024,
                   embedding, content_hash, true
            FROM chunks WHERE embedding IS NOT NULL
            ON CONFLICT DO NOTHING
            """
        )
        op.execute("DROP INDEX IF EXISTS ix_chunks_embedding")
        op.execute("ALTER TABLE chunks DROP COLUMN embedding")
    op.execute("ALTER TABLE chunk_embeddings ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE chunk_embeddings FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON chunk_embeddings "
        "USING (group_id = current_setting('vera.group_id', true)) "
        "WITH CHECK (group_id = current_setting('vera.group_id', true))"
    )
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON chunk_embeddings TO vera_app")
    op.execute("GRANT SELECT ON chunk_embeddings TO vera_trusted")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON chunk_embeddings TO vera_worker")


def downgrade() -> None:
    exists = bool(
        op.get_bind().scalar(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_name = 'chunk_embeddings')"
            )
        )
    )
    if not exists:
        return
    op.execute("ALTER TABLE chunks ADD COLUMN IF NOT EXISTS embedding vector(1024)")
    op.execute(
        """
        UPDATE chunks c SET embedding = selected.embedding::vector(1024)
        FROM (
            SELECT DISTINCT ON (chunk_id) chunk_id, embedding
            FROM chunk_embeddings
            WHERE active AND dimension = 1024
            ORDER BY chunk_id, created_at DESC
        ) selected
        WHERE c.id = selected.chunk_id
        """
    )
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_chunks_embedding "
            "ON chunks USING hnsw (embedding vector_cosine_ops)"
        )
    op.execute("DROP TABLE chunk_embeddings")
