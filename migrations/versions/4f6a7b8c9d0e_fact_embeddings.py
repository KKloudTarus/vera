"""authoritative fact embeddings

Revision ID: 4f6a7b8c9d0e
Revises: 3e5f6a7b8c9d
Create Date: 2026-08-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "4f6a7b8c9d0e"
down_revision: str | None = "3e5f6a7b8c9d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_vector() -> bool:
    return bool(
        op.get_bind().scalar(
            sa.text("SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')")
        )
    )


def _table_exists(table: str) -> bool:
    return bool(
        op.get_bind().scalar(
            sa.text("SELECT to_regclass(:table) IS NOT NULL"), {"table": f"public.{table}"}
        )
    )


def _constraint_exists(table: str, constraint: str) -> bool:
    return bool(
        op.get_bind().scalar(
            sa.text(
                "SELECT EXISTS ("
                "SELECT 1 FROM pg_catalog.pg_constraint "
                "WHERE conrelid = to_regclass(:table) AND conname = :constraint)"
            ),
            {"table": f"public.{table}", "constraint": constraint},
        )
    )


def _constraint_has_column(table: str, constraint: str, column: str) -> bool:
    return bool(
        op.get_bind().scalar(
            sa.text(
                "SELECT EXISTS ("
                "SELECT 1 FROM pg_catalog.pg_constraint constraint_def "
                "JOIN pg_catalog.pg_attribute column_def "
                "ON column_def.attrelid = constraint_def.conrelid "
                "AND column_def.attnum = ANY(constraint_def.conkey) "
                "WHERE constraint_def.conrelid = to_regclass(:table) "
                "AND constraint_def.conname = :constraint AND column_def.attname = :column)"
            ),
            {"table": f"public.{table}", "constraint": constraint, "column": column},
        )
    )


def _ensure_concurrent_index(name: str, ddl: str) -> None:
    # IF NOT EXISTS would preserve the invalid catalog entry left by a failed concurrent build.
    valid = op.get_bind().scalar(
        sa.text(
            "SELECT index_def.indisvalid AND index_def.indisready "
            "FROM pg_catalog.pg_index index_def "
            "JOIN pg_catalog.pg_class index ON index.oid = index_def.indexrelid "
            "JOIN pg_catalog.pg_namespace namespace ON namespace.oid = index.relnamespace "
            "WHERE namespace.nspname = 'public' AND index.relname = :name"
        ),
        {"name": name},
    )
    if valid is not None and bool(valid):
        return
    if valid is not None:
        op.execute(f"DROP INDEX CONCURRENTLY public.{name}")
    op.execute(ddl)


def upgrade() -> None:
    op.execute(
        "ALTER TABLE snapshot_facts ADD COLUMN IF NOT EXISTS "
        "object_type varchar(16) NOT NULL DEFAULT 'scalar'"
    )
    op.execute(
        "ALTER TABLE snapshot_facts ADD COLUMN IF NOT EXISTS "
        "qualifiers jsonb NOT NULL DEFAULT '{}'::jsonb"
    )
    if not _has_vector():
        return
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    if not _constraint_exists("chunk_embeddings", "ck_chunk_embedding_dimension"):
        op.execute(
            "ALTER TABLE chunk_embeddings ADD CONSTRAINT ck_chunk_embedding_dimension "
            "CHECK (vector_dims(embedding) = dimension) NOT VALID"
        )
    op.execute("ALTER TABLE chunk_embeddings VALIDATE CONSTRAINT ck_chunk_embedding_dimension")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_embeddings (
            id uuid PRIMARY KEY DEFAULT uuidv7(),
            group_id varchar(256) NOT NULL,
            fact_id uuid NOT NULL,
            provider varchar(128) NOT NULL,
            model varchar(256) NOT NULL,
            model_version varchar(128) NOT NULL,
            dimension integer NOT NULL CHECK (dimension > 0),
            embedding vector NOT NULL,
            content_hash varchar(128) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            active boolean NOT NULL DEFAULT true,
            CONSTRAINT fk_fact_embeddings_fact
                FOREIGN KEY (fact_id, group_id) REFERENCES facts(id, group_id) ON DELETE CASCADE,
            CONSTRAINT ck_fact_embedding_dimension
                CHECK (vector_dims(embedding) = dimension),
            CONSTRAINT uq_fact_embedding_model
                UNIQUE (fact_id, provider, model, model_version, dimension)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshot_fact_embeddings (
            snapshot_id uuid NOT NULL,
            fact_id uuid NOT NULL,
            group_id varchar(256) NOT NULL,
            provider varchar(128) NOT NULL,
            model varchar(256) NOT NULL,
            model_version varchar(128) NOT NULL,
            dimension integer NOT NULL CHECK (dimension > 0),
            embedding vector NOT NULL,
            content_hash varchar(128) NOT NULL,
            created_at timestamptz NOT NULL,
            CONSTRAINT pk_snapshot_fact_embeddings PRIMARY KEY
                (snapshot_id, fact_id, provider, model, model_version, dimension),
            CONSTRAINT fk_snapshot_fact_embeddings_fact
                FOREIGN KEY (snapshot_id, fact_id, group_id)
                REFERENCES snapshot_facts(snapshot_id, fact_id, group_id) ON DELETE CASCADE,
            CONSTRAINT ck_snapshot_fact_embedding_dimension
                CHECK (vector_dims(embedding) = dimension)
        )
        """
    )
    with op.get_context().autocommit_block():
        if not _constraint_has_column("chunk_embeddings", "uq_chunk_embedding_model", "dimension"):
            _ensure_concurrent_index(
                "ix_chunk_embedding_model_dimension",
                "CREATE UNIQUE INDEX CONCURRENTLY ix_chunk_embedding_model_dimension "
                "ON chunk_embeddings(chunk_id, provider, model, model_version, dimension)",
            )
        _ensure_concurrent_index(
            "ix_fact_embeddings_group_active",
            "CREATE INDEX CONCURRENTLY ix_fact_embeddings_group_active "
            "ON fact_embeddings(group_id, active)",
        )
        _ensure_concurrent_index(
            "ix_fact_embeddings_ann_256",
            "CREATE INDEX CONCURRENTLY ix_fact_embeddings_ann_256 ON fact_embeddings "
            "USING hnsw ((embedding::vector(256)) vector_cosine_ops) "
            "WHERE active AND dimension = 256",
        )
        _ensure_concurrent_index(
            "ix_fact_embeddings_ann_512",
            "CREATE INDEX CONCURRENTLY ix_fact_embeddings_ann_512 ON fact_embeddings "
            "USING hnsw ((embedding::vector(512)) vector_cosine_ops) "
            "WHERE active AND dimension = 512",
        )
        _ensure_concurrent_index(
            "ix_fact_embeddings_ann_1024",
            "CREATE INDEX CONCURRENTLY ix_fact_embeddings_ann_1024 ON fact_embeddings "
            "USING hnsw ((embedding::vector(1024)) vector_cosine_ops) "
            "WHERE active AND dimension = 1024",
        )
        _ensure_concurrent_index(
            "ix_fact_embeddings_ann_1536",
            "CREATE INDEX CONCURRENTLY ix_fact_embeddings_ann_1536 ON fact_embeddings "
            "USING hnsw ((embedding::vector(1536)) vector_cosine_ops) "
            "WHERE active AND dimension = 1536",
        )
        for dimension in (256, 512, 1024, 1536):
            name = f"ix_snapshot_fact_embeddings_ann_{dimension}"
            _ensure_concurrent_index(
                name,
                f"CREATE INDEX CONCURRENTLY ix_snapshot_fact_embeddings_ann_{dimension} "
                "ON snapshot_fact_embeddings "
                f"USING hnsw ((embedding::vector({dimension})) vector_cosine_ops) "
                f"WHERE dimension = {dimension}",
            )
        for dimension in (256, 512):
            name = f"ix_chunk_embeddings_ann_{dimension}"
            _ensure_concurrent_index(
                name,
                f"CREATE INDEX CONCURRENTLY ix_chunk_embeddings_ann_{dimension} "
                "ON chunk_embeddings "
                f"USING hnsw ((embedding::vector({dimension})) vector_cosine_ops) "
                f"WHERE active AND dimension = {dimension}",
            )
    if not _constraint_has_column("chunk_embeddings", "uq_chunk_embedding_model", "dimension"):
        if _constraint_exists("chunk_embeddings", "uq_chunk_embedding_model"):
            op.execute("ALTER TABLE chunk_embeddings DROP CONSTRAINT uq_chunk_embedding_model")
        op.execute(
            "ALTER TABLE chunk_embeddings ADD CONSTRAINT uq_chunk_embedding_model "
            "UNIQUE USING INDEX ix_chunk_embedding_model_dimension"
        )
    op.execute("ALTER TABLE fact_embeddings ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE fact_embeddings FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON fact_embeddings "
        "USING (group_id = current_setting('vera.group_id', true)) "
        "WITH CHECK (group_id = current_setting('vera.group_id', true))"
    )
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON fact_embeddings TO vera_app")
    op.execute("GRANT SELECT ON fact_embeddings TO vera_trusted")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON fact_embeddings TO vera_worker")
    op.execute("ALTER TABLE snapshot_fact_embeddings ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE snapshot_fact_embeddings FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON snapshot_fact_embeddings "
        "USING (group_id = current_setting('vera.group_id', true)) "
        "WITH CHECK (group_id = current_setting('vera.group_id', true))"
    )
    op.execute("GRANT SELECT, INSERT ON snapshot_fact_embeddings TO vera_app")
    op.execute("GRANT SELECT ON snapshot_fact_embeddings TO vera_trusted, vera_worker")
    op.execute(
        "CREATE TRIGGER snapshot_fact_embeddings_immutable "
        "BEFORE INSERT OR UPDATE OR DELETE ON snapshot_fact_embeddings "
        "FOR EACH ROW EXECUTE FUNCTION guard_snapshot_child_mutation()"
    )
    op.execute("GRANT SELECT, DELETE ON snapshot_fact_embeddings TO vera_erasure")


def downgrade() -> None:
    if _table_exists("fact_embeddings"):
        op.execute(
            "DELETE FROM chunk_embeddings newer USING chunk_embeddings older "
            "WHERE newer.chunk_id = older.chunk_id AND newer.provider = older.provider "
            "AND newer.model = older.model AND newer.model_version = older.model_version "
            "AND (newer.created_at, newer.id) < (older.created_at, older.id)"
        )
        with op.get_context().autocommit_block():
            op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_chunk_embeddings_ann_256")
            op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_chunk_embeddings_ann_512")
            _ensure_concurrent_index(
                "ix_chunk_embedding_model_rollback",
                "CREATE UNIQUE INDEX CONCURRENTLY ix_chunk_embedding_model_rollback "
                "ON chunk_embeddings(chunk_id, provider, model, model_version)",
            )
        op.execute("DROP TABLE IF EXISTS snapshot_fact_embeddings")
        op.execute("DROP TABLE fact_embeddings")
        op.execute("ALTER TABLE chunk_embeddings DROP CONSTRAINT ck_chunk_embedding_dimension")
        op.execute("ALTER TABLE chunk_embeddings DROP CONSTRAINT uq_chunk_embedding_model")
        op.execute(
            "ALTER TABLE chunk_embeddings ADD CONSTRAINT uq_chunk_embedding_model "
            "UNIQUE USING INDEX ix_chunk_embedding_model_rollback"
        )
    op.execute("ALTER TABLE snapshot_facts DROP COLUMN IF EXISTS qualifiers")
    op.execute("ALTER TABLE snapshot_facts DROP COLUMN IF EXISTS object_type")
