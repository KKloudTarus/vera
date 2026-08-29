"""freeze snapshot retrieval inputs

Revision ID: 1c3d4e5f6a7b
Revises: 0b2c3d4e5f6a
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "1c3d4e5f6a7b"
down_revision: str | None = "0b2c3d4e5f6a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ROLES = ("vera_app", "vera_trusted", "vera_worker")
_ERASURE_ROLE = "vera_erasure"
_SNAPSHOT_CHILDREN = (
    "snapshot_facts",
    "snapshot_sources",
    "snapshot_fact_sources",
    "snapshot_fact_citations",
    "snapshot_chunks",
    "snapshot_chunk_embeddings",
)


def _create_tenant_keys() -> None:
    # Existing snapshot tables may be large. Build the backing indexes without blocking writes.
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_knowledge_snapshots_tenant_key")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_snapshot_facts_tenant_key")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_facts_tenant_snapshot_key")
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY ix_knowledge_snapshots_tenant_key "
            "ON knowledge_snapshots(id, group_id)"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY ix_snapshot_facts_tenant_key "
            "ON snapshot_facts(snapshot_id, fact_id, group_id)"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY ix_facts_tenant_snapshot_key ON facts(id, group_id)"
        )
    op.execute(
        "ALTER TABLE knowledge_snapshots ADD CONSTRAINT uq_knowledge_snapshots_tenant "
        "UNIQUE USING INDEX ix_knowledge_snapshots_tenant_key"
    )
    op.execute(
        "ALTER TABLE snapshot_facts ADD CONSTRAINT uq_snapshot_facts_tenant "
        "UNIQUE USING INDEX ix_snapshot_facts_tenant_key"
    )
    op.execute(
        "ALTER TABLE facts ADD CONSTRAINT uq_facts_tenant_snapshot "
        "UNIQUE USING INDEX ix_facts_tenant_snapshot_key"
    )


def _replace_community_lineage_foreign_key() -> None:
    op.drop_constraint(
        "community_fact_lineage_fact_id_fkey", "community_fact_lineage", type_="foreignkey"
    )
    op.execute(
        "ALTER TABLE community_fact_lineage ADD CONSTRAINT fk_community_lineage_fact "
        "FOREIGN KEY (fact_id, group_id) REFERENCES facts(id, group_id) "
        "ON DELETE CASCADE NOT VALID"
    )
    op.execute("ALTER TABLE community_fact_lineage VALIDATE CONSTRAINT fk_community_lineage_fact")
    op.execute(
        "REVOKE INSERT, UPDATE, DELETE ON community_fact_lineage FROM vera_app, vera_trusted"
    )
    op.execute("REVOKE UPDATE, DELETE ON community_fact_lineage FROM vera_worker")
    op.execute("GRANT SELECT ON community_fact_lineage TO vera_app, vera_trusted, vera_worker")
    op.execute("GRANT INSERT ON community_fact_lineage TO vera_worker")


def _replace_snapshot_fact_foreign_keys() -> None:
    op.drop_constraint("snapshot_facts_snapshot_id_fkey", "snapshot_facts", type_="foreignkey")
    op.drop_constraint("snapshot_facts_fact_id_fkey", "snapshot_facts", type_="foreignkey")
    op.execute(
        "ALTER TABLE snapshot_facts ADD CONSTRAINT fk_snapshot_facts_snapshot "
        "FOREIGN KEY (snapshot_id, group_id) "
        "REFERENCES knowledge_snapshots(id, group_id) ON DELETE CASCADE NOT VALID"
    )
    op.execute(
        "ALTER TABLE snapshot_facts ADD CONSTRAINT snapshot_facts_fact_id_fkey "
        "FOREIGN KEY (fact_id, group_id) REFERENCES facts(id, group_id) "
        "ON DELETE RESTRICT NOT VALID"
    )
    op.execute("ALTER TABLE snapshot_facts VALIDATE CONSTRAINT fk_snapshot_facts_snapshot")
    op.execute("ALTER TABLE snapshot_facts VALIDATE CONSTRAINT snapshot_facts_fact_id_fkey")


def _replace_context_pack_foreign_key() -> None:
    op.drop_constraint("context_packs_snapshot_id_fkey", "context_packs", type_="foreignkey")
    op.execute(
        "ALTER TABLE context_packs ADD CONSTRAINT fk_context_packs_snapshot "
        "FOREIGN KEY (snapshot_id, group_id) "
        "REFERENCES knowledge_snapshots(id, group_id) ON DELETE RESTRICT NOT VALID"
    )
    op.execute("ALTER TABLE context_packs VALIDATE CONSTRAINT fk_context_packs_snapshot")


def _create_snapshot_tables() -> None:
    op.create_table(
        "snapshot_sources",
        sa.Column("snapshot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("knowledge_source_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("repository", sa.Text, nullable=True),
        sa.Column("branch", sa.Text, nullable=True),
        sa.Column("document_type", sa.Text, nullable=True),
        sa.Column("source_type", sa.String(32), nullable=False),
        sa.Column("trust_tier", sa.Integer, nullable=False),
        sa.PrimaryKeyConstraint("snapshot_id", "knowledge_source_id", name="pk_snapshot_sources"),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "group_id"],
            ["knowledge_snapshots.id", "knowledge_snapshots.group_id"],
            ondelete="CASCADE",
            name="fk_snapshot_sources_snapshot",
        ),
    )
    op.create_table(
        "snapshot_fact_sources",
        sa.Column("snapshot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("fact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("assertion_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("knowledge_source_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("artifact_version_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("assertion_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("repository", sa.Text, nullable=True),
        sa.Column("branch", sa.Text, nullable=True),
        sa.Column("document_type", sa.Text, nullable=True),
        sa.Column("source_type", sa.String(32), nullable=True),
        sa.Column("trust_tier", sa.Integer, nullable=True),
        sa.PrimaryKeyConstraint("snapshot_id", "assertion_id", name="pk_snapshot_fact_sources"),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "fact_id", "group_id"],
            ["snapshot_facts.snapshot_id", "snapshot_facts.fact_id", "snapshot_facts.group_id"],
            ondelete="CASCADE",
            name="fk_snapshot_fact_sources_fact",
        ),
        sa.UniqueConstraint(
            "snapshot_id",
            "fact_id",
            "assertion_id",
            "group_id",
            name="uq_snapshot_fact_sources_tenant",
        ),
    )
    op.create_index(
        "ix_snapshot_fact_sources_fact",
        "snapshot_fact_sources",
        ["snapshot_id", "fact_id"],
    )
    op.create_table(
        "snapshot_fact_citations",
        sa.Column("snapshot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("evidence_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("fact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("assertion_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("knowledge_source_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("excerpt", sa.Text, nullable=True),
        sa.Column("chunk_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("artifact_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("heading_path", sa.Text, nullable=True),
        sa.Column("quote_start", sa.Integer, nullable=True),
        sa.Column("quote_end", sa.Integer, nullable=True),
        sa.Column("quote_hash", sa.String(128), nullable=True),
        sa.Column("content_hash", sa.String(128), nullable=False),
        sa.Column("extraction_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("structured_record", postgresql.JSONB, nullable=True),
        sa.Column("citation_uri", sa.Text, nullable=True),
        sa.Column(
            "source_coordinates",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("assertion_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(chunk_id IS NOT NULL AND quote_start IS NOT NULL AND quote_end IS NOT NULL "
            "AND quote_hash IS NOT NULL) OR structured_record IS NOT NULL "
            "OR citation_uri IS NOT NULL",
            name="ck_snapshot_fact_citation_variant",
        ),
        sa.PrimaryKeyConstraint("snapshot_id", "evidence_id", name="pk_snapshot_fact_citations"),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "fact_id", "assertion_id", "group_id"],
            [
                "snapshot_fact_sources.snapshot_id",
                "snapshot_fact_sources.fact_id",
                "snapshot_fact_sources.assertion_id",
                "snapshot_fact_sources.group_id",
            ],
            ondelete="CASCADE",
            name="fk_snapshot_fact_citations_source",
        ),
    )
    op.create_index(
        "ix_snapshot_fact_citations_fact",
        "snapshot_fact_citations",
        ["snapshot_id", "fact_id"],
    )
    op.create_table(
        "snapshot_chunks",
        sa.Column("snapshot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("knowledge_source_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("artifact_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("text", sa.Text, nullable=False),
        sa.Column("content_hash", sa.String(128), nullable=False),
        sa.Column("heading_path", sa.Text, nullable=True),
        sa.Column("symbol_name", sa.String(512), nullable=True),
        sa.Column("start_offset", sa.Integer, nullable=True),
        sa.Column("end_offset", sa.Integer, nullable=True),
        sa.Column("page_number", sa.Integer, nullable=True),
        sa.Column("start_line", sa.Integer, nullable=True),
        sa.Column("end_line", sa.Integer, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("search_vector", postgresql.TSVECTOR, nullable=False),
        sa.PrimaryKeyConstraint("snapshot_id", "chunk_id", name="pk_snapshot_chunks"),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "group_id"],
            ["knowledge_snapshots.id", "knowledge_snapshots.group_id"],
            ondelete="CASCADE",
            name="fk_snapshot_chunks_snapshot",
        ),
        sa.UniqueConstraint(
            "snapshot_id", "chunk_id", "group_id", name="uq_snapshot_chunks_tenant"
        ),
    )
    op.create_index(
        "ix_snapshot_chunks_search",
        "snapshot_chunks",
        ["search_vector"],
        postgresql_using="gin",
    )
    op.create_table(
        "snapshot_chunk_embeddings",
        sa.Column("snapshot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("provider", sa.String(128), nullable=False),
        sa.Column("model", sa.String(256), nullable=False),
        sa.Column("model_version", sa.String(128), nullable=False),
        sa.Column("dimension", sa.Integer, nullable=False),
        # Text keeps the migration portable when the pgvector extension is unavailable.
        sa.Column("embedding", sa.Text, nullable=False),
        sa.Column("content_hash", sa.String(128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("snapshot_id", "chunk_id", name="pk_snapshot_chunk_embeddings"),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "chunk_id", "group_id"],
            ["snapshot_chunks.snapshot_id", "snapshot_chunks.chunk_id", "snapshot_chunks.group_id"],
            ondelete="CASCADE",
            name="fk_snapshot_chunk_embeddings_chunk",
        ),
    )


def _configure_snapshot_security() -> None:
    for table in _SNAPSHOT_CHILDREN[1:]:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY tenant_isolation ON {table} "
            "USING (group_id = current_setting('vera.group_id', true)) "
            "WITH CHECK (group_id = current_setting('vera.group_id', true))"
        )
        op.execute(f"REVOKE ALL ON {table} FROM {', '.join(_ROLES)}")
        op.execute(f"GRANT SELECT, INSERT ON {table} TO vera_app")
        op.execute(f"GRANT SELECT ON {table} TO vera_trusted, vera_worker")

    op.execute("REVOKE INSERT, UPDATE, DELETE ON snapshot_facts FROM vera_trusted, vera_worker")
    op.execute("REVOKE UPDATE, DELETE ON snapshot_facts FROM vera_app")
    op.execute(
        "REVOKE INSERT, UPDATE, DELETE ON knowledge_snapshots FROM vera_trusted, vera_worker"
    )
    op.execute("REVOKE DELETE ON knowledge_snapshots FROM vera_app")
    op.execute("REVOKE UPDATE, DELETE ON chunks FROM vera_app, vera_trusted, vera_worker")

    op.execute(
        """
        CREATE FUNCTION reject_chunk_mutation() RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'DELETE'
               AND current_setting('vera.erasure_mode', true) = 'on' THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION 'chunks are immutable';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        "CREATE TRIGGER chunks_immutable BEFORE UPDATE OR DELETE ON chunks "
        "FOR EACH ROW EXECUTE FUNCTION reject_chunk_mutation()"
    )

    op.execute(
        """
        CREATE FUNCTION guard_knowledge_snapshot_mutation() RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'DELETE'
               AND current_setting('vera.erasure_mode', true) = 'on' THEN
                RETURN OLD;
            END IF;
            IF TG_OP = 'DELETE' OR OLD.retrieval_frozen THEN
                RAISE EXCEPTION 'knowledge snapshots are immutable after creation';
            END IF;
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.group_id IS DISTINCT FROM OLD.group_id
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR NEW.frozen_at_system_time IS DISTINCT FROM OLD.frozen_at_system_time
               OR NEW.as_of_valid_time IS DISTINCT FROM OLD.as_of_valid_time
               OR NEW.ontology_version_id IS DISTINCT FROM OLD.ontology_version_id
               OR NEW.policy_version IS DISTINCT FROM OLD.policy_version
               OR NEW.embedding_version IS DISTINCT FROM OLD.embedding_version
               OR NEW.retrieval_index_version IS DISTINCT FROM OLD.retrieval_index_version
               OR NEW.assembler_version IS DISTINCT FROM OLD.assembler_version
               OR NEW.graph_projection_checkpoint IS DISTINCT FROM OLD.graph_projection_checkpoint THEN
                RAISE EXCEPTION 'knowledge snapshot metadata is immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        "CREATE TRIGGER knowledge_snapshots_immutable BEFORE UPDATE OR DELETE "
        "ON knowledge_snapshots FOR EACH ROW EXECUTE FUNCTION guard_knowledge_snapshot_mutation()"
    )
    op.execute(
        """
        CREATE FUNCTION guard_snapshot_child_mutation() RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'DELETE'
               AND current_setting('vera.erasure_mode', true) = 'on' THEN
                RETURN OLD;
            END IF;
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION 'snapshot retrieval inputs are immutable';
            END IF;
            PERFORM 1 FROM knowledge_snapshots
             WHERE id = NEW.snapshot_id AND group_id = NEW.group_id
               AND retrieval_frozen = false
             FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'snapshot retrieval inputs are immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    for table in _SNAPSHOT_CHILDREN:
        op.execute(
            f"CREATE TRIGGER {table}_immutable BEFORE INSERT OR UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION guard_snapshot_child_mutation()"
        )


def _configure_erasure() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{_ERASURE_ROLE}') THEN
                CREATE ROLE {_ERASURE_ROLE} NOLOGIN NOSUPERUSER BYPASSRLS;
            END IF;
        END
        $$;
        """
    )
    op.execute(f"GRANT USAGE ON SCHEMA public TO {_ERASURE_ROLE}")
    op.execute(
        f"GRANT SELECT, DELETE ON knowledge_snapshots, context_packs, snapshot_facts, "
        f"snapshot_sources, snapshot_fact_sources, snapshot_fact_citations, snapshot_chunks, "
        f"snapshot_chunk_embeddings, evidence, chunks TO {_ERASURE_ROLE}"
    )
    op.execute("REVOKE UPDATE, DELETE ON context_packs FROM vera_app, vera_trusted, vera_worker")
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reject_context_pack_mutation() RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'DELETE'
               AND current_setting('vera.erasure_mode', true) = 'on' THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION 'context packs are immutable';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE FUNCTION erase_artifact_retrieval_inputs(
            p_group_id text,
            p_artifact_version_ids uuid[]
        ) RETURNS uuid[] AS $$
        DECLARE
            removed_snapshot_ids uuid[];
        BEGIN
            IF p_group_id IS NULL
               OR p_group_id IS DISTINCT FROM current_setting('vera.group_id', true) THEN
                RAISE EXCEPTION 'erasure tenant mismatch' USING ERRCODE = '42501';
            END IF;
            PERFORM set_config('vera.erasure_mode', 'on', true);

            SELECT COALESCE(array_agg(DISTINCT doomed.snapshot_id), ARRAY[]::uuid[])
              INTO removed_snapshot_ids
              FROM (
                  SELECT sc.snapshot_id
                    FROM snapshot_chunks sc
                   WHERE sc.group_id = p_group_id
                     AND sc.artifact_version_id = ANY(p_artifact_version_ids)
                  UNION
                  SELECT sfc.snapshot_id
                    FROM snapshot_fact_citations sfc
                   WHERE sfc.group_id = p_group_id
                      AND sfc.artifact_version_id = ANY(p_artifact_version_ids)
                  UNION
                  SELECT sfs.snapshot_id
                    FROM snapshot_fact_sources sfs
                   WHERE sfs.group_id = p_group_id
                     AND sfs.artifact_version_id = ANY(p_artifact_version_ids)
              ) doomed;

            DELETE FROM context_packs cp
             WHERE cp.group_id = p_group_id
               AND (
                   cp.snapshot_id = ANY(removed_snapshot_ids)
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(cp.results) result
                        WHERE result->'citation'->>'artifact_version_id' IN (
                                  SELECT version_id::text
                                    FROM unnest(p_artifact_version_ids) version_id
                              )
                           OR EXISTS (
                               SELECT 1
                                 FROM jsonb_array_elements(
                                     COALESCE(result->'citations', '[]'::jsonb)
                                 ) citation
                                WHERE citation->>'artifact_version_id' IN (
                                    SELECT version_id::text
                                      FROM unnest(p_artifact_version_ids) version_id
                                )
                           )
                   )
               );
            DELETE FROM knowledge_snapshots snapshot
             WHERE snapshot.group_id = p_group_id
               AND snapshot.id = ANY(removed_snapshot_ids);
            DELETE FROM evidence e
             WHERE e.group_id = p_group_id
               AND e.artifact_version_id = ANY(p_artifact_version_ids);
            DELETE FROM chunks chunk
             WHERE chunk.group_id = p_group_id
               AND chunk.artifact_version_id = ANY(p_artifact_version_ids);
            RETURN removed_snapshot_ids;
        END;
        $$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp
        """
    )
    op.execute(
        f"ALTER FUNCTION erase_artifact_retrieval_inputs(text, uuid[]) OWNER TO {_ERASURE_ROLE}"
    )
    op.execute("REVOKE ALL ON FUNCTION erase_artifact_retrieval_inputs(text, uuid[]) FROM PUBLIC")
    op.execute(
        "GRANT EXECUTE ON FUNCTION erase_artifact_retrieval_inputs(text, uuid[]) TO vera_app"
    )


def upgrade() -> None:
    _create_tenant_keys()
    _replace_community_lineage_foreign_key()
    _replace_snapshot_fact_foreign_keys()
    _replace_context_pack_foreign_key()
    _create_snapshot_tables()
    _configure_snapshot_security()
    _configure_erasure()


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS erase_artifact_retrieval_inputs(text, uuid[])")
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reject_context_pack_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'context packs are immutable';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute("DROP TRIGGER IF EXISTS chunks_immutable ON chunks")
    op.execute("DROP TRIGGER IF EXISTS knowledge_snapshots_immutable ON knowledge_snapshots")
    for table in _SNAPSHOT_CHILDREN:
        op.execute(f"DROP TRIGGER IF EXISTS {table}_immutable ON {table}")
    op.execute("DROP FUNCTION IF EXISTS guard_snapshot_child_mutation()")
    op.execute("DROP FUNCTION IF EXISTS guard_knowledge_snapshot_mutation()")
    op.execute("DROP FUNCTION IF EXISTS reject_chunk_mutation()")

    op.drop_table("snapshot_chunk_embeddings")
    op.drop_table("snapshot_chunks")
    op.drop_table("snapshot_fact_citations")
    op.drop_table("snapshot_fact_sources")
    op.drop_table("snapshot_sources")

    op.drop_constraint("fk_snapshot_facts_snapshot", "snapshot_facts", type_="foreignkey")
    op.drop_constraint("snapshot_facts_fact_id_fkey", "snapshot_facts", type_="foreignkey")
    op.drop_constraint("fk_context_packs_snapshot", "context_packs", type_="foreignkey")
    op.drop_constraint("fk_community_lineage_fact", "community_fact_lineage", type_="foreignkey")
    op.execute(
        "ALTER TABLE community_fact_lineage ADD CONSTRAINT "
        "community_fact_lineage_fact_id_fkey FOREIGN KEY (fact_id) "
        "REFERENCES facts(id) ON DELETE CASCADE NOT VALID"
    )
    op.execute(
        "ALTER TABLE community_fact_lineage VALIDATE CONSTRAINT community_fact_lineage_fact_id_fkey"
    )
    op.execute(
        "ALTER TABLE context_packs ADD CONSTRAINT context_packs_snapshot_id_fkey "
        "FOREIGN KEY (snapshot_id) REFERENCES knowledge_snapshots(id) "
        "ON DELETE SET NULL NOT VALID"
    )
    op.execute(
        "ALTER TABLE snapshot_facts ADD CONSTRAINT snapshot_facts_snapshot_id_fkey "
        "FOREIGN KEY (snapshot_id) REFERENCES knowledge_snapshots(id) "
        "ON DELETE CASCADE NOT VALID"
    )
    op.execute(
        "ALTER TABLE snapshot_facts ADD CONSTRAINT snapshot_facts_fact_id_fkey "
        "FOREIGN KEY (fact_id) REFERENCES facts(id) ON DELETE CASCADE NOT VALID"
    )
    op.execute("ALTER TABLE context_packs VALIDATE CONSTRAINT context_packs_snapshot_id_fkey")
    op.execute("ALTER TABLE snapshot_facts VALIDATE CONSTRAINT snapshot_facts_snapshot_id_fkey")
    op.execute("ALTER TABLE snapshot_facts VALIDATE CONSTRAINT snapshot_facts_fact_id_fkey")
    op.drop_constraint("uq_snapshot_facts_tenant", "snapshot_facts", type_="unique")
    op.drop_constraint("uq_knowledge_snapshots_tenant", "knowledge_snapshots", type_="unique")
    op.drop_constraint("uq_facts_tenant_snapshot", "facts", type_="unique")

    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON knowledge_snapshots, snapshot_facts "
        "TO vera_app, vera_worker"
    )
    op.execute("GRANT UPDATE, DELETE ON chunks TO vera_app, vera_worker")
    op.execute("GRANT SELECT ON knowledge_snapshots, snapshot_facts TO vera_trusted")
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON community_fact_lineage TO vera_app, vera_worker"
    )
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON context_packs TO vera_app, vera_worker")
    op.execute("GRANT SELECT ON community_fact_lineage TO vera_trusted")
    op.execute(f"REVOKE USAGE ON SCHEMA public FROM {_ERASURE_ROLE}")
    op.execute(f"DROP OWNED BY {_ERASURE_ROLE}")
    op.execute(f"DROP ROLE IF EXISTS {_ERASURE_ROLE}")
