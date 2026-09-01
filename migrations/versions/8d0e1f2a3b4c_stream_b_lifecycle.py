"""stream B context retention and feedback attribution

Revision ID: 8d0e1f2a3b4c
Revises: 7c9d0e1f2a3b
Create Date: 2026-08-31
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "8d0e1f2a3b4c"
down_revision: str | None = "7c9d0e1f2a3b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE FUNCTION canonical_repository_ref(p_value text) RETURNS text AS $$
        DECLARE
            raw text := regexp_replace(p_value, '^[[:space:]]+|[[:space:]]+$', '', 'g');
            location text;
            authority text;
            repository_path text;
            scheme_name text;
            scp_parts text[];
            host_name text;
            port_text text;
            port_number integer;
        BEGIN
            raw := split_part(split_part(raw, '?', 1), '#', 1);
            raw := regexp_replace(raw, '^[[:space:]]+|[[:space:]]+$', '', 'g');
            IF raw = '' OR raw ~* '^(file:|/|\./|\.\./|~|[a-z]:)' OR position('\' IN raw) > 0
               OR (position('://' IN raw) = 0 AND position(':' IN raw) > 0
                   AND position(':' IN raw) < position('@' IN raw)) THEN
                RETURN NULL;
            END IF;
            IF position('://' IN raw) > 0 THEN
                scheme_name := lower(split_part(raw, '://', 1));
                IF scheme_name NOT IN ('git', 'http', 'https', 'ssh') THEN
                    RETURN NULL;
                END IF;
                location := substring(raw FROM position('://' IN raw) + 3);
                authority := split_part(location, '/', 1);
                authority := lower(regexp_replace(authority, '^.*@', ''));
                IF authority = ''
                   OR (authority LIKE '[%' AND authority !~ '^\[[0-9a-f:.]+\](?::[0-9]+)?$')
                   OR (authority NOT LIKE '[%' AND authority !~ '^[^:]+(?::[0-9]+)?$') THEN
                    RETURN NULL;
                END IF;
                repository_path := substring(location FROM length(split_part(location, '/', 1)) + 2);
            ELSIF raw ~* '^(?:\[[0-9a-f:.]+\]|[^@:/[:space:]]+):[0-9]+/.+$' THEN
                authority := lower(split_part(raw, '/', 1));
                repository_path := substring(raw FROM length(split_part(raw, '/', 1)) + 2);
            ELSIF raw ~ '^[^@:/[:space:]]+/.+$' THEN
                authority := lower(split_part(raw, '/', 1));
                repository_path := substring(raw FROM length(split_part(raw, '/', 1)) + 2);
            ELSE
                scp_parts := regexp_match(
                    raw,
                    '^(?:[^@/[:space:]]+@)?([^:/[:space:]]+):(.+)$'
                );
                IF scp_parts IS NOT NULL THEN
                    authority := lower(scp_parts[1]);
                    repository_path := scp_parts[2];
                ELSE
                    repository_path := raw;
                END IF;
            END IF;
            IF authority IS NOT NULL AND authority ~ ':[0-9]+$' THEN
                port_text := substring(authority FROM ':([0-9]+)$');
                IF port_text::numeric > 65535 THEN
                    RETURN NULL;
                END IF;
                port_number := port_text::integer;
                IF authority LIKE '[%' THEN
                    host_name := substring(authority FROM '^(\[[0-9a-f:.]+\])');
                ELSE
                    host_name := regexp_replace(authority, ':[0-9]+$', '');
                END IF;
                IF host_name IS NULL OR host_name = '' THEN
                    RETURN NULL;
                END IF;
                authority := host_name || ':' || port_number::text;
            END IF;
            repository_path := regexp_replace(repository_path, '/+', '/', 'g');
            repository_path := btrim(repository_path, '/');
            IF repository_path = '' OR repository_path ~ '(^|/)\.\.(/|$)' THEN
                RETURN NULL;
            END IF;
            WHILE repository_path ~ '(^|/)\.(/|$)' LOOP
                repository_path := regexp_replace(repository_path, '(^|/)\.(/|$)', '\1', 'g');
            END LOOP;
            repository_path := regexp_replace(repository_path, '\.git$', '', 'i');
            IF repository_path = '' THEN
                RETURN NULL;
            END IF;
            RETURN CASE
                WHEN authority IS NULL THEN repository_path
                ELSE authority || '/' || repository_path
            END;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
        """
    )
    op.alter_column(
        "knowledge_snapshots",
        "assembler_version",
        existing_type=sa.String(length=64),
        server_default=sa.text("'context-assembler-v3'"),
    )
    op.add_column("retrieval_feedback", sa.Column("rank", sa.Integer(), nullable=True))
    op.add_column(
        "retrieval_feedback",
        sa.Column("context_pack_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.execute(
        "ALTER TABLE retrieval_feedback ADD CONSTRAINT ck_feedback_rank_positive "
        "CHECK (rank IS NULL OR rank > 0) NOT VALID"
    )
    op.create_table(
        "proposal_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuidv7()")),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_key", sa.String(128), nullable=False),
        sa.Column("fact_key", sa.String(128), nullable=True),
        sa.Column("proposal_ref", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("outcome", sa.String(16), nullable=False),
        sa.Column("operation", sa.String(16), nullable=False),
        sa.Column(
            "context", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column(
            "detail", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "outcome IN ('created','deduplicated','conflicted','skipped','rejected')",
            name="ck_proposal_attempt_outcome",
        ),
        sa.CheckConstraint(
            "operation IN ('created','deduplicated','skipped','rejected')",
            name="ck_proposal_attempt_operation",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_proposal_attempts_report",
        "proposal_attempts",
        ["group_id", "run_key", "created_at"],
    )
    op.create_index(
        "ix_proposal_attempts_context",
        "proposal_attempts",
        ["context"],
        postgresql_using="gin",
    )
    op.execute("ALTER TABLE proposal_attempts ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE proposal_attempts FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON proposal_attempts "
        "USING (group_id = current_setting('vera.group_id', true)) "
        "WITH CHECK (group_id = current_setting('vera.group_id', true))"
    )
    op.execute("REVOKE ALL ON proposal_attempts FROM vera_app, vera_trusted, vera_worker")
    op.execute("GRANT SELECT, INSERT ON proposal_attempts TO vera_app")
    op.execute("GRANT SELECT ON proposal_attempts TO vera_trusted")
    op.execute(
        """
        CREATE FUNCTION delete_expired_context_packs(p_group_id text) RETURNS integer AS $$
        DECLARE
            removed integer;
        BEGIN
            IF p_group_id IS NULL
               OR p_group_id IS DISTINCT FROM current_setting('vera.group_id', true) THEN
                RAISE EXCEPTION 'context-pack retention tenant mismatch' USING ERRCODE = '42501';
            END IF;
            PERFORM set_config('vera.erasure_mode', 'on', true);
            WITH expired AS (
                SELECT id FROM context_packs
                 WHERE group_id = p_group_id AND expires_at <= now()
                 ORDER BY expires_at, id
                 LIMIT 1000
                 FOR UPDATE SKIP LOCKED
            )
            DELETE FROM context_packs packs
             USING expired
             WHERE packs.id = expired.id;
            GET DIAGNOSTICS removed = ROW_COUNT;
            RETURN removed;
        END;
        $$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp
        """
    )
    op.execute("ALTER FUNCTION delete_expired_context_packs(text) OWNER TO vera_erasure")
    op.execute("REVOKE ALL ON FUNCTION delete_expired_context_packs(text) FROM PUBLIC")
    op.execute("GRANT EXECUTE ON FUNCTION delete_expired_context_packs(text) TO vera_app")
    op.execute("GRANT UPDATE (id) ON context_packs TO vera_erasure")
    op.execute(
        """
        CREATE FUNCTION delete_all_expired_context_packs() RETURNS integer AS $$
        DECLARE
            removed integer;
        BEGIN
            PERFORM set_config('vera.erasure_mode', 'on', true);
            WITH expired AS (
                SELECT id FROM context_packs
                 WHERE expires_at <= now()
                 ORDER BY expires_at, id
                 LIMIT 1000
                 FOR UPDATE SKIP LOCKED
            )
            DELETE FROM context_packs packs
             USING expired
             WHERE packs.id = expired.id;
            GET DIAGNOSTICS removed = ROW_COUNT;
            RETURN removed;
        END;
        $$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp
        """
    )
    op.execute("ALTER FUNCTION delete_all_expired_context_packs() OWNER TO vera_erasure")
    op.execute("REVOKE ALL ON FUNCTION delete_all_expired_context_packs() FROM PUBLIC")
    op.execute("GRANT EXECUTE ON FUNCTION delete_all_expired_context_packs() TO vera_worker")


def downgrade() -> None:
    op.execute("DROP FUNCTION delete_all_expired_context_packs()")
    op.execute("REVOKE UPDATE (id) ON context_packs FROM vera_erasure")
    op.execute("DROP FUNCTION delete_expired_context_packs(text)")
    op.alter_column(
        "knowledge_snapshots",
        "assembler_version",
        existing_type=sa.String(length=64),
        server_default=sa.text("'context-assembler-v2'"),
    )
    op.drop_table("proposal_attempts")
    op.drop_constraint("ck_feedback_rank_positive", "retrieval_feedback", type_="check")
    op.drop_column("retrieval_feedback", "context_pack_id")
    op.drop_column("retrieval_feedback", "rank")
    op.execute("DROP FUNCTION canonical_repository_ref(text)")
