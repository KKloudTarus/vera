"""persist LLM usage cost completeness

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-09-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d4e5f6a7b8c9"
down_revision: str | None = "c3d4e5f6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_DATABASE_ROLES = {
    "vera_app": "NOBYPASSRLS",
    "vera_trusted": "BYPASSRLS",
    "vera_worker": "BYPASSRLS",
}
_APP_MEMBERS = ("vera_legacy", "vera_runtime", "vera_worker_runtime")
_TRUSTED_MEMBERS = ("vera_runtime", "vera_worker_runtime")
_WORKER_MEMBERS = ("vera_worker_runtime",)
_LOGIN_ROLES = {
    "vera_runtime": ("LOGIN", "NOBYPASSRLS", ("vera_app", "vera_trusted")),
    "vera_worker_runtime": (
        "LOGIN",
        "NOBYPASSRLS",
        ("vera_app", "vera_trusted", "vera_worker"),
    ),
    "vera_scaler_runtime": ("NOLOGIN", "NOBYPASSRLS", ()),
    "vera_legacy": ("LOGIN", "BYPASSRLS", ("vera_app",)),
}
_SNAPSHOT_CHILDREN = (
    "snapshot_facts",
    "snapshot_sources",
    "snapshot_fact_sources",
    "snapshot_fact_citations",
    "snapshot_chunks",
    "snapshot_chunk_embeddings",
    "snapshot_fact_embeddings",
)


def _harden_database_role(name: str, row_security: str) -> None:
    op.execute(
        f"ALTER ROLE {name} NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE "
        f"NOREPLICATION {row_security}"
    )
    op.execute(
        f"""
        DO $$
        DECLARE granted_role text;
        BEGIN
            FOR granted_role IN
                SELECT granted.rolname
                FROM pg_auth_members membership
                JOIN pg_roles granted ON granted.oid = membership.roleid
                JOIN pg_roles member ON member.oid = membership.member
                WHERE member.rolname = '{name}'
            LOOP
                EXECUTE format('REVOKE %I FROM {name} CASCADE', granted_role);
            END LOOP;
        END
        $$;
        """
    )
    op.execute(
        f"""
        DO $$
        DECLARE role_oid oid := (SELECT oid FROM pg_roles WHERE rolname = '{name}');
        BEGIN
            IF EXISTS (
                SELECT FROM pg_shdepend
                WHERE refclassid = 'pg_authid'::regclass
                  AND refobjid = role_oid
                  AND deptype = 'o'
            ) THEN
                RAISE EXCEPTION 'database role {name} owns database objects';
            END IF;
        END
        $$;
        """
    )


def _reset_role_members(role: str, allowed: tuple[str, ...]) -> None:
    op.execute(
        f"""
        DO $$
        DECLARE member_role text;
        BEGIN
            FOR member_role IN
                SELECT member.rolname
                FROM pg_auth_members membership
                JOIN pg_roles granted ON granted.oid = membership.roleid
                JOIN pg_roles member ON member.oid = membership.member
                WHERE granted.rolname = '{role}'
            LOOP
                EXECUTE format('REVOKE {role} FROM %I CASCADE', member_role);
            END LOOP;
        END
        $$;
        """
    )
    for member in allowed:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT FROM pg_roles WHERE rolname = '{member}') THEN
                    GRANT {role} TO {member};
                END IF;
            END
            $$;
            """
        )


def _harden_existing_login(
    name: str, login_status: str, row_security: str, allowed_memberships: tuple[str, ...]
) -> None:
    op.execute(
        f"""
        DO $$
        DECLARE role_oid oid := (SELECT oid FROM pg_roles WHERE rolname = '{name}');
        DECLARE related_role text;
        BEGIN
            IF role_oid IS NULL THEN
                RETURN;
            END IF;
            IF EXISTS (
                SELECT FROM pg_shdepend
                WHERE refclassid = 'pg_authid'::regclass
                  AND refobjid = role_oid
                  AND deptype = 'o'
            ) THEN
                RAISE EXCEPTION 'database role {name} owns database objects';
            END IF;
            ALTER ROLE {name}
                {login_status} NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
                NOREPLICATION {row_security};
            FOR related_role IN
                SELECT granted.rolname
                FROM pg_auth_members membership
                JOIN pg_roles granted ON granted.oid = membership.roleid
                WHERE membership.member = role_oid
            LOOP
                EXECUTE format('REVOKE %I FROM {name} CASCADE', related_role);
            END LOOP;
            FOR related_role IN
                SELECT member.rolname
                FROM pg_auth_members membership
                JOIN pg_roles member ON member.oid = membership.member
                WHERE membership.roleid = role_oid
            LOOP
                EXECUTE format('REVOKE {name} FROM %I CASCADE', related_role);
            END LOOP;
        END
        $$;
        """
    )
    for granted_role in allowed_memberships:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT FROM pg_roles WHERE rolname = '{name}') THEN
                    GRANT {granted_role} TO {name};
                END IF;
            END
            $$;
            """
        )


def _normalize_database_role_privileges() -> None:
    roles = ", ".join(_DATABASE_ROLES)
    op.execute(
        f"""
        DO $$
        BEGIN
            EXECUTE format('REVOKE ALL PRIVILEGES ON DATABASE %I FROM {roles}', current_database());
            EXECUTE format('GRANT CONNECT ON DATABASE %I TO {roles}', current_database());
        END
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON SCHEMA public FROM {roles}")
    op.execute(f"GRANT USAGE ON SCHEMA public TO {roles}")
    op.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {roles}")
    op.execute(
        f"""
        DO $$
        DECLARE column_acl record;
        BEGIN
            FOR column_acl IN
                SELECT namespace.nspname, relation.relname, attribute.attname
                FROM pg_attribute attribute
                JOIN pg_class relation ON relation.oid = attribute.attrelid
                JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'public'
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
                  AND attribute.attacl IS NOT NULL
            LOOP
                EXECUTE format(
                    'REVOKE ALL PRIVILEGES (%I) ON TABLE %I.%I FROM {roles}',
                    column_acl.attname,
                    column_acl.nspname,
                    column_acl.relname
                );
            END LOOP;
        END
        $$;
        """
    )
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO vera_app")
    op.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO vera_trusted")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO vera_worker")
    op.execute(f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {roles}")
    op.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO vera_app, vera_worker")
    op.execute(f"REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM {roles}")
    op.execute("GRANT EXECUTE ON FUNCTION delete_expired_context_packs(text) TO vera_app")
    op.execute("GRANT EXECUTE ON FUNCTION delete_all_expired_context_packs() TO vera_worker")
    op.execute(
        "GRANT EXECUTE ON FUNCTION erase_artifact_retrieval_inputs(text, uuid[]) TO vera_app"
    )

    snapshot_children = ", ".join(_SNAPSHOT_CHILDREN)
    op.execute(f"REVOKE ALL ON {snapshot_children} FROM {roles}")
    op.execute(f"GRANT SELECT, INSERT ON {snapshot_children} TO vera_app")
    op.execute(f"GRANT SELECT ON {snapshot_children} TO vera_trusted, vera_worker")
    op.execute("REVOKE DELETE ON knowledge_snapshots FROM vera_app")
    op.execute(
        "REVOKE INSERT, UPDATE, DELETE ON knowledge_snapshots FROM vera_trusted, vera_worker"
    )
    op.execute("REVOKE UPDATE, DELETE ON chunks FROM vera_app, vera_trusted, vera_worker")
    op.execute("REVOKE UPDATE, DELETE ON context_packs FROM vera_app, vera_trusted, vera_worker")
    op.execute(f"REVOKE ALL ON community_fact_lineage FROM {roles}")
    op.execute("GRANT SELECT ON community_fact_lineage TO vera_app, vera_trusted, vera_worker")
    op.execute("GRANT INSERT ON community_fact_lineage TO vera_worker")
    op.execute(f"REVOKE ALL ON proposal_attempts FROM {roles}")
    op.execute("GRANT SELECT, INSERT ON proposal_attempts TO vera_app")
    op.execute("GRANT SELECT ON proposal_attempts TO vera_trusted")
    op.execute(f"REVOKE ALL ON legal_holds FROM {roles}")
    op.execute("GRANT SELECT, INSERT, UPDATE ON legal_holds TO vera_app, vera_worker")
    op.execute("GRANT SELECT ON legal_holds TO vera_trusted")
    op.execute(f"REVOKE ALL ON fact_revisions FROM {roles}")
    op.execute("GRANT SELECT ON fact_revisions TO vera_app, vera_trusted, vera_worker")


def _normalize_legacy_privileges() -> None:
    op.execute(
        """
        DO $$
        DECLARE target_relation record;
        DECLARE column_acl record;
        DECLARE privilege_list text;
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vera_legacy') THEN
                RETURN;
            END IF;
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON DATABASE %I FROM vera_legacy', current_database()
            );
            REVOKE ALL ON SCHEMA public FROM vera_legacy;
            REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vera_legacy;
            FOR column_acl IN
                SELECT namespace.nspname, class.relname, attribute.attname
                FROM pg_attribute attribute
                JOIN pg_class class ON class.oid = attribute.attrelid
                JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
                WHERE namespace.nspname = 'public'
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
                  AND attribute.attacl IS NOT NULL
            LOOP
                EXECUTE format(
                    'REVOKE ALL PRIVILEGES (%I) ON TABLE %I.%I FROM vera_legacy',
                    column_acl.attname,
                    column_acl.nspname,
                    column_acl.relname
                );
            END LOOP;
            REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM vera_legacy;
            REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM vera_legacy;
            EXECUTE format('GRANT CONNECT ON DATABASE %I TO vera_legacy', current_database());
            GRANT USAGE ON SCHEMA public TO vera_legacy;
            FOR target_relation IN
                SELECT class.oid, namespace.nspname, class.relname
                FROM pg_class class
                JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
                WHERE namespace.nspname = 'public'
                  AND class.relkind IN ('r', 'p', 'v', 'm', 'f')
            LOOP
                SELECT string_agg(candidate.privilege, ', ' ORDER BY candidate.privilege)
                INTO privilege_list
                FROM (VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE')) candidate(privilege)
                WHERE has_table_privilege(
                    'vera_app', target_relation.oid, candidate.privilege
                );
                IF privilege_list IS NOT NULL THEN
                    EXECUTE format(
                        'GRANT %s ON TABLE %I.%I TO vera_legacy',
                        privilege_list,
                        target_relation.nspname,
                        target_relation.relname
                    );
                END IF;
            END LOOP;
            FOR target_relation IN
                SELECT class.oid, namespace.nspname, class.relname
                FROM pg_class class
                JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
                WHERE namespace.nspname = 'public' AND class.relkind = 'S'
            LOOP
                SELECT string_agg(candidate.privilege, ', ' ORDER BY candidate.privilege)
                INTO privilege_list
                FROM (VALUES ('SELECT'), ('USAGE'), ('UPDATE')) candidate(privilege)
                WHERE has_sequence_privilege(
                    'vera_app', target_relation.oid, candidate.privilege
                );
                IF privilege_list IS NOT NULL THEN
                    EXECUTE format(
                        'GRANT %s ON SEQUENCE %I.%I TO vera_legacy',
                        privilege_list,
                        target_relation.nspname,
                        target_relation.relname
                    );
                END IF;
            END LOOP;
        END
        $$;
        """
    )


def upgrade() -> None:
    op.create_table(
        "provider_run_budget_reservations",
        sa.Column("run_key", sa.String(length=256), nullable=False),
        sa.Column("max_cost_usd", sa.Float(), nullable=False),
        sa.Column("reserved_cost_usd", sa.Float(), server_default="0", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "max_cost_usd > 0 AND max_cost_usd < 'Infinity'::double precision",
            name="ck_provider_run_budget_maximum",
        ),
        sa.CheckConstraint(
            "reserved_cost_usd >= 0 AND reserved_cost_usd < 'Infinity'::double precision",
            name="ck_provider_run_budget_reserved",
        ),
        sa.CheckConstraint(
            "reserved_cost_usd <= max_cost_usd",
            name="ck_provider_run_budget_within_maximum",
        ),
        sa.PrimaryKeyConstraint("run_key"),
    )
    op.create_table(
        "provider_budget_reservations",
        sa.Column("action_key", sa.String(length=512), nullable=False),
        sa.Column("run_key", sa.String(length=256), nullable=False),
        sa.Column("max_cost_usd", sa.Float(), nullable=False),
        sa.Column("reserved_cost_usd", sa.Float(), server_default="0", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "max_cost_usd > 0 AND max_cost_usd < 'Infinity'::double precision",
            name="ck_provider_budget_maximum",
        ),
        sa.CheckConstraint(
            "reserved_cost_usd >= 0 AND reserved_cost_usd < 'Infinity'::double precision",
            name="ck_provider_budget_reserved",
        ),
        sa.CheckConstraint(
            "reserved_cost_usd <= max_cost_usd",
            name="ck_provider_budget_within_maximum",
        ),
        sa.ForeignKeyConstraint(
            ["run_key"], ["provider_run_budget_reservations.run_key"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("action_key"),
    )
    op.create_index(
        "ix_provider_budget_run_key", "provider_budget_reservations", ["run_key"], unique=False
    )
    op.execute(
        """
        CREATE FUNCTION enforce_provider_budget_immutable_fields() RETURNS trigger AS $$
        BEGIN
            IF (to_jsonb(NEW) - 'reserved_cost_usd')
               IS DISTINCT FROM (to_jsonb(OLD) - 'reserved_cost_usd') THEN
                RAISE EXCEPTION 'provider budget identity and ceiling are immutable';
            END IF;
            RETURN NEW;
        END
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute("REVOKE ALL ON FUNCTION enforce_provider_budget_immutable_fields() FROM PUBLIC")
    for table in ("provider_budget_reservations", "provider_run_budget_reservations"):
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable BEFORE UPDATE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION enforce_provider_budget_immutable_fields()"
        )
    for name, row_security in _DATABASE_ROLES.items():
        _harden_database_role(name, row_security)
    for name, (login_status, row_security, memberships) in _LOGIN_ROLES.items():
        _harden_existing_login(name, login_status, row_security, memberships)
    _reset_role_members("vera_app", _APP_MEMBERS)
    _reset_role_members("vera_trusted", _TRUSTED_MEMBERS)
    _reset_role_members("vera_worker", _WORKER_MEMBERS)
    _normalize_database_role_privileges()
    op.execute(
        "REVOKE UPDATE, DELETE, TRUNCATE ON llm_usage FROM vera_app, vera_trusted, vera_worker"
    )
    op.execute("REVOKE INSERT ON llm_usage FROM vera_trusted")
    op.execute("GRANT SELECT, INSERT ON llm_usage TO vera_app, vera_worker")
    op.execute("GRANT SELECT ON llm_usage TO vera_trusted")
    for table in ("provider_budget_reservations", "provider_run_budget_reservations"):
        op.execute(f"REVOKE UPDATE, DELETE ON {table} FROM vera_app, vera_trusted, vera_worker")
        op.execute(f"GRANT SELECT, INSERT, UPDATE ON {table} TO vera_app, vera_worker")
        op.execute(f"GRANT SELECT ON {table} TO vera_trusted")
        op.execute(f"GRANT DELETE ON {table} TO vera_worker")
    _normalize_legacy_privileges()
    op.execute(
        "ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS "
        "cost_complete boolean DEFAULT false NOT NULL"
    )
    op.execute(
        "ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS "
        "provider_retry_fenced boolean DEFAULT false NOT NULL"
    )
    op.execute("ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS claim_token uuid")
    op.execute(
        """
        CREATE OR REPLACE FUNCTION enforce_ingestion_claim_fence() RETURNS trigger AS $$
        BEGIN
            IF OLD.status = 'pending' AND NEW.status = 'inflight'
               AND OLD.provider_retry_fenced THEN
                RAISE EXCEPTION 'provider-fenced ingestion job cannot be claimed';
            END IF;
            RETURN NEW;
        END
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute("REVOKE ALL ON FUNCTION enforce_ingestion_claim_fence() FROM PUBLIC")
    op.execute("DROP TRIGGER IF EXISTS trg_enforce_ingestion_claim_fence ON ingestion_jobs")
    op.execute(
        """
        CREATE TRIGGER trg_enforce_ingestion_claim_fence
        BEFORE UPDATE ON ingestion_jobs
        FOR EACH ROW EXECUTE FUNCTION enforce_ingestion_claim_fence()
        """
    )
    constraints = {
        "ck_llm_usage_prompt_tokens_nonnegative": "prompt_tokens >= 0",
        "ck_llm_usage_completion_tokens_nonnegative": "completion_tokens >= 0",
        "ck_llm_usage_cost_finite_nonnegative": (
            "cost_usd >= 0 AND cost_usd < 'Infinity'::double precision"
        ),
    }
    for name, expression in constraints.items():
        op.execute(
            f"""
            ALTER TABLE llm_usage
            ADD CONSTRAINT {name} CHECK ({expression}) NOT VALID
            """
        )
    op.execute(
        "UPDATE llm_usage SET "
        "prompt_tokens=GREATEST(prompt_tokens, 0), "
        "completion_tokens=GREATEST(completion_tokens, 0), "
        "cost_usd=CASE WHEN cost_usd >= 0 "
        "AND cost_usd < 'Infinity'::double precision THEN cost_usd ELSE 0 END, "
        "cost_complete=false WHERE prompt_tokens < 0 OR completion_tokens < 0 OR "
        "NOT (cost_usd >= 0 AND cost_usd < 'Infinity'::double precision)"
    )
    for name in constraints:
        op.execute(f"ALTER TABLE llm_usage VALIDATE CONSTRAINT {name}")


def downgrade() -> None:
    op.execute("LOCK TABLE ingestion_jobs IN ACCESS EXCLUSIVE MODE")
    op.execute(
        "UPDATE ingestion_jobs SET status='dead', locked_until=NULL, "
        "last_error=COALESCE(last_error, 'provider retry fenced before downgrade') "
        "WHERE provider_retry_fenced AND status IN ('pending', 'inflight')"
    )
    op.execute("DROP TRIGGER IF EXISTS trg_enforce_ingestion_claim_fence ON ingestion_jobs")
    op.execute("DROP FUNCTION IF EXISTS enforce_ingestion_claim_fence()")
    op.drop_column("ingestion_jobs", "claim_token")
    op.drop_column("ingestion_jobs", "provider_retry_fenced")
    for table in ("provider_budget_reservations", "provider_run_budget_reservations"):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_immutable ON {table}")
    op.execute("DROP FUNCTION IF EXISTS enforce_provider_budget_immutable_fields()")
    op.drop_table("provider_budget_reservations")
    op.drop_table("provider_run_budget_reservations")
    op.drop_constraint("ck_llm_usage_cost_finite_nonnegative", "llm_usage", type_="check")
    op.drop_constraint("ck_llm_usage_completion_tokens_nonnegative", "llm_usage", type_="check")
    op.drop_constraint("ck_llm_usage_prompt_tokens_nonnegative", "llm_usage", type_="check")
    op.drop_column("llm_usage", "cost_complete")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON llm_usage TO vera_app, vera_worker")
