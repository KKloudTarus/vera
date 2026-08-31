"""harden fact revision history

Revision ID: 5a7b8c9d0e1f
Revises: 4f6a7b8c9d0e
Create Date: 2026-08-31
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "5a7b8c9d0e1f"
down_revision: str | None = "4f6a7b8c9d0e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_HISTORY_WRITER = "vera_fact_history_writer"
_RUNTIME_ROLES = "vera_app, vera_trusted, vera_worker"


def upgrade() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{_HISTORY_WRITER}') THEN
                CREATE ROLE {_HISTORY_WRITER}
                    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                    NOINHERIT NOREPLICATION NOBYPASSRLS;
            END IF;
        END
        $$;
        """
    )
    op.execute(
        f"ALTER ROLE {_HISTORY_WRITER} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
        "NOINHERIT NOREPLICATION NOBYPASSRLS"
    )
    op.execute(f"GRANT USAGE ON SCHEMA public TO {_HISTORY_WRITER}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.fact_revisions FROM {_HISTORY_WRITER}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON TABLE public.fact_revisions TO {_HISTORY_WRITER}")
    op.execute(
        f"CREATE POLICY fact_revision_writer_select ON public.fact_revisions "
        f"FOR SELECT TO {_HISTORY_WRITER} USING (true)"
    )
    op.execute(
        f"CREATE POLICY fact_revision_writer_insert ON public.fact_revisions "
        f"FOR INSERT TO {_HISTORY_WRITER} WITH CHECK (true)"
    )
    op.execute(
        f"CREATE POLICY fact_revision_writer_update ON public.fact_revisions "
        f"FOR UPDATE TO {_HISTORY_WRITER} USING (true) WITH CHECK (true)"
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.record_fact_revision() RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            changed_at timestamptz := clock_timestamp();
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                UPDATE public.fact_revisions
                SET system_to = changed_at
                WHERE group_id = OLD.group_id AND fact_id = OLD.id AND system_to IS NULL;
            END IF;

            INSERT INTO public.fact_revisions (
                group_id, fact_id, lifecycle_state, authority, confidence,
                valid_from, valid_to, expires_at, system_from, system_to
            ) VALUES (
                NEW.group_id, NEW.id, NEW.lifecycle_state, NEW.authority, NEW.confidence,
                NEW.valid_from, NEW.valid_to, NEW.expires_at,
                CASE WHEN TG_OP = 'INSERT' THEN NEW.system_from ELSE changed_at END,
                NULL
            );
            RETURN NEW;
        END;
        $function$
        """
    )
    op.execute(f"ALTER FUNCTION public.record_fact_revision() OWNER TO {_HISTORY_WRITER}")
    op.execute("REVOKE ALL ON FUNCTION public.record_fact_revision() FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION public.record_fact_revision() FROM {_RUNTIME_ROLES}")
    op.execute(
        f"REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE public.fact_revisions "
        f"FROM PUBLIC, {_RUNTIME_ROLES}"
    )
    op.execute(f"GRANT SELECT ON TABLE public.fact_revisions TO {_RUNTIME_ROLES}")


def downgrade() -> None:
    raise RuntimeError("fact revision history hardening is forward-only")
