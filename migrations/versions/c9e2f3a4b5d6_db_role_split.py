"""knowledge fabric phase 7: production database role split

Adds the two non-superuser roles the cross-scope paths need, so production never relies on a
superuser login (invariant 12 / section 13). The deployment model:

- the migration owner (a superuser or the schema owner) runs migrations only;
- ``vera_app`` (existing, NOBYPASSRLS) is the tenant path: the API SET ROLEs to it and RLS is
  enforced per request;
- ``vera_trusted`` (BYPASSRLS, read-only) is the cross-scope read path (the retrieval and
  knowledge read models, which filter group_id explicitly across a principal's resolved
  scopes);
- ``vera_worker`` (BYPASSRLS, read/write) is the worker and projection path, which rebuilds
  the graph and writes fabric rows across groups, always filtering group_id explicitly.

The BYPASSRLS roles still write and read only through queries that pass explicit group_id
filters; RLS remains the enforcing boundary for the tenant (vera_app) path.

Revision ID: c9e2f3a4b5d6
Revises: b8d0f1a2c3e4
Create Date: 2026-08-28
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "c9e2f3a4b5d6"
down_revision: str | None = "b8d0f1a2c3e4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TRUSTED = "vera_trusted"
_WORKER = "vera_worker"


def _ensure_role(name: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{name}') THEN
                CREATE ROLE {name} NOLOGIN NOSUPERUSER BYPASSRLS;
            END IF;
        END
        $$;
        """
    )


def upgrade() -> None:
    _ensure_role(_TRUSTED)
    _ensure_role(_WORKER)

    # Trusted read path: read-only across all tables.
    op.execute(f"GRANT USAGE ON SCHEMA public TO {_TRUSTED}")
    op.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {_TRUSTED}")
    op.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {_TRUSTED}")

    # Worker / projection path: read/write across all tables (still filters group_id in SQL).
    op.execute(f"GRANT USAGE ON SCHEMA public TO {_WORKER}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {_WORKER}")
    op.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {_WORKER}")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {_WORKER}"
    )
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO {_WORKER}"
    )


def downgrade() -> None:
    for role in (_WORKER, _TRUSTED):
        op.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
            f"REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM {role}"
        )
        op.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
            f"REVOKE USAGE, SELECT ON SEQUENCES FROM {role}"
        )
        op.execute(f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {role}")
        op.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {role}")
        op.execute(f"REVOKE USAGE ON SCHEMA public FROM {role}")
        op.execute(f"DROP ROLE IF EXISTS {role}")
