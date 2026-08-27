"""allow 'filesystem' knowledge source kind

Adds the filesystem connector's kind to the ck_source_kind check constraint.

Revision ID: c8e1f0a9b2d3
Revises: b7f3a1c2d4e5
Create Date: 2026-08-27 13:15:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "c8e1f0a9b2d3"
down_revision: str | None = "b7f3a1c2d4e5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KINDS_NEW = "'git','confluence','jira','cmdb','slack','pdf','filesystem','agent'"
_KINDS_OLD = "'git','confluence','jira','cmdb','slack','pdf','agent'"


def _reset(kinds: str) -> None:
    op.execute("ALTER TABLE knowledge_sources DROP CONSTRAINT IF EXISTS ck_source_kind")
    op.execute(f"ALTER TABLE knowledge_sources ADD CONSTRAINT ck_source_kind CHECK (kind IN ({kinds}))")


def upgrade() -> None:
    _reset(_KINDS_NEW)


def downgrade() -> None:
    _reset(_KINDS_OLD)
