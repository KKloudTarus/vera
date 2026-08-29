"""artifact version ordering

Adds source-native and observed ordering metadata so stale same-source deliveries are rejected
without comparing UUIDs.

Revision ID: a5c6d7e8f9b0
Revises: f4b2c3d4e5f6
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a5c6d7e8f9b0"
down_revision: str | None = "f4b2c3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("artifact_versions", sa.Column("source_revision", sa.BigInteger(), nullable=True))
    op.add_column(
        "artifact_versions",
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "artifact_versions", sa.Column("source_version_id", sa.String(256), nullable=True)
    )
    op.add_column(
        "artifact_versions", sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "artifact_versions",
        sa.Column("predecessor_version_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.execute(
        "UPDATE artifact_versions SET observed_at = COALESCE(created_at, reference_time) "
        "WHERE observed_at IS NULL"
    )
    op.execute(
        "UPDATE artifact_versions current SET predecessor_version_id = previous.id "
        "FROM artifact_versions previous "
        "WHERE previous.artifact_id = current.artifact_id "
        "AND previous.version = current.version - 1 "
        "AND current.predecessor_version_id IS NULL"
    )
    op.alter_column(
        "artifact_versions", "observed_at", nullable=False, server_default=sa.func.now()
    )
    op.execute(
        "ALTER TABLE artifact_versions ADD CONSTRAINT fk_artifact_version_predecessor "
        "FOREIGN KEY (predecessor_version_id) REFERENCES artifact_versions (id) "
        "ON DELETE SET NULL NOT VALID"
    )
    op.execute("ALTER TABLE artifact_versions VALIDATE CONSTRAINT fk_artifact_version_predecessor")
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_artifact_versions_predecessor",
            "artifact_versions",
            ["predecessor_version_id"],
            postgresql_concurrently=True,
        )
        op.create_index(
            "uq_artifact_source_revision",
            "artifact_versions",
            ["artifact_id", "source_revision"],
            unique=True,
            postgresql_where=sa.text("source_revision IS NOT NULL"),
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "uq_artifact_source_revision",
            table_name="artifact_versions",
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_artifact_versions_predecessor",
            table_name="artifact_versions",
            postgresql_concurrently=True,
        )
    op.drop_constraint("fk_artifact_version_predecessor", "artifact_versions", type_="foreignkey")
    op.drop_column("artifact_versions", "predecessor_version_id")
    op.drop_column("artifact_versions", "observed_at")
    op.drop_column("artifact_versions", "source_version_id")
    op.drop_column("artifact_versions", "source_updated_at")
    op.drop_column("artifact_versions", "source_revision")
