"""fact transaction history

Revision ID: 3e5f6a7b8c9d
Revises: 2d4e5f6a7b8c
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "3e5f6a7b8c9d"
down_revision: str | None = "2d4e5f6a7b8c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

APP_ROLE = "vera_app"
_LIFECYCLE_VALUES = "'proposed', 'active', 'disputed', 'superseded', 'retracted', 'expired'"


def upgrade() -> None:
    op.create_table(
        "fact_revisions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("uuidv7()"),
            nullable=False,
        ),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("fact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("lifecycle_state", sa.String(16), nullable=False),
        sa.Column("authority", sa.Float(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("system_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("system_to", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["fact_id", "group_id"],
            ["facts.id", "facts.group_id"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            f"lifecycle_state IN ({_LIFECYCLE_VALUES})",
            name="ck_fact_revision_lifecycle",
        ),
    )
    op.create_index(
        "ix_fact_revisions_lookup",
        "fact_revisions",
        ["group_id", "fact_id", sa.text("system_from DESC")],
    )
    op.create_index(
        "uq_fact_revision_current",
        "fact_revisions",
        ["group_id", "fact_id"],
        unique=True,
        postgresql_where=sa.text("system_to IS NULL"),
    )
    op.execute(
        """
        INSERT INTO fact_revisions (
            group_id, fact_id, lifecycle_state, authority, confidence,
            valid_from, valid_to, expires_at, system_from, system_to
        )
        SELECT group_id, id, lifecycle_state, authority, confidence,
               valid_from, valid_to, expires_at, system_from, NULL
        FROM facts
        """
    )
    op.execute(
        """
        CREATE FUNCTION record_fact_revision() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            changed_at timestamptz := clock_timestamp();
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                UPDATE fact_revisions
                SET system_to = changed_at
                WHERE group_id = OLD.group_id AND fact_id = OLD.id AND system_to IS NULL;
            END IF;

            INSERT INTO fact_revisions (
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
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER facts_record_revision_insert
        AFTER INSERT ON facts
        FOR EACH ROW EXECUTE FUNCTION record_fact_revision()
        """
    )
    op.execute(
        """
        CREATE TRIGGER facts_record_revision_update
        AFTER UPDATE OF lifecycle_state, authority, confidence, valid_from, valid_to, expires_at
        ON facts
        FOR EACH ROW
        WHEN (
            OLD.lifecycle_state IS DISTINCT FROM NEW.lifecycle_state
            OR OLD.authority IS DISTINCT FROM NEW.authority
            OR OLD.confidence IS DISTINCT FROM NEW.confidence
            OR OLD.valid_from IS DISTINCT FROM NEW.valid_from
            OR OLD.valid_to IS DISTINCT FROM NEW.valid_to
            OR OLD.expires_at IS DISTINCT FROM NEW.expires_at
        )
        EXECUTE FUNCTION record_fact_revision()
        """
    )
    op.execute("ALTER TABLE fact_revisions ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE fact_revisions FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON fact_revisions "
        "USING (group_id = current_setting('vera.group_id', true)) "
        "WITH CHECK (group_id = current_setting('vera.group_id', true))"
    )
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON fact_revisions TO {APP_ROLE}")


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS facts_record_revision_update ON facts")
    op.execute("DROP TRIGGER IF EXISTS facts_record_revision_insert ON facts")
    op.execute("DROP FUNCTION IF EXISTS record_fact_revision()")
    op.drop_table("fact_revisions")
