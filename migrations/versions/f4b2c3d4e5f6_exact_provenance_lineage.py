"""exact chunk provenance and extraction lineage

Adds durable extraction runs and carries exact quote coordinates from candidate claims into
Fabric assertions and evidence. The previous free-form assertion run id is retained as
``run_key`` for legacy proposal and backfill idempotency.

Revision ID: f4b2c3d4e5f6
Revises: e2b3c4d5f6a7
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f4b2c3d4e5f6"
down_revision: str | None = "e2b3c4d5f6a7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

APP_ROLE = "vera_app"
TRUSTED_ROLE = "vera_trusted"
WORKER_ROLE = "vera_worker"


def upgrade() -> None:
    op.create_table(
        "extraction_runs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            server_default=sa.text("uuidv7()"),
        ),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column(
            "artifact_version_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("artifact_versions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("model", sa.String(256), nullable=False),
        sa.Column("provider", sa.String(128), nullable=False),
        sa.Column("prompt_version", sa.String(64), nullable=False),
        sa.Column("pipeline_version", postgresql.JSONB, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_extraction_runs_version", "extraction_runs", ["artifact_version_id"])
    op.execute("ALTER TABLE extraction_runs ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE extraction_runs FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON extraction_runs "
        "USING (group_id = current_setting('vera.group_id', true)) "
        "WITH CHECK (group_id = current_setting('vera.group_id', true))"
    )
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON extraction_runs TO {APP_ROLE}")
    op.execute(f"GRANT SELECT ON extraction_runs TO {TRUSTED_ROLE}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON extraction_runs TO {WORKER_ROLE}")

    op.add_column(
        "candidate_claims",
        sa.Column("extraction_run_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.execute(
        "ALTER TABLE candidate_claims ADD CONSTRAINT fk_candidate_claims_extraction_run "
        "FOREIGN KEY (extraction_run_id) REFERENCES extraction_runs (id) "
        "ON DELETE SET NULL NOT VALID"
    )
    op.add_column(
        "candidate_claims", sa.Column("chunk_id", postgresql.UUID(as_uuid=True), nullable=True)
    )
    op.execute(
        "ALTER TABLE candidate_claims ADD CONSTRAINT fk_candidate_claims_chunk "
        "FOREIGN KEY (chunk_id) REFERENCES chunks (id) ON DELETE SET NULL NOT VALID"
    )
    op.add_column("candidate_claims", sa.Column("source_quote", sa.Text(), nullable=True))
    op.add_column("candidate_claims", sa.Column("quote_start", sa.Integer(), nullable=True))
    op.add_column("candidate_claims", sa.Column("quote_end", sa.Integer(), nullable=True))
    op.add_column("candidate_claims", sa.Column("quote_hash", sa.String(128), nullable=True))
    op.add_column(
        "candidate_claims",
        sa.Column("needs_review", sa.Boolean(), nullable=False, server_default=sa.false()),
    )

    op.alter_column("assertions", "extraction_run_id", new_column_name="run_key")
    op.add_column(
        "assertions",
        sa.Column("extraction_run_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.execute(
        "ALTER TABLE assertions ADD CONSTRAINT fk_assertions_extraction_run "
        "FOREIGN KEY (extraction_run_id) REFERENCES extraction_runs (id) "
        "ON DELETE SET NULL NOT VALID"
    )
    op.drop_constraint("ck_assertion_state", "assertions", type_="check")
    op.execute(
        "ALTER TABLE assertions ADD CONSTRAINT ck_assertion_state "
        "CHECK (state IN ('active', 'needs_review', 'withdrawn')) NOT VALID"
    )

    op.add_column("evidence", sa.Column("quote_start", sa.Integer(), nullable=True))
    op.add_column("evidence", sa.Column("quote_end", sa.Integer(), nullable=True))
    op.add_column("evidence", sa.Column("quote_hash", sa.String(128), nullable=True))
    op.add_column("evidence", sa.Column("citation_override", sa.Text(), nullable=True))
    op.add_column(
        "evidence", sa.Column("extraction_run_id", postgresql.UUID(as_uuid=True), nullable=True)
    )
    op.execute(
        "ALTER TABLE evidence ADD CONSTRAINT fk_evidence_extraction_run "
        "FOREIGN KEY (extraction_run_id) REFERENCES extraction_runs (id) "
        "ON DELETE SET NULL NOT VALID"
    )
    op.execute(
        "ALTER TABLE evidence ADD CONSTRAINT ck_evidence_quote_offsets CHECK ("
        "(quote_start IS NULL AND quote_end IS NULL AND quote_hash IS NULL) OR "
        "(quote_start >= 0 AND quote_end > quote_start AND quote_hash IS NOT NULL)"
        ") NOT VALID"
    )

    for table, constraint in (
        ("candidate_claims", "fk_candidate_claims_extraction_run"),
        ("candidate_claims", "fk_candidate_claims_chunk"),
        ("assertions", "fk_assertions_extraction_run"),
        ("assertions", "ck_assertion_state"),
        ("evidence", "fk_evidence_extraction_run"),
        ("evidence", "ck_evidence_quote_offsets"),
    ):
        op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {constraint}")

    with op.get_context().autocommit_block():
        op.create_index(
            "ix_assertions_extraction_run",
            "assertions",
            ["extraction_run_id"],
            postgresql_concurrently=True,
        )
        op.create_index(
            "uq_assertion_run_key",
            "assertions",
            ["fact_id", "run_key", "polarity"],
            unique=True,
            postgresql_where=sa.text("run_key IS NOT NULL"),
            postgresql_concurrently=True,
        )
        op.create_index(
            "ix_evidence_extraction_run",
            "evidence",
            ["extraction_run_id"],
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_evidence_extraction_run",
            table_name="evidence",
            postgresql_concurrently=True,
        )
        op.drop_index(
            "uq_assertion_run_key",
            table_name="assertions",
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_assertions_extraction_run",
            table_name="assertions",
            postgresql_concurrently=True,
        )

    op.drop_constraint("ck_evidence_quote_offsets", "evidence", type_="check")
    op.drop_constraint("fk_evidence_extraction_run", "evidence", type_="foreignkey")
    op.drop_column("evidence", "extraction_run_id")
    op.drop_column("evidence", "citation_override")
    op.drop_column("evidence", "quote_hash")
    op.drop_column("evidence", "quote_end")
    op.drop_column("evidence", "quote_start")

    op.drop_constraint("ck_assertion_state", "assertions", type_="check")
    op.execute(
        "UPDATE assertions SET state = 'withdrawn', withdrawn_at = COALESCE(withdrawn_at, now()) "
        "WHERE state = 'needs_review'"
    )
    op.execute(
        "ALTER TABLE assertions ADD CONSTRAINT ck_assertion_state "
        "CHECK (state IN ('active', 'withdrawn')) NOT VALID"
    )
    op.execute("ALTER TABLE assertions VALIDATE CONSTRAINT ck_assertion_state")
    op.drop_constraint("fk_assertions_extraction_run", "assertions", type_="foreignkey")
    op.drop_column("assertions", "extraction_run_id")
    op.alter_column("assertions", "run_key", new_column_name="extraction_run_id")

    op.drop_column("candidate_claims", "needs_review")
    op.drop_column("candidate_claims", "quote_hash")
    op.drop_column("candidate_claims", "quote_end")
    op.drop_column("candidate_claims", "quote_start")
    op.drop_column("candidate_claims", "source_quote")
    op.drop_constraint("fk_candidate_claims_chunk", "candidate_claims", type_="foreignkey")
    op.drop_column("candidate_claims", "chunk_id")
    op.drop_constraint("fk_candidate_claims_extraction_run", "candidate_claims", type_="foreignkey")
    op.drop_column("candidate_claims", "extraction_run_id")

    op.drop_table("extraction_runs")
