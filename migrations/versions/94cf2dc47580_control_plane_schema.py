"""control plane schema

Revision ID: 94cf2dc47580
Revises: 388ab7a6550d
Create Date: 2026-08-27 07:31:55.287092+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "94cf2dc47580"
down_revision: str | None = "388ab7a6550d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # pg_trgm backs the fuzzy alias index; create it before that index is built.
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.create_table(
        "audit_events",
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("actor", sa.String(length=256), nullable=True),
        sa.Column("group_id", sa.String(length=256), nullable=True),
        sa.Column("action", sa.String(length=128), nullable=False),
        sa.Column("target", sa.String(length=512), nullable=True),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", "occurred_at", name="pk_audit_events"),
        postgresql_partition_by="RANGE (occurred_at)",
    )
    op.create_index(
        "ix_audit_group_time", "audit_events", ["group_id", "occurred_at"], unique=False
    )
    op.create_table(
        "canonical_entities",
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("entity_type", sa.String(length=128), nullable=False),
        sa.Column("canonical_name", sa.String(length=512), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column(
            "attributes",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("version_id", sa.Integer(), server_default="1", nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_canonical_group_type", "canonical_entities", ["group_id", "entity_type"], unique=False
    )
    op.create_table(
        "ontology_versions",
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column(
            "entity_types",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "edge_types",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("version"),
    )
    op.create_table(
        "organizations",
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_table(
        "principals",
        sa.Column("kind", sa.String(length=32), server_default="user", nullable=False),
        sa.Column("email", sa.String(length=320), nullable=True),
        sa.Column("display_name", sa.String(length=256), nullable=False),
        sa.Column("personal_group_id", sa.String(length=256), nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
        sa.UniqueConstraint("personal_group_id"),
    )
    op.create_table(
        "retrieval_feedback",
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("principal_id", sa.UUID(), nullable=True),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("result_ref", sa.String(length=512), nullable=False),
        sa.Column("signal", sa.String(length=8), nullable=False),
        sa.Column("weight", sa.Float(), server_default="1.0", nullable=False),
        sa.CheckConstraint("signal IN ('up','down')", name="ck_feedback_signal"),
        sa.PrimaryKeyConstraint("id", "created_at", name="pk_retrieval_feedback"),
        postgresql_partition_by="RANGE (created_at)",
    )
    op.create_index(
        "ix_feedback_group_time", "retrieval_feedback", ["group_id", "created_at"], unique=False
    )
    op.create_table(
        "entity_aliases",
        sa.Column("canonical_entity_id", sa.UUID(), nullable=False),
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("alias", sa.String(length=512), nullable=False),
        sa.Column(
            "alias_norm",
            sa.String(length=512),
            sa.Computed(
                "lower(btrim(regexp_replace(alias, '[^a-zA-Z0-9]+', ' ', 'g')))", persisted=True
            ),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["canonical_entity_id"], ["canonical_entities.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_id", "alias_norm", name="uq_alias_norm"),
    )
    op.create_index(
        "ix_alias_norm_trgm",
        "entity_aliases",
        ["alias_norm"],
        unique=False,
        postgresql_using="gin",
        postgresql_ops={"alias_norm": "gin_trgm_ops"},
    )
    op.create_table(
        "workspaces",
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_id"),
        sa.UniqueConstraint("org_id", "slug", name="uq_workspace_slug"),
    )
    op.create_table(
        "projects",
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("slug", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["workspace_id"], ["workspaces.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_id"),
        sa.UniqueConstraint("workspace_id", "slug", name="uq_project_slug"),
    )
    op.create_table(
        "service_accounts",
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("owner_principal_id", sa.UUID(), nullable=False),
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("description", sa.String(length=512), nullable=True),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["owner_principal_id"], ["principals.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["workspace_id"], ["workspaces.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "credentials",
        sa.Column("principal_id", sa.UUID(), nullable=True),
        sa.Column("service_account_id", sa.UUID(), nullable=True),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("key_prefix", sa.String(length=64), nullable=False),
        sa.Column("hashed_secret", sa.String(length=256), nullable=False),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("kind IN ('api_key', 'oauth')", name="ck_credential_kind"),
        sa.CheckConstraint(
            "(principal_id IS NOT NULL)::int + (service_account_id IS NOT NULL)::int = 1",
            name="ck_credential_one_owner",
        ),
        sa.ForeignKeyConstraint(["principal_id"], ["principals.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["service_account_id"], ["service_accounts.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key_prefix"),
    )
    op.create_table(
        "knowledge_sources",
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("project_id", sa.UUID(), nullable=True),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column(
            "config",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("trust_tier", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "kind IN ('git', 'confluence', 'jira', 'cmdb', 'slack', 'pdf', 'agent')",
            name="ck_source_kind",
        ),
        sa.CheckConstraint("trust_tier BETWEEN 1 AND 4", name="ck_source_trust_tier"),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["workspace_id"], ["workspaces.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "memberships",
        sa.Column("principal_id", sa.UUID(), nullable=False),
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("project_id", sa.UUID(), nullable=True),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "role IN ('owner', 'admin', 'member', 'viewer')", name="ck_membership_role"
        ),
        sa.ForeignKeyConstraint(["principal_id"], ["principals.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["workspace_id"], ["workspaces.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "principal_id", "workspace_id", "project_id", name="uq_membership_scope"
        ),
    )
    op.create_table(
        "artifacts",
        sa.Column("source_id", sa.UUID(), nullable=False),
        sa.Column("external_id", sa.String(length=512), nullable=False),
        sa.Column("title", sa.String(length=1024), nullable=True),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("s3_key", sa.String(length=1024), nullable=False),
        sa.Column("reference_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("current_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["source_id"], ["knowledge_sources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_id", "external_id", name="uq_artifact_external"),
    )
    op.create_table(
        "sync_cursors",
        sa.Column("source_id", sa.UUID(), nullable=False),
        sa.Column(
            "cursor",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.ForeignKeyConstraint(["source_id"], ["knowledge_sources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_id"),
    )
    op.create_table(
        "sync_jobs",
        sa.Column("source_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.String(length=16), server_default="pending", nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "stats",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending','running','succeeded','failed')", name="ck_sync_status"
        ),
        sa.ForeignKeyConstraint(["source_id"], ["knowledge_sources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_sync_jobs_source", "sync_jobs", ["source_id", "created_at"], unique=False)
    op.create_table(
        "artifact_versions",
        sa.Column("artifact_id", sa.UUID(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("s3_key", sa.String(length=1024), nullable=False),
        sa.Column("reference_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.ForeignKeyConstraint(["artifact_id"], ["artifacts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("artifact_id", "version", name="uq_artifact_version"),
    )
    op.create_table(
        "candidate_claims",
        sa.Column("artifact_version_id", sa.UUID(), nullable=False),
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("statement", sa.Text(), nullable=False),
        sa.Column("claim_type", sa.String(length=32), nullable=False),
        sa.Column(
            "verification_status", sa.String(length=32), server_default="unverified", nullable=False
        ),
        sa.Column("subject", sa.String(length=512), nullable=True),
        sa.Column("predicate", sa.String(length=256), nullable=True),
        sa.Column("object", sa.String(length=512), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("version_id", sa.Integer(), server_default="1", nullable=False),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "claim_type IN ('fact', 'requirement', 'decision', 'procedure', 'hypothesis', 'proposal')",
            name="ck_claim_type",
        ),
        sa.CheckConstraint(
            "verification_status IN ('unverified', 'pending', 'verified', 'disputed')",
            name="ck_claim_status",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_version_id"], ["artifact_versions.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "published_episodes",
        sa.Column("source_id", sa.String(length=512), nullable=False),
        sa.Column("artifact_version_id", sa.UUID(), nullable=True),
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("knowledge_type", sa.String(length=64), nullable=False),
        sa.Column("verification", sa.String(length=64), nullable=False),
        sa.Column("reference_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("ontology_version_id", sa.UUID(), nullable=True),
        sa.Column(
            "pipeline",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("dedup_uuid", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["artifact_version_id"], ["artifact_versions.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["ontology_version_id"], ["ontology_versions.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dedup_uuid", name="uq_episode_dedup"),
        sa.UniqueConstraint("group_id", "source_id", name="uq_episode_group_source"),
    )
    op.create_table(
        "graph_edge_map",
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("edge_uuid", sa.UUID(), nullable=False),
        sa.Column("published_episode_id", sa.UUID(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["published_episode_id"], ["published_episodes.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_id", "edge_uuid", name="uq_edge_map"),
    )
    op.create_index("ix_edge_map_episode", "graph_edge_map", ["published_episode_id"], unique=False)
    op.create_table(
        "graph_node_map",
        sa.Column("group_id", sa.String(length=256), nullable=False),
        sa.Column("node_uuid", sa.UUID(), nullable=False),
        sa.Column("canonical_entity_id", sa.UUID(), nullable=True),
        sa.Column("published_episode_id", sa.UUID(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["canonical_entity_id"], ["canonical_entities.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["published_episode_id"], ["published_episodes.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_id", "node_uuid", name="uq_node_map"),
    )
    op.create_index(
        "ix_node_map_canonical", "graph_node_map", ["canonical_entity_id"], unique=False
    )
    op.create_table(
        "reviews",
        sa.Column("candidate_claim_id", sa.UUID(), nullable=False),
        sa.Column("reviewer_principal_id", sa.UUID(), nullable=True),
        sa.Column("decision", sa.String(length=32), nullable=False),
        sa.Column("authority", sa.String(length=128), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), server_default=sa.text("uuidv7()"), nullable=False),
        sa.CheckConstraint(
            "decision IN ('approve', 'reject', 'needs_changes')", name="ck_review_decision"
        ),
        sa.ForeignKeyConstraint(
            ["candidate_claim_id"], ["candidate_claims.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["reviewer_principal_id"], ["principals.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )

    # Default partitions so inserts work immediately. Monthly partitions and
    # retention come from pg_partman in a later phase.
    op.execute("CREATE TABLE audit_events_default PARTITION OF audit_events DEFAULT")
    op.execute("CREATE TABLE retrieval_feedback_default PARTITION OF retrieval_feedback DEFAULT")

    # Row-level security: tenant isolation by group_id. Callers set the tenant per
    # transaction with SET LOCAL vera.group_id (see SqlAlchemyUnitOfWork.use_tenant).
    _rls_tables = (
        "candidate_claims",
        "published_episodes",
        "canonical_entities",
        "entity_aliases",
        "graph_node_map",
        "graph_edge_map",
        "retrieval_feedback",
    )
    for table in _rls_tables:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY tenant_isolation ON {table} "
            "USING (group_id = current_setting('vera.group_id', true)) "
            "WITH CHECK (group_id = current_setting('vera.group_id', true))"
        )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("reviews")
    op.drop_index("ix_node_map_canonical", table_name="graph_node_map")
    op.drop_table("graph_node_map")
    op.drop_index("ix_edge_map_episode", table_name="graph_edge_map")
    op.drop_table("graph_edge_map")
    op.drop_table("published_episodes")
    op.drop_table("candidate_claims")
    op.drop_table("artifact_versions")
    op.drop_index("ix_sync_jobs_source", table_name="sync_jobs")
    op.drop_table("sync_jobs")
    op.drop_table("sync_cursors")
    op.drop_table("artifacts")
    op.drop_table("memberships")
    op.drop_table("knowledge_sources")
    op.drop_table("credentials")
    op.drop_table("service_accounts")
    op.drop_table("projects")
    op.drop_table("workspaces")
    op.drop_index(
        "ix_alias_norm_trgm",
        table_name="entity_aliases",
        postgresql_using="gin",
        postgresql_ops={"alias_norm": "gin_trgm_ops"},
    )
    op.drop_table("entity_aliases")
    op.drop_index("ix_feedback_group_time", table_name="retrieval_feedback")
    op.drop_table("retrieval_feedback")
    op.drop_table("principals")
    op.drop_table("organizations")
    op.drop_table("ontology_versions")
    op.drop_index("ix_canonical_group_type", table_name="canonical_entities")
    op.drop_table("canonical_entities")
    op.drop_index("ix_audit_group_time", table_name="audit_events")
    op.drop_table("audit_events")
    # ### end Alembic commands ###
