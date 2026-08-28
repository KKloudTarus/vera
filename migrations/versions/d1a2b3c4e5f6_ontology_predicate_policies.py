"""ontology governance: persist per-predicate policies with the ontology version

Adds ``ontology_versions.predicate_policies`` so the reconciliation rules (cardinality,
absence semantics, conflict strategy) are versioned alongside the entity and edge types, and
backfills the version 1 row with the policies the code shipped. From here a process fails fast
at startup if the code registry and the persisted row disagree, instead of the two silently
drifting.

Expand only: the column is added with a default, then the existing v1 row is populated in the
same migration. Re-running is safe because the backfill only touches a row still at the empty
default.

Revision ID: d1a2b3c4e5f6
Revises: c9e2f3a4b5d6
Create Date: 2026-08-28
"""

from __future__ import annotations

import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d1a2b3c4e5f6"
down_revision: str | None = "c9e2f3a4b5d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The governance policies for ontology version 1, frozen here so the row matches what the code
# emits for v1. A later ontology change ships a new version row with its own policies; changing
# v1's policies in code without a matching migration is caught by the startup drift check.
_MULTI = {
    "cardinality": "multi",
    "absence_semantics": "retract",
    "conflict_strategy": "higher_authority_then_review",
}
_SINGLE = {
    "cardinality": "one_per_qualifier_set",
    "absence_semantics": "retract",
    "conflict_strategy": "higher_authority_then_review",
}
_V1_POLICIES = {
    "CAUSED": _MULTI,
    "DECIDED_BY": _MULTI,
    "DEPENDS_ON": _MULTI,
    "DEPLOYED_TO": _SINGLE,
    "HAS_STATUS": _SINGLE,
    "MEMBER_OF": _MULTI,
    "OWNS": _MULTI,
    "RUNS_ON": _SINGLE,
}


def upgrade() -> None:
    op.add_column(
        "ontology_versions",
        sa.Column(
            "predicate_policies",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.execute(
        sa.text(
            "UPDATE ontology_versions SET predicate_policies = CAST(:policies AS jsonb) "
            "WHERE version = 1 AND predicate_policies = '{}'::jsonb"
        ).bindparams(policies=json.dumps(_V1_POLICIES))
    )


def downgrade() -> None:
    op.drop_column("ontology_versions", "predicate_policies")
