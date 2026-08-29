"""ontology governance v2

Persists typed/qualified/source-aware predicate policies, durable extraction metadata, and
fact freshness deadlines.

Revision ID: e9f0a1b2c3d4
Revises: d8f9a0b1c2d3
Create Date: 2026-08-29
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "e9f0a1b2c3d4"
down_revision: str | None = "d8f9a0b1c2d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _policy(
    *,
    cardinality: str,
    subjects: list[str],
    objects: list[str],
    object_kind: str,
    authority: float = 0.7,
    qualifiers: dict[str, dict[str, Any]] | None = None,
    ttl_seconds: int | None = None,
) -> dict[str, Any]:
    return {
        "cardinality": cardinality,
        "absence_semantics": "retract",
        "conflict_strategy": "higher_authority_then_review",
        "subject_types": subjects,
        "object_types": objects,
        "object_kind": object_kind,
        "qualifier_schema": qualifiers or {},
        "allow_additional_qualifiers": True,
        "minimum_source_authority": authority,
        "ttl_seconds": ttl_seconds,
        "deprecated": False,
        "replacement_predicate": None,
    }


_POLICIES = {
    "CAUSED": _policy(
        cardinality="multi", subjects=["Service"], objects=["Incident"], object_kind="entity"
    ),
    "DECIDED_BY": _policy(
        cardinality="multi",
        subjects=["Person", "Team"],
        objects=["Decision"],
        object_kind="entity",
        authority=0.85,
    ),
    "DEPENDS_ON": _policy(
        cardinality="multi",
        subjects=["Service"],
        objects=["Component", "Datastore", "Service"],
        object_kind="entity",
    ),
    "DEPLOYED_TO": _policy(
        cardinality="one_per_qualifier_set",
        subjects=["Service"],
        objects=["Environment"],
        object_kind="entity",
    ),
    "HAS_STATUS": _policy(
        cardinality="one_per_qualifier_set",
        subjects=["Environment", "Incident", "Service"],
        objects=[],
        object_kind="scalar",
        qualifiers={"environment": {"type": "string", "required": True}},
        ttl_seconds=86_400,
    ),
    "MEMBER_OF": _policy(
        cardinality="multi", subjects=["Person"], objects=["Team"], object_kind="entity"
    ),
    "OWNS": _policy(
        cardinality="multi",
        subjects=["Team"],
        objects=["Repository", "Service"],
        object_kind="entity",
    ),
    "RUNS_ON": _policy(
        cardinality="one_per_qualifier_set",
        subjects=["Service"],
        objects=["Environment"],
        object_kind="entity",
        qualifiers={"environment": {"type": "string", "required": False}},
    ),
}

_EVENT_TYPES_V1 = (
    "ARTIFACT_DISCOVERED",
    "ARTIFACT_CHANGED",
    "ARTIFACT_REMOVED",
    "ASSERTION_ADDED",
    "ASSERTION_REAFFIRMED",
    "ASSERTION_WITHDRAWN",
    "EVIDENCE_ADDED",
    "EVIDENCE_REMOVED",
    "FACT_ACTIVATED",
    "FACT_DISPUTED",
    "FACT_SUPERSEDED",
    "FACT_RETRACTED",
    "FACT_RESTORED",
    "ENTITY_MERGED",
    "ENTITY_SPLIT",
    "ONTOLOGY_CHANGED",
    "SNAPSHOT_CREATED",
    "CONTEXT_PACK_CREATED",
)
_EVENT_TYPES_V2 = (*_EVENT_TYPES_V1, "FACT_EXPIRED")


def _event_type_constraint(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    op.execute("ALTER TABLE knowledge_events DROP CONSTRAINT ck_knowledge_event_type")
    op.execute(
        "ALTER TABLE knowledge_events ADD CONSTRAINT ck_knowledge_event_type "
        f"CHECK (event_type IN ({_event_type_constraint(_EVENT_TYPES_V2)}))"
    )
    op.add_column(
        "candidate_claims", sa.Column("subject_entity_type", sa.String(128), nullable=True)
    )
    op.add_column(
        "candidate_claims", sa.Column("object_entity_type", sa.String(128), nullable=True)
    )
    op.add_column(
        "candidate_claims",
        sa.Column(
            "qualifiers",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column("facts", sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index(
        "ix_facts_expiry",
        "facts",
        ["expires_at"],
        postgresql_where=sa.text("lifecycle_state = 'active' AND expires_at IS NOT NULL"),
    )

    bind = op.get_bind()
    bind.execute(
        sa.text(
            "INSERT INTO ontology_versions "
            "(version, name, entity_types, edge_types, predicate_policies) VALUES "
            "(:version, :name, CAST(:entities AS jsonb), CAST(:edges AS jsonb), "
            "CAST(:policies AS jsonb)) ON CONFLICT (version) DO NOTHING"
        ),
        {
            "version": 2,
            "name": "vera-core",
            "entities": json.dumps(
                {
                    "types": [
                        "Service",
                        "Environment",
                        "Team",
                        "Person",
                        "Repository",
                        "Datastore",
                        "Component",
                        "Incident",
                        "Decision",
                    ]
                }
            ),
            "edges": json.dumps(
                {
                    "types": [
                        "RUNS_ON",
                        "DEPENDS_ON",
                        "OWNS",
                        "DEPLOYED_TO",
                        "MEMBER_OF",
                        "CAUSED",
                        "DECIDED_BY",
                    ]
                }
            ),
            "policies": json.dumps(_POLICIES),
        },
    )


def downgrade() -> None:
    op.execute("DELETE FROM ontology_versions WHERE version = 2")
    op.drop_index("ix_facts_expiry", table_name="facts")
    op.drop_column("facts", "expires_at")
    op.drop_column("candidate_claims", "qualifiers")
    op.drop_column("candidate_claims", "object_entity_type")
    op.drop_column("candidate_claims", "subject_entity_type")
    op.execute("ALTER TABLE knowledge_events DROP CONSTRAINT ck_knowledge_event_type")
    op.execute(
        "ALTER TABLE knowledge_events ADD CONSTRAINT ck_knowledge_event_type "
        f"CHECK (event_type IN ({_event_type_constraint(_EVENT_TYPES_V1)}))"
    )
