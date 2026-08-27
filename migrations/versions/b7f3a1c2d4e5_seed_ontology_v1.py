"""seed ontology version 1 (vera-core)

Records the initial ontology so published episodes can reference an active ontology
version. The type names mirror ``vera.domain.ontology.registry`` at v1; they are seeded
here (not imported) so the migration stays self-contained.

Revision ID: b7f3a1c2d4e5
Revises: dc5ccddfa2ca
Create Date: 2026-08-27 11:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "b7f3a1c2d4e5"
down_revision: str | None = "dc5ccddfa2ca"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ENTITY_TYPES = [
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
_EDGE_TYPES = ["RUNS_ON", "DEPENDS_ON", "OWNS", "DEPLOYED_TO", "MEMBER_OF", "CAUSED", "DECIDED_BY"]


def upgrade() -> None:
    entity_json = '{"types": [' + ", ".join(f'"{t}"' for t in _ENTITY_TYPES) + "]}"
    edge_json = '{"types": [' + ", ".join(f'"{t}"' for t in _EDGE_TYPES) + "]}"
    op.execute(
        f"""
        INSERT INTO ontology_versions (version, name, entity_types, edge_types)
        VALUES (1, 'vera-core', '{entity_json}'::jsonb, '{edge_json}'::jsonb)
        ON CONFLICT (version) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute("DELETE FROM ontology_versions WHERE version = 1")
