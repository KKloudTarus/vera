"""canonical_entities.name_embedding

Stores the canonical name's embedding for semantic (cross-lingual/synonym) alias linking.

Revision ID: f2b3c4d5e6a7
Revises: e1f2a3b4c5d6
Create Date: 2026-08-27 16:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "f2b3c4d5e6a7"
down_revision: str | None = "e1f2a3b4c5d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("canonical_entities", sa.Column("name_embedding", JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("canonical_entities", "name_embedding")
