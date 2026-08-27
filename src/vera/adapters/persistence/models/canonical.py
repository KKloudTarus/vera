"""Canonical registry: the semantic layer that stitches graph fragments.

Graphiti resolves entities only within one group_id, so the same real-world entity
becomes a separate node per scope. VERA keeps the canonical identity here and maps
each Graphiti node and edge uuid back to it, which also gives retraction a target.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Computed,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from vera.adapters.persistence.base import Base
from vera.adapters.persistence.models._mixins import UUIDPK, Timestamps

# Immutable normalization so it can back a generated column: lowercase, replace any
# run of non-alphanumerics with a single space, trim. Accent folding happens in the
# app layer before write, since unaccent() is not IMMUTABLE.
_NORM = "lower(btrim(regexp_replace({col}, '[^a-zA-Z0-9]+', ' ', 'g')))"


class CanonicalEntityRow(Base, UUIDPK, Timestamps):
    __tablename__ = "canonical_entities"

    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(128), nullable=False)
    canonical_name: Mapped[str] = mapped_column(String(512), nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Canonical-name embedding for semantic (cross-lingual/synonym) alias linking.
    # none_as_null so a missing embedding is SQL NULL, not JSON 'null': the backfill
    # query (IS NULL) and the candidate filter (IS NOT NULL) both depend on that.
    name_embedding: Mapped[list[float] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    attributes: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    version_id: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")

    __mapper_args__ = {"version_id_col": version_id}  # noqa: RUF012  SQLAlchemy config dict
    __table_args__ = (Index("ix_canonical_group_type", "group_id", "entity_type"),)


class EntityAliasRow(Base, UUIDPK):
    __tablename__ = "entity_aliases"

    canonical_entity_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("canonical_entities.id", ondelete="CASCADE"),
        nullable=False,
    )
    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    alias: Mapped[str] = mapped_column(String(512), nullable=False)
    alias_norm: Mapped[str] = mapped_column(
        String(512), Computed(_NORM.format(col="alias"), persisted=True)
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        # Exact resolution path: one normalized alias per group.
        UniqueConstraint("group_id", "alias_norm", name="uq_alias_norm"),
        # Fuzzy suggestion path only.
        Index(
            "ix_alias_norm_trgm",
            "alias_norm",
            postgresql_using="gin",
            postgresql_ops={"alias_norm": "gin_trgm_ops"},
        ),
    )


class GraphNodeMapRow(Base, UUIDPK):
    __tablename__ = "graph_node_map"

    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    node_uuid: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    canonical_entity_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("canonical_entities.id", ondelete="SET NULL"),
        nullable=True,
    )
    published_episode_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("published_episodes.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("group_id", "node_uuid", name="uq_node_map"),
        Index("ix_node_map_canonical", "canonical_entity_id"),
    )


class GraphEdgeMapRow(Base, UUIDPK):
    __tablename__ = "graph_edge_map"

    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    edge_uuid: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    published_episode_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("published_episodes.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("group_id", "edge_uuid", name="uq_edge_map"),
        Index("ix_edge_map_episode", "published_episode_id"),
    )
