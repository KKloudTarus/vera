"""Authoritative per-fact lineage for derived graph communities."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, ForeignKeyConstraint, Index, String, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from vera.adapters.persistence.base import Base


class CommunityFactLineageRow(Base):
    __tablename__ = "community_fact_lineage"

    community_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    fact_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    derivation_run_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["fact_id", "group_id"],
            ["facts.id", "facts.group_id"],
            ondelete="CASCADE",
            name="fk_community_lineage_fact",
        ),
        Index(
            "ix_community_lineage_group_community_run",
            "group_id",
            "community_id",
            "derivation_run_id",
        ),
        Index("ix_community_lineage_fact", "fact_id"),
    )
