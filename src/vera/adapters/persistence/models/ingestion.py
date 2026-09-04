"""Ingestion queue table (the transactional outbox consumed by the worker)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from vera.adapters.persistence.base import Base


class IngestionJobRow(Base):
    """Transactional outbox / ingestion queue (see the ``JobQueue`` port)."""

    __tablename__ = "ingestion_jobs"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("uuidv7()")
    )
    group_id: Mapped[str] = mapped_column(String(256), nullable=False)
    source_id: Mapped[str] = mapped_column(String(512), nullable=False)
    dedup_uuid: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    trace_context: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="8")
    provider_retry_fenced: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    claim_token: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_visible_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("dedup_uuid", name="uq_jobs_dedup"),
        CheckConstraint("status IN ('pending','inflight','done','dead')", name="ck_jobs_status"),
        Index(
            "ix_jobs_claimable",
            "group_id",
            "next_visible_at",
            "id",
            postgresql_where=text("status = 'pending'"),
        ),
    )
