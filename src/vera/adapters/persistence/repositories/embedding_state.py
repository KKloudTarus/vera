"""Per-group embedding fingerprint store.

Runs on the worker's trusted session (RLS bypassed) inside the per-group advisory lock,
so a read-then-write needs no extra locking. Enforces one embedding dimension per group.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.ops import GroupEmbeddingStateRow
from vera.application.ingestion.embedding_guard import EmbeddingFingerprint, reconcile


class SqlAlchemyEmbeddingStateRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def ensure_compatible(self, *, group_id: str, model: str, dim: int) -> None:
        """Record the fingerprint for a fresh group, accept a match, or raise VeraError on
        a model/dimension change.
        """
        row = await self._session.get(GroupEmbeddingStateRow, group_id)
        existing = (
            EmbeddingFingerprint(model=row.embedding_model, dim=row.embedding_dim)
            if row is not None
            else None
        )
        if reconcile(existing, EmbeddingFingerprint(model=model, dim=dim)) == "initialize":
            self._session.add(
                GroupEmbeddingStateRow(group_id=group_id, embedding_model=model, embedding_dim=dim)
            )
            await self._session.flush()

    async def get(self, group_id: str) -> EmbeddingFingerprint | None:
        row = (
            await self._session.execute(
                select(GroupEmbeddingStateRow).where(GroupEmbeddingStateRow.group_id == group_id)
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        return EmbeddingFingerprint(model=row.embedding_model, dim=row.embedding_dim)
