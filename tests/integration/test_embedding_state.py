"""Per-group embedding fingerprint against the live database."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories.embedding_state import (
    SqlAlchemyEmbeddingStateRepository,
)
from vera.shared.errors import VeraError
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_fingerprint_initializes_then_enforces(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    model = "text-embedding-3-small"

    async with sessionmaker() as session, session.begin():
        repo = SqlAlchemyEmbeddingStateRepository(session)
        await repo.ensure_compatible(group_id=group, model=model, dim=1536)  # first write
        await repo.ensure_compatible(group_id=group, model=model, dim=1536)  # same -> ok
        stored = await repo.get(group)
    assert stored is not None and stored.model == model and stored.dim == 1536

    # A changed dimension is refused until the group is reprocessed.
    async with sessionmaker() as session, session.begin():
        repo = SqlAlchemyEmbeddingStateRepository(session)
        with pytest.raises(VeraError, match="reprocess"):
            await repo.ensure_compatible(group_id=group, model=model, dim=1024)
