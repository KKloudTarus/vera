"""Persisted rerank weights: calibration saves an active set, startup loads it."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories.rerank_weights import (
    SqlAlchemyRerankWeightsRepository,
)
from vera.application.queries.search_memory import RerankWeights
from vera.bootstrap import refresh_rerank_weights

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_active_weights_round_trip(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    repo = SqlAlchemyRerankWeightsRepository(sessionmaker)
    assert await repo.get_active() is None  # nothing persisted yet

    weights = RerankWeights(
        relevance=0.5, authority=0.3, verification=0.1, recency=0.05, feedback=0.03, confidence=0.02
    )
    await repo.save_active(weights, sample_count=42)
    loaded = await repo.get_active()
    assert loaded is not None
    assert loaded.relevance == 0.5
    assert loaded.authority == 0.3


async def test_refresh_adopts_the_active_weights(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: object,
) -> None:
    from vera.adapters.graph.null import NullMemoryEngine

    container = make_container(NullMemoryEngine())  # type: ignore[operator]
    before = container.rerank_weights.authority

    await SqlAlchemyRerankWeightsRepository(sessionmaker).save_active(
        RerankWeights(
            relevance=0.1,
            authority=0.6,
            verification=0.1,
            recency=0.1,
            feedback=0.05,
            confidence=0.05,
        ),
        sample_count=30,
    )
    await refresh_rerank_weights(container)
    assert container.rerank_weights.authority == 0.6
    assert container.rerank_weights.authority != before
