"""Store for calibrated rerank weights.

Calibration writes an active weight set here; the ranker loads the latest active one at
startup in place of the configured defaults. Runs on its own short transaction, since it
is a control-plane write outside any request's unit of work.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.ops import RerankWeightsRow
from vera.application.queries.search_memory import RerankWeights


def _to_weights(row: RerankWeightsRow) -> RerankWeights:
    return RerankWeights(
        relevance=row.w_relevance,
        authority=row.w_authority,
        verification=row.w_verification,
        recency=row.w_recency,
        feedback=row.w_feedback,
        confidence=row.w_confidence,
        half_life_s=row.half_life_s,
    )


class SqlAlchemyRerankWeightsRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_active(self) -> RerankWeights | None:
        stmt = (
            select(RerankWeightsRow)
            .where(RerankWeightsRow.active.is_(True))
            .order_by(RerankWeightsRow.created_at.desc())
            .limit(1)
        )
        async with self._session_factory() as session:
            row = (await session.execute(stmt)).scalars().first()
        return _to_weights(row) if row is not None else None

    async def save_active(self, weights: RerankWeights, *, sample_count: int) -> None:
        async with self._session_factory() as session, session.begin():
            session.add(
                RerankWeightsRow(
                    w_relevance=weights.relevance,
                    w_authority=weights.authority,
                    w_verification=weights.verification,
                    w_recency=weights.recency,
                    w_feedback=weights.feedback,
                    w_confidence=weights.confidence,
                    half_life_s=weights.half_life_s,
                    sample_count=sample_count,
                    active=True,
                )
            )
