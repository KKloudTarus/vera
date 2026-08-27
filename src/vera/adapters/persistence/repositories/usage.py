"""Durable sink for LLM usage events: one row per provider call in ``llm_usage``.

Writes in its own short transaction so metering never rides on the caller's unit of
work (a cost row must not be rolled back with a failed ingest, and must not hold the
ingest transaction open). Cost per episode or per query is then a simple aggregate.
"""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.ops import LlmUsageRow
from vera.observability.cost import UsageEvent


class SqlAlchemyUsageSink:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def record(self, event: UsageEvent) -> None:
        async with self._session_factory() as session, session.begin():
            session.add(
                LlmUsageRow(
                    model=event.model,
                    operation=event.operation,
                    request_kind=event.request_kind,
                    group_id=event.group_id,
                    ref=event.ref,
                    prompt_tokens=event.prompt_tokens,
                    completion_tokens=event.completion_tokens,
                    cost_usd=event.cost_usd,
                )
            )

    async def total_cost_for_group(self, group_id: str) -> float:
        async with self._session_factory() as session:
            total = await session.scalar(
                select(func.coalesce(func.sum(LlmUsageRow.cost_usd), 0.0)).where(
                    LlmUsageRow.group_id == group_id
                )
            )
        return float(total or 0.0)

    async def total_cost_for_ref(self, ref: str) -> float:
        async with self._session_factory() as session:
            total = await session.scalar(
                select(func.coalesce(func.sum(LlmUsageRow.cost_usd), 0.0)).where(
                    LlmUsageRow.ref == ref
                )
            )
        return float(total or 0.0)
