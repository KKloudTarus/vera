"""Outbox repository: inserts ingestion jobs in the current UoW transaction.

Idempotent by ``dedup_uuid`` (``ON CONFLICT DO NOTHING``), so a re-run of the same
source adds nothing. Unlike ``PostgresJobQueue``, this does not open its own
transaction; the write commits with the rest of the unit of work.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.ingestion import IngestionJobRow
from vera.observability.cost import provider_budget_trace_context
from vera.shared.types import JsonDict


class SqlAlchemyOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        *,
        group_id: str,
        source_id: str,
        dedup_uuid: UUID,
        payload: JsonDict,
        trace_context: JsonDict | None = None,
    ) -> None:
        stmt = (
            pg_insert(IngestionJobRow)
            .values(
                group_id=group_id,
                source_id=source_id,
                dedup_uuid=dedup_uuid,
                payload=payload,
                trace_context=provider_budget_trace_context(trace_context),
            )
            .on_conflict_do_nothing(index_elements=["dedup_uuid"])
        )
        await self._session.execute(stmt)
