"""Persistence for append-only personal proposal attempt reporting."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.ops import ProposalAttemptRow
from vera.shared.ids import uuid7
from vera.shared.types import JsonDict


class SqlAlchemyProposalAttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(
        self,
        *,
        group_id: str,
        principal_id: UUID,
        run_key: str,
        fact_key: str | None,
        proposal_ref: UUID | None,
        outcome: str,
        operation: str,
        context: JsonDict,
        detail: JsonDict | None = None,
    ) -> None:
        self._session.add(
            ProposalAttemptRow(
                id=uuid7(),
                group_id=group_id,
                principal_id=principal_id,
                run_key=run_key,
                fact_key=fact_key,
                proposal_ref=proposal_ref,
                outcome=outcome,
                operation=operation,
                context=dict(context),
                detail=dict(detail or {}),
            )
        )
        await self._session.flush()

    async def count_created_since(
        self, *, group_id: str, principal_id: UUID, since: datetime
    ) -> int:
        count = await self._session.scalar(
            select(func.count(func.distinct(ProposalAttemptRow.fact_key))).where(
                ProposalAttemptRow.group_id == group_id,
                ProposalAttemptRow.principal_id == principal_id,
                ProposalAttemptRow.outcome == "created",
                ProposalAttemptRow.created_at >= since,
            )
        )
        return int(count or 0)
