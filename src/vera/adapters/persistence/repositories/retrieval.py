"""Retrieval read model and feedback repository.

The read model runs as the trusted read role and scopes every query to the
principal's allowed group_ids explicitly, so it can enrich across several scopes in
one batched query (which a single RLS tenant setting could not span). The feedback
repository writes under the request's tenant.
"""

from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.canonical import GraphEdgeMapRow
from vera.adapters.persistence.models.knowledge import PublishedEpisodeRow
from vera.adapters.persistence.models.ops import RetrievalFeedbackRow
from vera.domain.ports.retrieval import EpisodeProvenance, HitProvenance, RecentChange


def _as_uuids(values: Sequence[str]) -> list[UUID]:
    result: list[UUID] = []
    for value in values:
        try:
            result.append(UUID(value))
        except (ValueError, AttributeError):
            continue
    return result


class SqlAlchemyRetrievalReadModel:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def enrich(
        self, *, group_ids: Sequence[str], edge_uuids: Sequence[str]
    ) -> dict[str, HitProvenance]:
        uuids = _as_uuids(edge_uuids)
        if not uuids or not group_ids:
            return {}
        stmt = (
            select(
                GraphEdgeMapRow.edge_uuid,
                PublishedEpisodeRow.verification,
                PublishedEpisodeRow.authority,
                PublishedEpisodeRow.source_id,
            )
            .join(
                PublishedEpisodeRow,
                PublishedEpisodeRow.id == GraphEdgeMapRow.published_episode_id,
            )
            .where(
                GraphEdgeMapRow.group_id.in_(list(group_ids)),
                GraphEdgeMapRow.edge_uuid.in_(uuids),
            )
        )
        async with self._session_factory() as session:
            rows = (await session.execute(stmt)).all()
        return {
            str(edge_uuid): HitProvenance(
                edge_uuid=str(edge_uuid),
                verification=verification,
                authority=authority,
                source_id=source_id,
            )
            for edge_uuid, verification, authority, source_id in rows
        }

    async def feedback_counts(
        self, *, group_ids: Sequence[str], refs: Sequence[str]
    ) -> dict[str, tuple[int, int]]:
        if not refs or not group_ids:
            return {}
        up = func.count().filter(RetrievalFeedbackRow.signal == "up")
        down = func.count().filter(RetrievalFeedbackRow.signal == "down")
        stmt = (
            select(RetrievalFeedbackRow.result_ref, up, down)
            .where(
                RetrievalFeedbackRow.group_id.in_(list(group_ids)),
                RetrievalFeedbackRow.result_ref.in_(list(refs)),
            )
            .group_by(RetrievalFeedbackRow.result_ref)
        )
        async with self._session_factory() as session:
            rows = (await session.execute(stmt)).all()
        return {ref: (int(up_count), int(down_count)) for ref, up_count, down_count in rows}

    async def recent_changes(self, *, group_ids: Sequence[str], limit: int) -> list[RecentChange]:
        if not group_ids:
            return []
        stmt = (
            select(
                PublishedEpisodeRow.source_id,
                PublishedEpisodeRow.group_id,
                PublishedEpisodeRow.knowledge_type,
                PublishedEpisodeRow.verification,
                PublishedEpisodeRow.reference_time,
            )
            .where(PublishedEpisodeRow.group_id.in_(list(group_ids)))
            .order_by(PublishedEpisodeRow.reference_time.desc())
            .limit(limit)
        )
        async with self._session_factory() as session:
            rows = (await session.execute(stmt)).all()
        return [
            RecentChange(
                source_id=source_id,
                group_id=group_id,
                knowledge_type=knowledge_type,
                verification=verification,
                reference_time=reference_time,
            )
            for source_id, group_id, knowledge_type, verification, reference_time in rows
        ]

    async def get_source(
        self, *, group_ids: Sequence[str], source_id: str
    ) -> EpisodeProvenance | None:
        if not group_ids:
            return None
        stmt = select(PublishedEpisodeRow).where(
            PublishedEpisodeRow.group_id.in_(list(group_ids)),
            PublishedEpisodeRow.source_id == source_id,
        )
        async with self._session_factory() as session:
            row = (await session.execute(stmt)).scalars().first()
        if row is None:
            return None
        return EpisodeProvenance(
            source_id=row.source_id,
            group_id=row.group_id,
            knowledge_type=row.knowledge_type,
            verification=row.verification,
            authority=row.authority,
            reference_time=row.reference_time,
            payload=row.payload,
        )


class SqlAlchemyRetrievalFeedbackRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(
        self,
        *,
        group_id: str,
        principal_id: UUID | None,
        query: str,
        result_ref: str,
        signal: str,
        weight: float = 1.0,
    ) -> None:
        self._session.add(
            RetrievalFeedbackRow(
                group_id=group_id,
                principal_id=principal_id,
                query=query,
                result_ref=result_ref,
                signal=signal,
                weight=weight,
            )
        )
        await self._session.flush()
