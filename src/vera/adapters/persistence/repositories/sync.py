"""Sync state store: cursors in ``sync_cursors`` and run outcomes in ``sync_jobs``.

Cursors persist incremental progress per source; job rows record each run's status and
stats. Each method runs in its own short transaction so sync bookkeeping is independent
of the artifact-ingestion transactions it brackets.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.ops import SyncCursorRow, SyncJobRow
from vera.shared.time import utc_now
from vera.shared.types import JsonDict


class SqlAlchemySyncStateStore:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_cursor(self, source_id: UUID) -> JsonDict | None:
        async with self._session_factory() as session:
            cursor = await session.scalar(
                select(SyncCursorRow.cursor).where(SyncCursorRow.source_id == source_id)
            )
        return dict(cursor) if cursor else None

    async def save_cursor(self, source_id: UUID, cursor: JsonDict) -> None:
        async with self._session_factory() as session, session.begin():
            stmt = (
                pg_insert(SyncCursorRow)
                .values(source_id=source_id, cursor=cursor, updated_at=utc_now())
                .on_conflict_do_update(
                    index_elements=[SyncCursorRow.source_id],
                    set_={"cursor": cursor, "updated_at": utc_now()},
                )
            )
            await session.execute(stmt)

    async def start_job(self, source_id: UUID) -> UUID:
        async with self._session_factory() as session, session.begin():
            row = SyncJobRow(source_id=source_id, status="running", started_at=utc_now())
            session.add(row)
            await session.flush()
            return row.id

    async def finish_job(self, job_id: UUID, *, processed: int, unchanged: int) -> None:
        async with self._session_factory() as session, session.begin():
            row = await session.get(SyncJobRow, job_id)
            if row is not None:
                row.status = "succeeded"
                row.finished_at = utc_now()
                row.stats = {"processed": processed, "unchanged": unchanged}

    async def fail_job(self, job_id: UUID, *, error: str) -> None:
        async with self._session_factory() as session, session.begin():
            row = await session.get(SyncJobRow, job_id)
            if row is not None:
                row.status = "failed"
                row.finished_at = utc_now()
                row.error = error[:2000]

    async def last_synced_at(self, source_id: UUID) -> datetime | None:
        async with self._session_factory() as session:
            return await session.scalar(
                select(SyncCursorRow.updated_at).where(SyncCursorRow.source_id == source_id)
            )
