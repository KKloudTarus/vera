"""Postgres-native ``JobQueue``: transactional outbox with ``FOR UPDATE SKIP LOCKED``.

Vendor-neutral: the source-of-truth database is the queue; no broker required.
``claim`` skips groups that already have an in-flight job and locks rows with SKIP
LOCKED so multiple worker replicas never hand out the same row. The worker holds a
per-group advisory lock while processing, which is the cross-replica guard against
the dedup race; ``reclaim_stuck`` returns timed-out in-flight jobs to pending.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, cast
from uuid import UUID

from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.domain.ports.job_queue import QueuedJob
from vera.shared.types import GroupId, JsonDict, SourceId

_ENQUEUE = text(
    """
    INSERT INTO ingestion_jobs (group_id, source_id, dedup_uuid, payload, trace_context)
    VALUES (:group_id, :source_id, :dedup_uuid, CAST(:payload AS jsonb), CAST(:trace AS jsonb))
    ON CONFLICT (dedup_uuid) DO NOTHING
    RETURNING id
    """
)

_CLAIM = text(
    """
    WITH ready AS (
        SELECT j.id
        FROM ingestion_jobs j
        WHERE j.status = 'pending'
          AND j.next_visible_at <= now()
          AND NOT EXISTS (
              SELECT 1 FROM ingestion_jobs k
              WHERE k.group_id = j.group_id AND k.status = 'inflight'
          )
        ORDER BY j.next_visible_at, j.created_at
        FOR UPDATE SKIP LOCKED
        LIMIT :batch
    )
    UPDATE ingestion_jobs u
    SET status = 'inflight',
        attempts = u.attempts + 1,
        completed_at = NULL,
        locked_until = now() + make_interval(secs => :visibility)
    FROM ready
    WHERE u.id = ready.id
    RETURNING u.id, u.group_id, u.source_id, u.dedup_uuid, u.payload, u.trace_context,
              u.attempts, u.created_at
    """
)

_COMPLETE = text(
    "UPDATE ingestion_jobs SET status = 'done', last_error = NULL, "
    "completed_at = now() WHERE id = :id"
)
_DEPTH = text("SELECT status, count(*) FROM ingestion_jobs GROUP BY status")

_FAIL = text(
    """
    UPDATE ingestion_jobs
    SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
        last_error = :error,
        completed_at = NULL,
        locked_until = NULL,
        next_visible_at = now() + make_interval(secs => :retry_in)
    WHERE id = :id
    """
)

_RELEASE = text(
    """
    UPDATE ingestion_jobs
    SET status = 'pending',
        attempts = greatest(attempts - 1, 0),
        last_error = :reason,
        completed_at = NULL,
        locked_until = NULL,
        next_visible_at = now()
    WHERE id = :id AND status = 'inflight'
    """
)

_RECLAIM = text(
    """
    UPDATE ingestion_jobs
    SET status = 'pending', completed_at = NULL, next_visible_at = now()
    WHERE status = 'inflight' AND locked_until IS NOT NULL AND locked_until < now()
    """
)


class PostgresJobQueue:
    """Concrete ``JobQueue`` backed by the ``ingestion_jobs`` table."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        visibility_timeout_s: int = 300,
    ) -> None:
        self._session_factory = session_factory
        self._visibility_timeout_s = visibility_timeout_s

    async def enqueue(
        self,
        *,
        group_id: GroupId,
        source_id: SourceId,
        dedup_uuid: UUID,
        payload: JsonDict,
        trace_context: JsonDict | None = None,
    ) -> bool:
        async with self._session_factory() as session, session.begin():
            row = (
                await session.execute(
                    _ENQUEUE,
                    {
                        "group_id": str(group_id),
                        "source_id": str(source_id),
                        "dedup_uuid": dedup_uuid,
                        "payload": json.dumps(payload),
                        "trace": json.dumps(trace_context or {}),
                    },
                )
            ).first()
        return row is not None

    async def claim(self, *, batch_size: int) -> Sequence[QueuedJob]:
        async with self._session_factory() as session, session.begin():
            rows = (
                (
                    await session.execute(
                        _CLAIM,
                        {"batch": batch_size, "visibility": self._visibility_timeout_s},
                    )
                )
                .mappings()
                .all()
            )
        return [
            QueuedJob(
                id=r["id"],
                group_id=GroupId(r["group_id"]),
                source_id=SourceId(r["source_id"]),
                dedup_uuid=r["dedup_uuid"],
                payload=r["payload"],
                trace_context=r["trace_context"],
                attempts=r["attempts"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    async def complete(self, job_id: UUID) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(_COMPLETE, {"id": job_id})

    async def fail(self, job_id: UUID, *, error: str, retry_in_s: float) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                _FAIL, {"id": job_id, "error": error[:2000], "retry_in": retry_in_s}
            )

    async def release(self, job_id: UUID, *, reason: str) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(_RELEASE, {"id": job_id, "reason": reason[:2000]})

    async def reclaim_stuck(self) -> int:
        async with self._session_factory() as session, session.begin():
            result = await session.execute(_RECLAIM)
        return max(cast("CursorResult[Any]", result).rowcount, 0)

    async def depth_by_status(self) -> dict[str, int]:
        async with self._session_factory() as session:
            rows = (await session.execute(_DEPTH)).all()
        return {str(status): int(count) for status, count in rows}
