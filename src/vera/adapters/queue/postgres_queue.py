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
from vera.observability.cost import provider_budget_trace_context
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
    WITH terminalized AS (
        UPDATE ingestion_jobs
        SET status = 'dead', locked_until = NULL,
            last_error = COALESCE(last_error, 'provider retry fence prevented reclaim')
        WHERE status = 'pending' AND provider_retry_fenced
        RETURNING id
    ), ready AS (
        SELECT j.id
        FROM ingestion_jobs j
        WHERE j.status = 'pending'
          AND NOT j.provider_retry_fenced
          AND j.next_visible_at <= now()
          AND NOT EXISTS (
              SELECT 1 FROM ingestion_jobs k
              WHERE k.group_id = j.group_id AND k.status = 'inflight'
          )
          AND NOT EXISTS (
              SELECT 1 FROM ingestion_jobs older
              WHERE older.group_id = j.group_id
                AND older.status = 'pending'
                AND NOT older.provider_retry_fenced
                AND (older.created_at, older.id) < (j.created_at, j.id)
          )
        ORDER BY j.next_visible_at, j.created_at, j.id
        FOR UPDATE SKIP LOCKED
        LIMIT :batch
    )
    UPDATE ingestion_jobs u
    SET status = 'inflight',
        attempts = u.attempts + 1,
        completed_at = NULL,
        locked_until = now() + make_interval(secs => :visibility),
        claim_token = uuidv7()
    FROM ready
    WHERE u.id = ready.id
    RETURNING u.id, u.group_id, u.source_id, u.dedup_uuid, u.payload, u.trace_context,
              u.attempts, u.created_at, u.claim_token
    """
)

_COMPLETE = text(
    "UPDATE ingestion_jobs SET status = 'done', last_error = NULL, "
    "completed_at = now(), locked_until = NULL, claim_token = NULL "
    "WHERE id = :id AND claim_token = :claim_token"
)
_RENEW = text(
    "UPDATE ingestion_jobs "
    "SET locked_until = now() + make_interval(secs => :visibility) "
    "WHERE id = :id AND claim_token = :claim_token AND status = 'inflight' "
    "RETURNING id"
)
_DEPTH = text("SELECT status, count(*) FROM ingestion_jobs GROUP BY status")

_FAIL = text(
    """
    UPDATE ingestion_jobs
    SET status = CASE
            WHEN provider_retry_fenced OR attempts >= max_attempts THEN 'dead'
            ELSE 'pending'
        END,
        last_error = :error,
        completed_at = NULL,
        locked_until = NULL,
        claim_token = NULL,
        next_visible_at = now() + make_interval(secs => :retry_in)
    WHERE id = :id AND claim_token = :claim_token
    """
)
_DEAD_LETTER = text(
    "UPDATE ingestion_jobs SET status = 'dead', last_error = :error, completed_at = NULL, "
    "locked_until = NULL, claim_token = NULL WHERE id = :id AND claim_token = :claim_token"
)

_RELEASE = text(
    """
    UPDATE ingestion_jobs
    SET status = CASE WHEN provider_retry_fenced THEN 'dead' ELSE 'pending' END,
        attempts = CASE
            WHEN provider_retry_fenced THEN attempts
            ELSE greatest(attempts - 1, 0)
        END,
        last_error = :reason,
        completed_at = NULL,
        locked_until = NULL,
        claim_token = NULL,
        next_visible_at = now()
    WHERE id = :id AND claim_token = :claim_token AND status = 'inflight'
    """
)

_RECLAIM = text(
    """
    UPDATE ingestion_jobs
    SET status = CASE WHEN provider_retry_fenced THEN 'dead' ELSE 'pending' END,
        completed_at = NULL,
        locked_until = NULL,
        claim_token = NULL,
        next_visible_at = now()
    WHERE status = 'inflight' AND locked_until IS NOT NULL AND locked_until < now()
    """
)


class PostgresJobQueue:
    """Concrete ``JobQueue`` backed by the ``ingestion_jobs`` table."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        worker_session_factory: async_sessionmaker[AsyncSession] | None = None,
        visibility_timeout_s: int = 300,
    ) -> None:
        self._enqueue_session_factory = session_factory
        self._worker_session_factory = worker_session_factory or session_factory
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
        async with self._enqueue_session_factory() as session, session.begin():
            row = (
                await session.execute(
                    _ENQUEUE,
                    {
                        "group_id": str(group_id),
                        "source_id": str(source_id),
                        "dedup_uuid": dedup_uuid,
                        "payload": json.dumps(payload),
                        "trace": json.dumps(provider_budget_trace_context(trace_context)),
                    },
                )
            ).first()
        return row is not None

    async def claim(self, *, batch_size: int) -> Sequence[QueuedJob]:
        async with self._worker_session_factory() as session, session.begin():
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
                claim_token=r["claim_token"],
            )
            for r in rows
        ]

    async def complete(self, job_id: UUID, *, claim_token: UUID) -> None:
        async with self._worker_session_factory() as session, session.begin():
            await session.execute(_COMPLETE, {"id": job_id, "claim_token": claim_token})

    async def renew(self, job_id: UUID, *, claim_token: UUID) -> bool:
        async with self._worker_session_factory() as session, session.begin():
            renewed = await session.scalar(
                _RENEW,
                {
                    "id": job_id,
                    "claim_token": claim_token,
                    "visibility": self._visibility_timeout_s,
                },
            )
        return renewed is not None

    async def fail(self, job_id: UUID, *, claim_token: UUID, error: str, retry_in_s: float) -> None:
        async with self._worker_session_factory() as session, session.begin():
            await session.execute(
                _FAIL,
                {
                    "id": job_id,
                    "claim_token": claim_token,
                    "error": error[:2000],
                    "retry_in": retry_in_s,
                },
            )

    async def dead_letter(self, job_id: UUID, *, claim_token: UUID, error: str) -> None:
        async with self._worker_session_factory() as session, session.begin():
            await session.execute(
                _DEAD_LETTER,
                {"id": job_id, "claim_token": claim_token, "error": error[:2000]},
            )

    async def fence_provider_attempt(self, job_id: UUID, *, claim_token: UUID) -> None:
        async with self._worker_session_factory() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE ingestion_jobs SET provider_retry_fenced=true "
                    "WHERE id=:id AND claim_token=:claim_token AND status='inflight'"
                ),
                {"id": job_id, "claim_token": claim_token},
            )

    async def release(self, job_id: UUID, *, claim_token: UUID, reason: str) -> None:
        async with self._worker_session_factory() as session, session.begin():
            await session.execute(
                _RELEASE,
                {"id": job_id, "claim_token": claim_token, "reason": reason[:2000]},
            )

    async def reclaim_stuck(self) -> int:
        async with self._worker_session_factory() as session, session.begin():
            result = await session.execute(_RECLAIM)
        return max(cast("CursorResult[Any]", result).rowcount, 0)

    async def depth_by_status(self) -> dict[str, int]:
        async with self._worker_session_factory() as session:
            rows = (await session.execute(_DEPTH)).all()
        return {str(status): int(count) for status, count in rows}
