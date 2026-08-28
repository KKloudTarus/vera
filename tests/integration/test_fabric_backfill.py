"""Backfill from published_episodes into the fact model (Phase 8), against the live database.

Proves the migration converts structured episodes into Facts/Assertions/Evidence, flags
free-text episodes for review instead of inventing provenance, preserves the legacy rows, and
is idempotent (a second run does not duplicate).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.models.knowledge import PublishedEpisodeRow
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.entrypoints.migration import FabricBackfillService
from vera.shared.ids import uuid7
from vera.shared.time import utc_now

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


async def _seed_episodes(sessionmaker: async_sessionmaker[AsyncSession], group: str) -> None:
    async with _tenant(sessionmaker, group) as s:
        for subj, pred, obj in (
            ("paymentapi", "RUNS_ON", "eks"),
            ("billing", "DEPENDS_ON", "postgres"),
        ):
            s.add(
                PublishedEpisodeRow(
                    source_id=f"{group}:{uuid7()}",
                    group_id=group,
                    knowledge_type="fact_triple",
                    verification="verified",
                    authority=1.0,
                    confidence=0.9,
                    reference_time=utc_now(),
                    payload={"triples": [{"subject": subj, "predicate": pred, "object": obj}]},
                    dedup_uuid=uuid7(),
                )
            )
        # A free-text episode with no structured triple: must be flagged, not fabricated.
        s.add(
            PublishedEpisodeRow(
                source_id=f"{group}:{uuid7()}",
                group_id=group,
                knowledge_type="text",
                verification="verified",
                authority=0.7,
                confidence=0.8,
                reference_time=utc_now(),
                payload={"body": "some prose without a triple"},
                dedup_uuid=uuid7(),
            )
        )


async def _counts(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> tuple[int, int, int]:
    async with _tenant(sessionmaker, group) as s:
        facts = await s.scalar(text("SELECT count(*) FROM facts WHERE group_id = :g"), {"g": group})
        assertions = await s.scalar(
            text("SELECT count(*) FROM assertions WHERE group_id = :g"), {"g": group}
        )
        evidence = await s.scalar(
            text("SELECT count(*) FROM evidence WHERE group_id = :g"), {"g": group}
        )
    return facts, assertions, evidence


async def _run_backfill(sessionmaker: async_sessionmaker[AsyncSession], group: str):
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        report = await FabricBackfillService(uow.session).backfill_group(group_id=group)
        await uow.commit()
    return report


async def test_backfill_converts_and_flags_and_is_idempotent(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:m-{uuid7().hex[:12]}"
    await _seed_episodes(sessionmaker, group)

    report = await _run_backfill(sessionmaker, group)
    assert report.episodes_processed == 3
    assert report.facts_created == 2
    assert report.assertions_created == 2
    assert report.needs_review == 1  # the free-text episode

    assert await _counts(sessionmaker, group) == (2, 2, 2)

    # The legacy rows are preserved, so the old read path keeps working during the transition.
    async with _tenant(sessionmaker, group) as s:
        episodes = await s.scalar(
            text("SELECT count(*) FROM published_episodes WHERE group_id = :g"), {"g": group}
        )
    assert episodes == 3

    # A second run converges: no duplicate facts, assertions, or evidence.
    await _run_backfill(sessionmaker, group)
    assert await _counts(sessionmaker, group) == (2, 2, 2)

    # Verification reports the mapping.
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        counts = await FabricBackfillService(uow.session).verify_group(group_id=group)
    assert counts == {"episodes": 3, "facts": 2, "assertions": 2}
