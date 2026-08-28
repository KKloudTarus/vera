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


async def test_backfill_links_edge_objects_as_entities(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    """gap 17: an edge-predicate triple's object becomes a canonical entity (the object side of
    the graph edge is reconstructed), not a scalar string.
    """
    group = f"p:oe-{uuid7().hex[:12]}"
    async with _tenant(sessionmaker, group) as s:
        s.add(
            PublishedEpisodeRow(
                source_id=f"{group}:{uuid7()}",
                group_id=group,
                knowledge_type="fact_triple",
                verification="verified",
                authority=1.0,
                confidence=0.9,
                reference_time=utc_now(),
                payload={
                    "triples": [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}]
                },
                dedup_uuid=uuid7(),
            )
        )
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        await FabricBackfillService(uow.session).backfill_group(group_id=group)
        await uow.commit()

    async with _tenant(sessionmaker, group) as s:
        row = (
            (
                await s.execute(
                    text(
                        "SELECT f.object_type, f.object_scalar, co.canonical_name AS obj_name "
                        "FROM facts f "
                        "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
                        "WHERE f.group_id = :g AND f.predicate = 'RUNS_ON'"
                    ),
                    {"g": group},
                )
            )
            .mappings()
            .one()
        )
    assert row["object_type"] == "entity"  # not a scalar
    assert row["object_scalar"] is None
    assert row["obj_name"] == "eks"  # the object resolved to the eks canonical entity


class _FakeExtractor:
    """A deterministic stand-in for the LLM extractor: turns known prose into one triple."""

    @property
    def provider(self) -> str:
        return "test"

    @property
    def model(self) -> str:
        return "backfill"

    async def extract(self, *, body, knowledge_type, metadata):
        from vera.domain.ports.curation import ExtractedClaim

        if "runs on" in body.lower():
            return [
                ExtractedClaim(
                    statement=body, subject="checkout", predicate="RUNS_ON", object="gke"
                )
            ]
        return []


async def test_backfill_reextracts_free_text_with_episode_provenance(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    """gap 17: with an extractor, a free-text episode is turned into a fact carrying the
    episode's own provenance (authority/confidence), so it is no longer merely needs_review.
    """
    group = f"p:ft-{uuid7().hex[:12]}"
    async with _tenant(sessionmaker, group) as s:
        s.add(
            PublishedEpisodeRow(
                source_id=f"{group}:{uuid7()}",
                group_id=group,
                knowledge_type="text",
                verification="verified",
                authority=0.7,
                confidence=0.8,
                reference_time=utc_now(),
                payload={"body": "The checkout service runs on gke in production."},
                dedup_uuid=uuid7(),
            )
        )

    # Without an extractor the free-text episode is only flagged for review, no fact.
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        plain = await FabricBackfillService(uow.session).backfill_group(group_id=group)
        await uow.commit()
    assert plain.needs_review == 1
    assert plain.facts_created == 0

    # With an extractor, the same episode yields a fact carrying the episode's provenance.
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        rep = await FabricBackfillService(uow.session, _FakeExtractor()).backfill_group(
            group_id=group
        )
        await uow.commit()
    assert rep.needs_review == 0
    assert rep.facts_created == 1

    async with _tenant(sessionmaker, group) as s:
        auth = await s.scalar(
            text("SELECT authority FROM facts WHERE group_id = :g AND predicate = 'RUNS_ON'"),
            {"g": group},
        )
    assert auth == 0.7  # provenance from the episode, not invented
