"""Bi-temporal update against the real engine (needs an LLM: Graphiti reconciles edges).

Ingest "paymentapi runs on prod", then a trusted contradicting "runs on stage". The
current view shows only the new value; an as_of query from before the change returns the
old one. Marked ``llm`` (real OpenAI) and excluded from the default gate.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.application.queries.search_memory import SearchMemory, SearchMemoryHandler
from vera.bootstrap import Container, build_container, dispose_container
from vera.config.settings import get_settings
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import GroupId

pytestmark = [pytest.mark.integration, pytest.mark.llm, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def container() -> AsyncIterator[Container]:
    from dotenv import load_dotenv

    load_dotenv(override=True)
    settings = get_settings()
    if settings.memory.provider != "graphiti" or settings.memory.openai_api_key is None:
        pytest.skip("real LLM engine not configured")
    c = build_container(settings)
    if not await c.memory.health():
        await dispose_container(c)
        pytest.skip("Neo4j not reachable")
    await c.memory.ensure_schema()
    try:
        yield c
    finally:
        await dispose_container(c)


async def _ingest(
    sm: async_sessionmaker[AsyncSession], *, group: str, source: object, ext: str, obj: str
) -> None:
    async with SqlAlchemyUnitOfWork(sm) as uow:
        await uow.use_tenant(group)
        await CurationService(uow, StructuredClaimExtractor()).ingest_artifact(
            IngestArtifact(
                source_id=source,  # type: ignore[arg-type]
                group_id=group,
                external_id=ext,
                body="",
                knowledge_type="fact_triple",
                metadata={
                    "triples": [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": obj}]
                },
            )
        )
        await uow.commit()


async def _drain(c: Container) -> None:
    pool = LanePool(c, lanes=2, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(c, pool, batch_size=10)
    finally:
        await pool.stop()


async def test_newer_fact_supersedes_and_history_is_queryable(
    sessionmaker: async_sessionmaker[AsyncSession],
    container: Container,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{group}", name="O", group_id=f"o:{group}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{group}", name="W", group_id=f"w:{group}"
        )
        proj = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{group}", name="P", group_id=group
        )
        source = await uow.sources.create(
            workspace_id=ws.id, project_id=proj.id, kind="cmdb", name="CMDB", trust_tier=1
        )
        await uow.commit()

    handler = SearchMemoryHandler(container.memory, container.retrieval_read)

    await _ingest(sessionmaker, group=group, source=source, ext="rec-a", obj="prod-eks")
    await _drain(container)
    await asyncio.sleep(1.1)
    midpoint = utc_now()
    await asyncio.sleep(1.1)
    await _ingest(sessionmaker, group=group, source=source, ext="rec-b", obj="stage-eks")
    await _drain(container)

    now_facts = " ".join(
        h.fact
        for h in await handler.handle(
            SearchMemory(text="where does paymentapi run", group_ids=(GroupId(group),), limit=10)
        )
    )
    assert "stage-eks" in now_facts
    assert "prod-eks" not in now_facts  # superseded

    past_facts = " ".join(
        h.fact
        for h in await handler.handle(
            SearchMemory(
                text="where does paymentapi run",
                group_ids=(GroupId(group),),
                limit=10,
                as_of=midpoint,
            )
        )
    )
    assert "prod-eks" in past_facts
    assert "stage-eks" not in past_facts  # not yet valid at the midpoint
