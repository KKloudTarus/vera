"""Connector sync against the live database: incremental, idempotent, no duplicates.

The SyncRunner drives records through curation into Postgres (the authoritative store);
memory is a projection of published_episodes, so counting those proves memory is updated
without duplicates. The scheduled-Confluence test is the phase's headline check.
"""

from __future__ import annotations

import httpx
import pytest
import pytest_asyncio
import respx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.connectors.confluence import ConfluenceConnector
from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.persistence.repositories.sync import SqlAlchemySyncStateStore
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.connectors import SyncRegistration, SyncRunner, SyncScheduler
from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import JsonDict

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _FakeConnector:
    def __init__(self, batches: list[ConnectorBatch]) -> None:
        self._batches = batches
        self._i = 0

    @property
    def kind(self) -> str:
        return "fake"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        batch = self._batches[min(self._i, len(self._batches) - 1)]
        self._i += 1
        return batch


@pytest_asyncio.fixture
async def source(sessionmaker: async_sessionmaker[AsyncSession]) -> tuple[str, object]:
    """Create tenancy plus a tier-1 (auto-publishing) source; return (group, source_id)."""
    group = f"p:{uuid7().hex[:12]}"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{group}", name="Org", group_id=f"o:{group}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{group}", name="WS", group_id=f"w:{group}"
        )
        proj = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{group}", name="Proj", group_id=group
        )
        source_id = await uow.sources.create(
            workspace_id=ws.id, project_id=proj.id, kind="cmdb", name="CMDB", trust_tier=1
        )
        await uow.commit()
    return group, source_id


def _runner(sessionmaker: async_sessionmaker[AsyncSession]) -> SyncRunner:
    return SyncRunner(
        uow_factory=lambda: SqlAlchemyUnitOfWork(sessionmaker),
        extractor=StructuredClaimExtractor(),
        state=SqlAlchemySyncStateStore(sessionmaker),
    )


async def _episode_count(sessionmaker: async_sessionmaker[AsyncSession], group: str) -> int:
    async with sessionmaker() as s:
        count = await s.scalar(
            text("SELECT count(*) FROM published_episodes WHERE group_id = :g"), {"g": group}
        )
    return int(count or 0)


def _triple_record(external_id: str, subject: str, obj: str) -> ConnectorRecord:
    return ConnectorRecord(
        external_id=external_id,
        body="",
        knowledge_type="fact_triple",
        metadata={"triples": [{"subject": subject, "predicate": "RUNSON", "object": obj}]},
        reference_time=utc_now(),
    )


async def test_sync_is_incremental_and_leaves_no_duplicates(
    sessionmaker: async_sessionmaker[AsyncSession],
    source: tuple[str, object],
) -> None:
    group, source_id = source
    first = ConnectorBatch(
        records=(
            _triple_record("cmdb:svc-1", "paymentapi", "prod"),
            _triple_record("cmdb:svc-2", "cacheapi", "stage"),
        ),
        next_cursor={"since": "1"},
    )
    empty = ConnectorBatch(records=(), next_cursor={"since": "1"})
    connector = _FakeConnector([first, empty])
    runner = _runner(sessionmaker)

    out1 = await runner.sync(source_id=source_id, group_id=group, connector=connector)  # type: ignore[arg-type]
    assert out1.processed == 2
    assert await _episode_count(sessionmaker, group) == 2

    # Second run: the connector reports no changes, so memory is untouched.
    out2 = await runner.sync(source_id=source_id, group_id=group, connector=connector)  # type: ignore[arg-type]
    assert out2.processed == 0
    assert await _episode_count(sessionmaker, group) == 2

    # The saved cursor advanced.
    state = SqlAlchemySyncStateStore(sessionmaker)
    assert await state.get_cursor(source_id) == {"since": "1"}  # type: ignore[arg-type]


async def test_reingesting_the_same_records_is_a_noop(
    sessionmaker: async_sessionmaker[AsyncSession],
    source: tuple[str, object],
) -> None:
    group, source_id = source
    batch = ConnectorBatch(
        records=(_triple_record("cmdb:svc-1", "paymentapi", "prod"),), next_cursor={"since": "1"}
    )
    # A connector that re-emits the same record every run (no incremental filter).
    connector = _FakeConnector([batch])
    runner = _runner(sessionmaker)

    await runner.sync(source_id=source_id, group_id=group, connector=connector)  # type: ignore[arg-type]
    out2 = await runner.sync(source_id=source_id, group_id=group, connector=connector)  # type: ignore[arg-type]
    assert out2.unchanged == 1  # content-idempotent: recognized as unchanged
    assert out2.processed == 0
    assert await _episode_count(sessionmaker, group) == 1  # no duplicate memory


@respx.mock
async def test_scheduled_confluence_sync_updates_without_duplicates(
    sessionmaker: async_sessionmaker[AsyncSession],
    source: tuple[str, object],
) -> None:
    group, source_id = source

    def _handler(request: httpx.Request) -> httpx.Response:
        # Incremental: once a cursor is set, the CQL carries a lastmodified filter and
        # the source reports nothing new.
        if "lastmodified >" in request.url.params["cql"]:
            return httpx.Response(200, json={"results": [], "_links": {}})
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "100",
                        "title": "Payments Runbook",
                        "version": {"when": "2026-01-02T10:00:00.000Z"},
                        "body": {"storage": {"value": "<p>How to restart paymentapi</p>"}},
                    }
                ],
                "_links": {},
            },
        )

    respx.get("https://cf.example/rest/api/content/search").mock(side_effect=_handler)

    async def _artifact_count() -> int:
        async with sessionmaker() as s:
            count = await s.scalar(
                text("SELECT count(*) FROM artifacts WHERE source_id = :s"),
                {"s": source_id},
            )
        return int(count or 0)

    async with httpx.AsyncClient() as client:
        connector = ConfluenceConnector(client, base_url="https://cf.example", space_key="ENG")
        scheduler = SyncScheduler(
            runner=_runner(sessionmaker),
            state=SqlAlchemySyncStateStore(sessionmaker),
            registrations=[
                SyncRegistration(
                    source_id=source_id,  # type: ignore[arg-type]
                    group_id=group,
                    connector=connector,
                    interval_s=0.0,
                )
            ],
        )
        # First scheduled run backfills the page.
        await scheduler.run_due()
        assert await _artifact_count() == 1
        # Second run is due again but the source reports no changes: no duplicate.
        await scheduler.run_due()
        assert await _artifact_count() == 1
