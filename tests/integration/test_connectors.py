"""Connector sync against the live database: incremental, idempotent, no duplicates.

The SyncRunner drives records through curation into Postgres (the authoritative store);
memory is a projection of published_episodes, so counting those proves memory is updated
without duplicates. The scheduled-Confluence test is the phase's headline check.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable

import httpx
import pytest
import pytest_asyncio
import respx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.connectors.confluence import ConfluenceConnector
from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.graph.null import NullMemoryEngine
from vera.adapters.persistence.repositories.sync import SqlAlchemySyncStateStore
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.connectors import SyncRegistration, SyncRunner, SyncScheduler
from vera.bootstrap import Container
from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord, SyncOutcome
from vera.domain.ports.curation import ExtractedClaim
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
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


class _ConfluenceFactExtractor:
    @property
    def provider(self) -> str:
        return "acceptance"

    @property
    def model(self) -> str:
        return "deterministic"

    async def extract(
        self, *, body: str, knowledge_type: str, metadata: JsonDict
    ) -> list[ExtractedClaim]:
        del knowledge_type, metadata
        quote = body.strip()
        if not quote:
            return []
        subject, obj = quote.split(" runs on ", maxsplit=1)
        start = body.index(quote)
        return [
            ExtractedClaim(
                statement=f"{subject} RUNS_ON {obj}",
                subject=subject,
                predicate="RUNS_ON",
                object=obj,
                source_quote=quote,
                quote_start=start,
                quote_end=start + len(quote),
            )
        ]


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


async def _drain_worker(container: Container) -> None:
    pool = LanePool(container, lanes=1, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()


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
        connector = ConfluenceConnector(
            client,
            base_url="https://cf.example",
            space_key="ENG",
            scan_tombstones=False,
        )
        scheduler = SyncScheduler(
            runner=_runner(sessionmaker),
            state=SqlAlchemySyncStateStore(sessionmaker),
            uow_factory=lambda: SqlAlchemyUnitOfWork(sessionmaker),
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


async def test_source_sync_lease_expires_and_enforces_ownership(
    sessionmaker: async_sessionmaker[AsyncSession],
    source: tuple[str, object],
) -> None:
    _, source_id = source
    first_owner = uuid7()
    second_owner = uuid7()

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        claimed = await uow.sources.claim_sync_lease(
            source_id=source_id,  # type: ignore[arg-type]
            owner_token=first_owner,
            lease_duration_s=0.05,
        )
        await uow.commit()
    assert claimed is True

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        blocked = await uow.sources.claim_sync_lease(
            source_id=source_id,  # type: ignore[arg-type]
            owner_token=second_owner,
            lease_duration_s=1.0,
        )
        await uow.commit()
    assert blocked is False

    await asyncio.sleep(0.08)
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        reclaimed = await uow.sources.claim_sync_lease(
            source_id=source_id,  # type: ignore[arg-type]
            owner_token=second_owner,
            lease_duration_s=1.0,
        )
        stale_renewal = await uow.sources.renew_sync_lease(
            source_id=source_id,  # type: ignore[arg-type]
            owner_token=first_owner,
            lease_duration_s=1.0,
        )
        stale_release = await uow.sources.release_sync_lease(
            source_id=source_id,  # type: ignore[arg-type]
            owner_token=first_owner,
        )
        renewed = await uow.sources.renew_sync_lease(
            source_id=source_id,  # type: ignore[arg-type]
            owner_token=second_owner,
            lease_duration_s=1.0,
        )
        released = await uow.sources.release_sync_lease(
            source_id=source_id,  # type: ignore[arg-type]
            owner_token=second_owner,
        )
        await uow.commit()

    assert reclaimed is True
    assert stale_renewal is False
    assert stale_release is False
    assert renewed is True
    assert released is True


async def test_schedulers_do_not_sync_the_same_source_concurrently(
    sessionmaker: async_sessionmaker[AsyncSession],
    source: tuple[str, object],
) -> None:
    group, source_id = source
    entered = asyncio.Event()
    release = asyncio.Event()

    class _Runner:
        def __init__(self) -> None:
            self.calls = 0

        async def sync(self, **_kwargs):
            self.calls += 1
            entered.set()
            await release.wait()
            return SyncOutcome(processed=0, unchanged=0, cursor={})

    runner = _Runner()
    registration = SyncRegistration(
        source_id=source_id,  # type: ignore[arg-type]
        group_id=group,
        connector=_FakeConnector([ConnectorBatch(records=(), next_cursor={})]),
        interval_s=60.0,
    )

    def _scheduler() -> SyncScheduler:
        return SyncScheduler(
            runner=runner,  # type: ignore[arg-type]
            state=SqlAlchemySyncStateStore(sessionmaker),
            uow_factory=lambda: SqlAlchemyUnitOfWork(sessionmaker),
            registrations=[registration],
            lease_duration_s=0.3,
        )

    async with asyncio.TaskGroup() as tasks:
        first = tasks.create_task(_scheduler().run_due())
        await entered.wait()
        # The original claim has expired by now. Renewal must still exclude another worker.
        await asyncio.sleep(0.45)
        second = await _scheduler().run_due()
        release.set()

    assert len(first.result()) == 1
    assert second == []
    assert runner.calls == 1


async def test_sync_drains_all_pages_in_one_run(
    sessionmaker: async_sessionmaker[AsyncSession],
    source: tuple[str, object],
) -> None:
    # gap 9: a run must follow the connector's pagination until exhausted, not stop after the
    # first API page. Two pages (has_more then done) must both be ingested in a single sync.
    group, source_id = source
    page1 = ConnectorBatch(
        records=(_triple_record("cmdb:p1", "svc1", "eks"),),
        next_cursor={"start": 1},
        has_more=True,
    )
    page2 = ConnectorBatch(
        records=(_triple_record("cmdb:p2", "svc2", "ecs"),),
        next_cursor={"since": "final"},
        has_more=False,
    )
    connector = _FakeConnector([page1, page2])

    out = await _runner(sessionmaker).sync(source_id=source_id, group_id=group, connector=connector)  # type: ignore[arg-type]
    assert out.processed == 2  # both pages drained in one run
    assert await _episode_count(sessionmaker, group) == 2

    # The persisted cursor is the last page's cursor (the new watermark), not the first page's.
    state = SqlAlchemySyncStateStore(sessionmaker)
    assert await state.get_cursor(source_id) == {"since": "final"}  # type: ignore[arg-type]


async def test_sync_checkpoints_each_page_and_resumes_after_a_crash(
    sessionmaker: async_sessionmaker[AsyncSession],
    source: tuple[str, object],
) -> None:
    group, source_id = source

    class _CrashingConnector:
        @property
        def kind(self) -> str:
            return "crashing"

        async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
            if cursor is None:
                return ConnectorBatch(
                    records=(_triple_record("cmdb:p1", "svc1", "eks"),),
                    next_cursor={"start": 1, "until": "fixed"},
                    has_more=True,
                )
            raise RuntimeError("simulated mid-space crash")

    with pytest.raises(RuntimeError, match="mid-space crash"):
        await _runner(sessionmaker).sync(
            source_id=source_id,
            group_id=group,
            connector=_CrashingConnector(),  # type: ignore[arg-type]
        )

    state = SqlAlchemySyncStateStore(sessionmaker)
    checkpoint = {"start": 1, "until": "fixed"}
    assert await state.get_cursor(source_id) == checkpoint  # type: ignore[arg-type]

    class _ResumingConnector:
        @property
        def kind(self) -> str:
            return "resuming"

        async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
            assert cursor == checkpoint
            return ConnectorBatch(
                records=(_triple_record("cmdb:p2", "svc2", "ecs"),),
                next_cursor={"since": "final"},
            )

    outcome = await _runner(sessionmaker).sync(
        source_id=source_id,
        group_id=group,
        connector=_ResumingConnector(),  # type: ignore[arg-type]
    )
    assert outcome.processed == 1
    assert await _episode_count(sessionmaker, group) == 2
    assert await state.get_cursor(source_id) == {"since": "final"}  # type: ignore[arg-type]


async def test_tombstone_creates_an_empty_reconciliation_version(
    sessionmaker: async_sessionmaker[AsyncSession],
    source: tuple[str, object],
) -> None:
    group, source_id = source
    live = _triple_record("cmdb:deleted", "paymentapi", "eks")
    tombstone = ConnectorRecord(
        external_id=live.external_id,
        body="",
        source_updated_at=utc_now(),
        source_version_id="trashed:1",
        tombstone=True,
        metadata={"status": "trashed"},
    )
    connector = _FakeConnector(
        [
            ConnectorBatch(records=(live,), next_cursor={"since": "1"}),
            ConnectorBatch(records=(tombstone,), next_cursor={"since": "2"}),
        ]
    )
    runner = _runner(sessionmaker)

    await runner.sync(source_id=source_id, group_id=group, connector=connector)  # type: ignore[arg-type]
    outcome = await runner.sync(source_id=source_id, group_id=group, connector=connector)  # type: ignore[arg-type]

    assert outcome.processed == 1
    async with sessionmaker() as session:
        versions = await session.scalar(
            text(
                "SELECT count(*) FROM artifact_versions av "
                "JOIN artifacts a ON a.id = av.artifact_id "
                "WHERE a.source_id = :source_id"
            ),
            {"source_id": source_id},
        )
        finalizers = await session.scalar(
            text(
                "SELECT count(*) FROM ingestion_jobs WHERE group_id = :group_id "
                "AND payload->>'job_kind' = 'fabric_reconcile_version'"
            ),
            {"group_id": group},
        )
    assert versions == 2
    assert finalizers == 1


@respx.mock
@pytest.mark.issue6_acceptance
async def test_e2e_confluence_crash_resume_and_tombstone_retracts_fact(
    sessionmaker: async_sessionmaker[AsyncSession],
    source: tuple[str, object],
    make_container: Callable[[object], Container],
) -> None:
    group, source_id = source
    base = make_container(NullMemoryEngine())
    memory = base.settings.memory.model_copy(update={"fabric_write_mode": "fabric"})
    container = dataclasses.replace(
        base, settings=base.settings.model_copy(update={"memory": memory})
    )
    runner = SyncRunner(
        uow_factory=lambda: SqlAlchemyUnitOfWork(sessionmaker),
        extractor=_ConfluenceFactExtractor(),
        state=SqlAlchemySyncStateStore(sessionmaker),
    )
    should_crash = {"value": True}

    def page(page_id: str, title: str, body: str, when: str) -> dict[str, object]:
        return {
            "id": page_id,
            "title": title,
            "status": "current",
            "version": {"number": 1, "when": when},
            "body": {"storage": {"value": f"<p>{body}</p>"}},
        }

    def search_handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("cursor") == "next-page":
            if should_crash["value"]:
                raise httpx.TransportError("simulated mid-space crash", request=request)
            return httpx.Response(
                200,
                json={
                    "results": [
                        page(
                            "101",
                            "Cache Runtime",
                            "cacheapi runs on redis",
                            "2026-01-02T11:00:00Z",
                        )
                    ],
                    "_links": {},
                },
            )
        return httpx.Response(
            200,
            json={
                "results": [
                    page(
                        "100",
                        "Payments Runtime",
                        "paymentapi runs on eks",
                        "2026-01-02T10:00:00Z",
                    )
                ],
                "_links": {"next": "/rest/api/content/search?cursor=next-page"},
            },
        )

    search = respx.get("https://cf.example/rest/api/content/search").mock(
        side_effect=search_handler
    )
    async with httpx.AsyncClient() as client:
        connector = ConfluenceConnector(
            client,
            base_url="https://cf.example",
            space_key="ENG",
            max_retries=0,
            scan_tombstones=False,
        )
        with pytest.raises(httpx.TransportError, match="mid-space crash"):
            await runner.sync(
                source_id=source_id,
                group_id=group,
                connector=connector,  # type: ignore[arg-type]
            )

        state = SqlAlchemySyncStateStore(sessionmaker)
        checkpoint = await state.get_cursor(source_id)  # type: ignore[arg-type]
        assert checkpoint is not None and checkpoint["api_cursor"] == "next-page"
        should_crash["value"] = False
        resumed = await runner.sync(
            source_id=source_id,
            group_id=group,
            connector=connector,  # type: ignore[arg-type]
        )
        assert resumed.processed == 1

        await _drain_worker(container)
        async with sessionmaker() as session:
            active = await session.scalar(
                text(
                    "SELECT count(*) FROM facts WHERE group_id = :group "
                    "AND lifecycle_state = 'active'"
                ),
                {"group": group},
            )
        assert active == 2

        search.mock(return_value=httpx.Response(200, json={"results": [], "_links": {}}))
        respx.get("https://cf.example/api/v2/spaces/space-1/pages").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "id": "100",
                            "title": "Payments Runtime",
                            "status": "trashed",
                            "version": {
                                "number": 2,
                                "createdAt": "2026-01-02T12:00:00Z",
                            },
                        }
                    ],
                    "_links": {},
                },
            )
        )
        tombstones = ConfluenceConnector(
            client,
            base_url="https://cf.example",
            space_key="ENG",
            space_id="space-1",
            scan_tombstones=True,
        )
        deleted = await runner.sync(
            source_id=source_id,
            group_id=group,
            connector=tombstones,  # type: ignore[arg-type]
        )
        assert deleted.processed == 1

    await _drain_worker(container)
    async with sessionmaker() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT co.canonical_name AS object_name, f.lifecycle_state "
                    "FROM facts f LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
                    "WHERE f.group_id = :group ORDER BY object_name"
                ),
                {"group": group},
            )
        ).mappings()
        states = {str(row["object_name"]): str(row["lifecycle_state"]) for row in rows}
    assert states == {"eks": "retracted", "redis": "active"}
