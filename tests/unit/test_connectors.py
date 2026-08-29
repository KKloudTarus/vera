"""Each connector maps its source records to artifacts with stable ids, incrementally."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from vera.adapters.connectors.cmdb import CmdbConnector
from vera.adapters.connectors.confluence import ConfluenceConnector
from vera.adapters.connectors.filesystem import FilesystemConnector
from vera.adapters.connectors.git import GitConnector
from vera.adapters.connectors.jira import JiraConnector
from vera.adapters.connectors.pdf import PdfConnector
from vera.adapters.connectors.registry import build_connector
from vera.adapters.connectors.slack import SlackConnector
from vera.application.connectors import SyncRegistration, SyncScheduler
from vera.domain.ports.connectors import SyncOutcome
from vera.shared.errors import VeraError
from vera.shared.ids import uuid7
from vera.shared.types import JsonDict


@pytest.mark.asyncio
@respx.mock
async def test_confluence_maps_pages_and_advances_cursor() -> None:
    route = respx.get("https://cf.example/rest/api/content/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "42",
                        "title": "Runbook",
                        "version": {"when": "2026-01-02T10:00:00.000Z"},
                        "body": {"storage": {"value": "<p>Restart <b>paymentapi</b></p>"}},
                    }
                ],
                "_links": {},
            },
        )
    )
    async with httpx.AsyncClient() as client:
        connector = ConfluenceConnector(
            client,
            base_url="https://cf.example",
            space_key="ENG",
            scan_tombstones=False,
        )
        batch = await connector.fetch_changes(None)

    assert len(batch.records) == 1
    record = batch.records[0]
    assert record.external_id == "confluence:ENG:42"
    assert record.body == "Restart paymentapi"
    assert batch.next_cursor == {"since": "2026-01-02T10:00:00.000Z"}
    # A first sync has no lastmodified filter (the order-by clause is always present).
    assert "lastmodified >" not in route.calls.last.request.url.params["cql"]


@pytest.mark.asyncio
@respx.mock
async def test_confluence_incremental_uses_the_cursor() -> None:
    route = respx.get("https://cf.example/rest/api/content/search").mock(
        return_value=httpx.Response(200, json={"results": [], "_links": {}})
    )
    async with httpx.AsyncClient() as client:
        connector = ConfluenceConnector(
            client,
            base_url="https://cf.example",
            space_key="ENG",
            scan_tombstones=False,
        )
        await connector.fetch_changes({"since": "2026-01-01T00:00:00.000Z"})

    cql = route.calls.last.request.url.params["cql"]
    # The overlap catches late indexing while content/version idempotency absorbs re-fetches.
    assert 'lastmodified >= "2025-12-31T23:55:00.000Z"' in cql
    assert "lastmodified <=" in cql


@pytest.mark.asyncio
@respx.mock
async def test_jira_maps_issues_with_status() -> None:
    respx.get("https://jira.example/rest/api/2/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "total": 1,
                "issues": [
                    {
                        "key": "OPS-7",
                        "fields": {
                            "summary": "Scale paymentapi",
                            "description": "add replicas",
                            "status": {"name": "Done"},
                            "updated": "2026-01-03T09:00:00.000+0000",
                        },
                    }
                ],
            },
        )
    )
    async with httpx.AsyncClient() as client:
        connector = JiraConnector(client, base_url="https://jira.example", project_key="OPS")
        batch = await connector.fetch_changes(None)

    record = batch.records[0]
    assert record.external_id == "jira:OPS-7"
    assert record.metadata["status"] == "Done"
    assert "Scale paymentapi" in record.body
    assert batch.next_cursor == {"since": "2026-01-03T09:00:00.000+0000"}


@pytest.mark.asyncio
@respx.mock
async def test_slack_maps_messages_and_tracks_oldest() -> None:
    respx.get("https://slack.example/api/conversations.history").mock(
        return_value=httpx.Response(
            200,
            json={
                "messages": [
                    {"ts": "1700000000.000100", "text": "deploy done", "user": "U1"},
                    {"ts": "1700000100.000200", "text": "rollback", "user": "U2"},
                ],
                "has_more": False,
            },
        )
    )
    async with httpx.AsyncClient() as client:
        connector = SlackConnector(client, base_url="https://slack.example", channel_id="C9")
        batch = await connector.fetch_changes(None)

    assert {r.external_id for r in batch.records} == {
        "slack:C9:1700000000.000100",
        "slack:C9:1700000100.000200",
    }
    assert batch.next_cursor == {"oldest": "1700000100.000200"}


@pytest.mark.asyncio
async def test_git_maps_commits_and_ranges_from_cursor() -> None:
    seen_args: list[list[str]] = []

    async def fake_runner(args: list[str]) -> str:
        seen_args.append(args)
        return (
            "abc123\x1fAlice\x1f2026-01-04T12:00:00+00:00\x1fFix cache\n"
            "def456\x1fBob\x1f2026-01-03T12:00:00+00:00\x1fAdd metrics\n"
        )

    connector = GitConnector("/srv/repos/payments", runner=fake_runner)
    batch = await connector.fetch_changes(None)
    assert batch.records[0].external_id == "git:payments:abc123"
    assert batch.next_cursor == {"last_sha": "abc123"}  # newest commit is first

    await connector.fetch_changes({"last_sha": "abc123"})
    assert "abc123..HEAD" in seen_args[-1]


@pytest.mark.asyncio
async def test_cmdb_maps_relations_to_triples_incrementally() -> None:
    items: list[JsonDict] = [
        {
            "id": "svc-1",
            "name": "paymentapi",
            "type": "Service",
            "updated_at": "2026-01-05",
            "relations": [{"predicate": "RUNSON", "object": "prod-eks"}],
        }
    ]

    async def provider() -> list[JsonDict]:
        return items

    connector = CmdbConnector(provider)
    batch = await connector.fetch_changes(None)
    record = batch.records[0]
    assert record.external_id == "cmdb:svc-1"
    assert record.knowledge_type == "fact_triple"
    assert record.metadata["triples"][0] == {
        "subject": "paymentapi",
        "predicate": "RUNSON",
        "object": "prod-eks",
        "entity_type": "Service",
    }
    assert batch.next_cursor == {"since": "2026-01-05"}

    # Nothing changed since the watermark: no records.
    batch2 = await connector.fetch_changes({"since": "2026-01-05"})
    assert batch2.records == ()


@pytest.mark.asyncio
async def test_filesystem_reads_markdown_recursively_and_incrementally(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("# synapse\nA control plane.")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "guide.md").write_text("## guide\nrun the cli")
    (tmp_path / "notes.txt").write_text("plain note")
    (tmp_path / "ignore.go").write_text("package main")

    connector = FilesystemConnector(str(tmp_path))
    batch = await connector.fetch_changes(None)
    ids = {r.external_id for r in batch.records}
    assert ids == {"fs:README.md", "fs:docs/guide.md", "fs:notes.txt"}  # .go excluded
    assert any("control plane" in r.body for r in batch.records)

    # Re-run with the watermark: nothing new.
    batch2 = await connector.fetch_changes(batch.next_cursor)
    assert batch2.records == ()


@pytest.mark.asyncio
async def test_scheduler_isolates_a_failed_connector() -> None:
    source_ids = [uuid7(), uuid7()]

    class _State:
        async def last_synced_at(self, _source_id):
            return None

    class _Runner:
        def __init__(self) -> None:
            self.calls = 0

        async def sync(self, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("broken source")
            return SyncOutcome(processed=1, unchanged=0, cursor={})

    runner = _Runner()
    connector = FilesystemConnector(".")
    scheduler = SyncScheduler(
        runner=runner,  # type: ignore[arg-type]
        state=_State(),  # type: ignore[arg-type]
        registrations=[
            SyncRegistration(
                source_id=source_id,
                group_id="p:test",
                connector=connector,
                interval_s=0,
            )
            for source_id in source_ids
        ],
    )

    outcomes = await scheduler.run_due()

    assert runner.calls == 2
    assert len(outcomes) == 1
    assert outcomes[0].processed == 1


@pytest.mark.asyncio
async def test_pdf_reads_new_files_only(tmp_path: Path) -> None:
    (tmp_path / "runbook.pdf").write_text("restart the service")
    connector = PdfConnector(str(tmp_path), extract=lambda p: p.read_text())

    batch = await connector.fetch_changes(None)
    assert batch.records[0].external_id == "pdf:runbook.pdf"
    assert batch.records[0].body == "restart the service"

    # Re-running with the saved mtime watermark finds no new files.
    batch2 = await connector.fetch_changes(batch.next_cursor)
    assert batch2.records == ()


def test_registry_builds_connectors_by_kind() -> None:
    assert build_connector({"kind": "filesystem", "root": "/srv/x"}).kind == "filesystem"
    assert build_connector({"kind": "git", "repo_path": "/srv/repo"}).kind == "git"
    assert (
        build_connector(
            {"kind": "confluence", "base_url": "https://c", "space_key": "ENG", "token": "t"}
        ).kind
        == "confluence"
    )
    with pytest.raises(ValueError, match="unknown connector kind"):
        build_connector({"kind": "nope"})


@pytest.mark.asyncio
@respx.mock
async def test_registry_wires_cmdb_over_http() -> None:
    respx.get("https://cmdb/export").mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [
                    {
                        "id": "svc-1",
                        "name": "paymentapi",
                        "type": "Service",
                        "updated_at": "2026-01-01T00:00:00Z",
                        "relations": [{"predicate": "RUNS_ON", "object": "prod-eks"}],
                    }
                ]
            },
        )
    )
    connector = build_connector({"kind": "cmdb", "url": "https://cmdb/export"})
    assert connector.kind == "cmdb"

    batch = await connector.fetch_changes(None)
    assert batch.records[0].external_id == "cmdb:svc-1"
    triples = batch.records[0].metadata["triples"]
    assert triples[0] == {
        "subject": "paymentapi",
        "predicate": "RUNS_ON",
        "object": "prod-eks",
        "entity_type": "Service",
    }


def test_registry_cmdb_without_url_is_an_error() -> None:
    with pytest.raises(VeraError, match="cmdb connector needs 'url'"):
        build_connector({"kind": "cmdb"})


def test_registry_resolves_token_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERA_TEST_JIRA_TOKEN", "secret-value")
    client = build_connector(
        {
            "kind": "jira",
            "base_url": "https://j",
            "project_key": "ENG",
            "token_env": "VERA_TEST_JIRA_TOKEN",
        }
    )
    assert client.kind == "jira"  # built, secret pulled from the environment


def test_registry_missing_token_env_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERA_TEST_ABSENT_TOKEN", raising=False)
    with pytest.raises(VeraError, match="token env"):
        build_connector(
            {
                "kind": "jira",
                "base_url": "https://j",
                "project_key": "ENG",
                "token_env": "VERA_TEST_ABSENT_TOKEN",
            }
        )


def test_storage_to_markdown_preserves_headings_lists_and_links() -> None:
    from vera.adapters.connectors.base import storage_to_markdown

    xhtml = (
        "<h1>Guide</h1><p>Intro prose.</p>"
        "<h2>Setup</h2><ul><li>Install it</li><li>Run it</li></ul>"
        "<p>See <a href='https://x/docs'>the docs</a>.</p>"
    )
    md = storage_to_markdown(xhtml)
    assert "# Guide" in md
    assert "## Setup" in md
    assert "- Install it" in md
    assert "the docs (https://x/docs)" in md


def _confluence_page(page_id: str, when: str, storage: str) -> dict:
    return {
        "id": page_id,
        "title": f"Page {page_id}",
        "version": {"when": when},
        "body": {"storage": {"value": storage}},
    }


@pytest.mark.asyncio
@respx.mock
async def test_confluence_paginates_and_converts_to_markdown() -> None:
    route = respx.get("https://c/rest/api/content/search")
    route.side_effect = [
        httpx.Response(
            200,
            json={
                "results": [
                    _confluence_page("1", "2026-01-01T00:00:00.000Z", "<h1>A</h1><p>alpha</p>"),
                    _confluence_page("2", "2026-01-02T00:00:00.000Z", "<h1>B</h1><p>bravo</p>"),
                ]
            },
        ),
        httpx.Response(
            200,
            json={
                "results": [
                    _confluence_page("3", "2026-01-03T00:00:00.000Z", "<h1>C</h1><p>charlie</p>"),
                ]
            },
        ),
    ]
    conn = ConfluenceConnector(
        httpx.AsyncClient(),
        base_url="https://c",
        space_key="ENG",
        page_size=2,
        scan_tombstones=False,
    )

    first = await conn.fetch_changes(None)
    assert first.has_more is True  # a full page => more to drain
    assert first.next_cursor["start"] == 2
    assert first.records[0].metadata["content_type"] == "text/markdown"
    assert "# A" in first.records[0].body  # headings survive for structural chunking

    second = await conn.fetch_changes(first.next_cursor)
    assert second.has_more is False  # partial page => drained
    assert second.next_cursor == {
        "since": "2026-01-03T00:00:00.000Z"
    }  # watermark = max lastmodified


@pytest.mark.asyncio
@respx.mock
async def test_confluence_honors_retry_after_and_jitters_backoff() -> None:
    route = respx.get("https://c/rest/api/content/search")
    route.side_effect = [
        httpx.Response(429, headers={"Retry-After": "2"}),
        httpx.Response(503),
        httpx.Response(200, json={"results": [], "_links": {}}),
    ]
    delays: list[float] = []

    async def _sleep(delay: float) -> None:
        delays.append(delay)

    async with httpx.AsyncClient() as client:
        conn = ConfluenceConnector(
            client,
            base_url="https://c",
            space_key="ENG",
            scan_tombstones=False,
            sleep=_sleep,
            jitter=lambda low, high: (low + high) / 2,
        )
        await conn.fetch_changes(None)

    assert route.call_count == 3
    assert delays == [2.5, 1.0]


@pytest.mark.asyncio
@respx.mock
async def test_confluence_emits_archived_pages_as_tombstones() -> None:
    respx.get("https://c/rest/api/content/search").mock(
        return_value=httpx.Response(200, json={"results": [], "_links": {}})
    )
    respx.get("https://c/api/v2/spaces").mock(
        return_value=httpx.Response(200, json={"results": [{"id": "7", "key": "ENG"}]})
    )
    tombstone_route = respx.get("https://c/api/v2/spaces/7/pages").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "42",
                        "status": "archived",
                        "title": "Old runbook",
                        "version": {
                            "number": 8,
                            "createdAt": "2026-01-02T10:00:00.000Z",
                        },
                        "body": {"storage": {"value": "<p>stale content</p>"}},
                    }
                ],
                "_links": {},
            },
        )
    )
    async with httpx.AsyncClient() as client:
        connector = ConfluenceConnector(client, base_url="https://c", space_key="ENG")
        changes = await connector.fetch_changes(None)
        batch = await connector.fetch_changes(changes.next_cursor)

    record = batch.records[0]
    assert record.tombstone is True
    assert record.body == ""
    assert record.source_revision is None
    assert record.metadata["status"] == "archived"
    assert tombstone_route.calls.last.request.url.params.get_list("status") == [
        "archived",
        "trashed",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("page_count", [28, 100, 500])
async def test_confluence_drains_large_stable_spaces(page_count: int) -> None:
    starts: list[int] = []
    cql_queries: set[str] = set()
    base_time = datetime(2026, 1, 1, tzinfo=UTC)

    async def _handler(request: httpx.Request) -> httpx.Response:
        start = int(request.url.params.get("cursor", "0"))
        starts.append(start)
        cql_queries.add(request.url.params["cql"])
        when = (base_time + timedelta(seconds=start)).isoformat().replace("+00:00", "Z")
        return httpx.Response(
            200,
            json={
                "results": [_confluence_page(str(start), when, f"<p>page {start}</p>")],
                "_links": (
                    {"next": f"/rest/api/content/search?cursor={start + 1}"}
                    if start + 1 < page_count
                    else {}
                ),
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(_handler)) as client:
        conn = ConfluenceConnector(
            client,
            base_url="https://c",
            space_key="ENG",
            page_size=1,
            scan_tombstones=False,
        )
        cursor = None
        seen: list[str] = []
        while True:
            batch = await conn.fetch_changes(cursor)
            seen.extend(record.external_id for record in batch.records)
            cursor = batch.next_cursor
            if not batch.has_more:
                break

    assert starts == list(range(page_count))
    assert len(seen) == page_count
    assert len(cql_queries) == 1  # the in-run upper bound freezes every paginated request


@pytest.mark.asyncio
async def test_confluence_bounds_concurrent_requests() -> None:
    active = 0
    peak = 0

    async def _handler(_request: httpx.Request) -> httpx.Response:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return httpx.Response(200, json={"results": [], "_links": {}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(_handler)) as client:
        conn = ConfluenceConnector(
            client,
            base_url="https://c",
            space_key="ENG",
            max_concurrency=2,
            scan_tombstones=False,
        )
        await asyncio.gather(*(conn.fetch_changes(None) for _ in range(8)))

    assert peak == 2
