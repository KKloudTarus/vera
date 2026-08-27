"""Each connector maps its source records to artifacts with stable ids, incrementally."""

from __future__ import annotations

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
from vera.adapters.connectors.slack import SlackConnector
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
        connector = ConfluenceConnector(client, base_url="https://cf.example", space_key="ENG")
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
        connector = ConfluenceConnector(client, base_url="https://cf.example", space_key="ENG")
        await connector.fetch_changes({"since": "2026-01-01T00:00:00.000Z"})

    cql = route.calls.last.request.url.params["cql"]
    assert 'lastmodified > "2026-01-01T00:00:00.000Z"' in cql


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
async def test_pdf_reads_new_files_only(tmp_path: Path) -> None:
    (tmp_path / "runbook.pdf").write_text("restart the service")
    connector = PdfConnector(str(tmp_path), extract=lambda p: p.read_text())

    batch = await connector.fetch_changes(None)
    assert batch.records[0].external_id == "pdf:runbook.pdf"
    assert batch.records[0].body == "restart the service"

    # Re-running with the saved mtime watermark finds no new files.
    batch2 = await connector.fetch_changes(batch.next_cursor)
    assert batch2.records == ()
