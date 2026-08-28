"""Resilient incremental Confluence ingestion with page-level resume and tombstones."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

import httpx

from vera.adapters.connectors.base import parse_iso, storage_to_markdown
from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.shared.time import utc_now
from vera.shared.types import JsonDict

Sleeper = Callable[[float], Awaitable[None]]
Jitter = Callable[[float, float], float]
_TOMBSTONE_STATUSES = frozenset({"archived", "deleted", "trashed"})


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
    return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())


def _next_api_value(data: dict[str, Any], name: str) -> str | None:
    links_value = data.get("_links")
    if not isinstance(links_value, dict):
        return None
    links = cast("dict[str, Any]", links_value)
    next_url = links.get("next")
    if not isinstance(next_url, str) or not next_url:
        return None
    values = parse_qs(urlparse(next_url).query).get(name)
    return values[0] if values else None


class ConfluenceConnector:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        base_url: str,
        space_key: str,
        page_size: int = 50,
        overlap_s: float = 300.0,
        max_retries: int = 4,
        retry_base_s: float = 1.0,
        retry_cap_s: float = 30.0,
        max_concurrency: int = 4,
        scan_tombstones: bool = True,
        space_id: str | None = None,
        sleep: Sleeper = asyncio.sleep,
        jitter: Jitter = random.uniform,
    ) -> None:
        if page_size <= 0 or max_concurrency <= 0:
            raise ValueError("page_size and max_concurrency must be positive")
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._space = space_key
        self._page_size = page_size
        self._overlap = timedelta(seconds=max(0.0, overlap_s))
        self._max_retries = max(0, max_retries)
        self._retry_base = max(0.0, retry_base_s)
        self._retry_cap = max(self._retry_base, retry_cap_s)
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._scan_tombstones = scan_tombstones
        self._space_id = space_id
        self._sleep = sleep
        self._jitter = jitter

    @property
    def kind(self) -> str:
        return "confluence"

    def _cql(self, since: str | None, until: str) -> str:
        clauses = [f'space="{self._space}"', "type=page"]
        if since:
            boundary = parse_iso(since)
            lower = _iso(boundary - self._overlap) if boundary is not None else since
            clauses.append(f'lastmodified >= "{lower}"')
        # The upper bound keeps new writes out of this run; Confluence's opaque cursor preserves
        # its pagination position while the runner checkpoints each completed page.
        clauses.append(f'lastmodified <= "{until}"')
        return " and ".join(clauses) + " order by lastmodified asc, title asc"

    async def _get(self, path: str, *, params: dict[str, Any]) -> httpx.Response:
        for attempt in range(self._max_retries + 1):
            try:
                async with self._semaphore:
                    response = await self._client.get(f"{self._base_url}{path}", params=params)
            except httpx.TransportError:
                if attempt >= self._max_retries:
                    raise
            else:
                if response.status_code != 429 and response.status_code < 500:
                    response.raise_for_status()
                    return response
                if attempt >= self._max_retries:
                    response.raise_for_status()
                retry_after = _retry_after(response)
                if retry_after is not None:
                    # Never retry before Atlassian's explicit boundary; jitter only adds delay.
                    await self._sleep(retry_after + self._jitter(0.0, self._retry_base))
                    continue

            ceiling = min(self._retry_cap, self._retry_base * (2**attempt))
            await self._sleep(self._jitter(0.0, ceiling))
        raise RuntimeError("unreachable Confluence retry state")

    def _record(self, page: dict[str, Any]) -> ConnectorRecord:
        page_id = str(page["id"])
        version = page.get("version", {})
        when = str(version.get("when") or version.get("createdAt") or "") or None
        revision_value = version.get("number")
        revision = int(revision_value) if revision_value is not None else None
        updated_at = parse_iso(when)
        status = str(page.get("status", "current")).lower()
        tombstone = status in _TOMBSTONE_STATUSES
        body = (
            ""
            if tombstone
            else storage_to_markdown(str(page.get("body", {}).get("storage", {}).get("value", "")))
        )
        return ConnectorRecord(
            external_id=f"confluence:{self._space}:{page_id}",
            title=str(page.get("title", "")) or None,
            body=body,
            knowledge_type="text",
            metadata={
                "url": f"{self._base_url}/pages/{page_id}",
                "page_id": page_id,
                "content_type": "text/markdown",
                "status": status,
            },
            reference_time=updated_at,
            # A status transition need not increment the content revision. Comparing its
            # lastmodified tuple lets archive/delete supersede the live content version.
            source_revision=None if tombstone else revision,
            source_updated_at=updated_at,
            source_version_id=(
                f"{status}:{revision or when}"
                if tombstone
                else str(revision)
                if revision is not None
                else when
            ),
            tombstone=tombstone,
        )

    async def _resolve_space_id(self) -> str:
        if self._space_id:
            return self._space_id
        response = await self._get("/api/v2/spaces", params={"keys": self._space, "limit": 1})
        data: dict[str, Any] = response.json()
        results: list[dict[str, Any]] = data.get("results", [])
        if not results:
            raise ValueError(f"Confluence space {self._space!r} was not found")
        self._space_id = str(results[0]["id"])
        return self._space_id

    async def _fetch_tombstones(self, cursor: JsonDict) -> ConnectorBatch:
        since = str(cursor["since"])
        space_id = str(cursor.get("space_id") or await self._resolve_space_id())
        params: dict[str, Any] = {
            "status": ["archived", "trashed"],
            "body-format": "storage",
            "limit": self._page_size,
        }
        if cursor.get("api_cursor"):
            params["cursor"] = str(cursor["api_cursor"])
        response = await self._get(f"/api/v2/spaces/{space_id}/pages", params=params)
        data: dict[str, Any] = response.json()
        results: list[dict[str, Any]] = data.get("results", [])
        records = tuple(record for page in results if (record := self._record(page)).tombstone)
        api_cursor = _next_api_value(data, "cursor")
        if api_cursor:
            return ConnectorBatch(
                records=records,
                next_cursor={
                    "phase": "tombstones",
                    "since": since,
                    "space_id": space_id,
                    "api_cursor": api_cursor,
                },
                has_more=True,
            )
        return ConnectorBatch(records=records, next_cursor={"since": since})

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        cursor = cursor or {}
        if cursor.get("phase") == "tombstones":
            return await self._fetch_tombstones(cursor)

        since = str(cursor["since"]) if cursor.get("since") else None
        run_max = str(cursor["max"]) if cursor.get("max") else since
        until = str(cursor["until"]) if cursor.get("until") else _iso(utc_now())
        params: dict[str, Any] = {
            "cql": self._cql(since, until),
            "limit": self._page_size,
            "expand": "body.storage,version",
        }
        if cursor.get("api_cursor"):
            params["cursor"] = str(cursor["api_cursor"])
        elif cursor.get("start"):
            # Older Confluence/Data Center responses use offsets rather than opaque cursors.
            params["start"] = int(cursor["start"])
        response = await self._get("/rest/api/content/search", params=params)
        data: dict[str, Any] = response.json()
        results: list[dict[str, Any]] = data.get("results", [])
        records = tuple(self._record(page) for page in results)

        for record in records:
            when = record.source_updated_at
            current_max = parse_iso(run_max)
            if when is not None and (run_max is None or current_max is None or when > current_max):
                run_max = _iso(when)

        api_cursor = _next_api_value(data, "cursor")
        start_value = _next_api_value(data, "start")
        try:
            api_start = int(start_value) if start_value is not None else None
        except ValueError:
            api_start = None
        links = data.get("_links")
        has_more = (
            api_cursor is not None
            or api_start is not None
            or (not isinstance(links, dict) and len(results) == self._page_size)
        )
        if has_more:
            next_cursor: JsonDict = {
                "until": until,
            }
            if api_cursor:
                next_cursor["api_cursor"] = api_cursor
            elif api_start is not None:
                next_cursor["start"] = api_start
            else:
                start = int(cursor.get("start") or 0)
                next_cursor["start"] = start + (len(results) or self._page_size)
            if since:
                next_cursor["since"] = since
            if run_max:
                next_cursor["max"] = run_max
            return ConnectorBatch(records=records, next_cursor=next_cursor, has_more=True)

        watermark = run_max or until
        if self._scan_tombstones:
            return ConnectorBatch(
                records=records,
                next_cursor={"phase": "tombstones", "since": watermark},
                has_more=True,
            )
        return ConnectorBatch(records=records, next_cursor={"since": watermark})
