"""Confluence connector: pages in a space become Markdown artifacts.

Incremental via a CQL ``lastmodified`` filter carried in the cursor. A run drains all API
pages of the query using an in-run ``start`` offset (SyncRunner follows ``has_more``); the
persisted watermark is the max ``lastmodified`` seen. The filter uses ``>=`` so pages sharing
the boundary timestamp are never skipped; re-fetched unchanged pages are content-hash no-ops.
The external id is the stable Confluence page id, so re-syncing updates in place. Storage XHTML
is converted to Markdown so headings survive for structure-aware chunking.
"""

from __future__ import annotations

from typing import Any

import httpx

from vera.adapters.connectors.base import parse_iso, storage_to_markdown
from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.shared.types import JsonDict


class ConfluenceConnector:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        base_url: str,
        space_key: str,
        page_size: int = 50,
    ) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._space = space_key
        self._page_size = page_size

    @property
    def kind(self) -> str:
        return "confluence"

    def _cql(self, since: str | None) -> str:
        clauses = [f'space="{self._space}"']
        if since:
            # >= (not >) so pages sharing the boundary lastmodified are never skipped across
            # the run boundary; content-hash idempotency makes the re-fetch a no-op.
            clauses.append(f'lastmodified >= "{since}"')
        return " and ".join(clauses) + " order by lastmodified asc"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        cursor = cursor or {}
        since = str(cursor["since"]) if cursor.get("since") else None
        start = int(cursor["start"]) if cursor.get("start") else 0
        run_max = str(cursor["max"]) if cursor.get("max") else since

        response = await self._client.get(
            f"{self._base_url}/rest/api/content/search",
            params={
                "cql": self._cql(since),
                "limit": self._page_size,
                "start": start,
                "expand": "body.storage,version",
            },
        )
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        results: list[dict[str, Any]] = data.get("results", [])

        records: list[ConnectorRecord] = []
        for page in results:
            page_id = str(page["id"])
            when = str(page.get("version", {}).get("when", "")) or None
            body = storage_to_markdown(
                str(page.get("body", {}).get("storage", {}).get("value", ""))
            )
            records.append(
                ConnectorRecord(
                    external_id=f"confluence:{self._space}:{page_id}",
                    title=str(page.get("title", "")) or None,
                    body=body,
                    knowledge_type="text",
                    metadata={
                        "url": f"{self._base_url}/pages/{page_id}",
                        "page_id": page_id,
                        "content_type": "text/markdown",
                    },
                    reference_time=parse_iso(when),
                )
            )
            if when and (run_max is None or when > run_max):
                run_max = when

        # has_more when the API links to a next page, or the page came back full.
        has_more = bool(data.get("_links", {}).get("next")) or len(results) == self._page_size
        if has_more:
            next_cursor: JsonDict = {"start": start + self._page_size}
            if since:
                next_cursor["since"] = since
            if run_max:
                next_cursor["max"] = run_max
        else:
            next_cursor = {"since": run_max} if run_max else {}
        return ConnectorBatch(records=tuple(records), next_cursor=next_cursor, has_more=has_more)
