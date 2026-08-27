"""Confluence connector: pages in a space become text artifacts.

Incremental via a CQL ``lastmodified`` filter carried in the cursor, so each run pulls
only pages changed since the last sync. The external id is the stable Confluence page
id, so re-syncing a page updates it in place rather than duplicating it.
"""

from __future__ import annotations

from typing import Any

import httpx

from vera.adapters.connectors.base import parse_iso, strip_html
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
            clauses.append(f'lastmodified > "{since}"')
        return " and ".join(clauses) + " order by lastmodified asc"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        since = str(cursor["since"]) if cursor and cursor.get("since") else None
        response = await self._client.get(
            f"{self._base_url}/rest/api/content/search",
            params={
                "cql": self._cql(since),
                "limit": self._page_size,
                "expand": "body.storage,version",
            },
        )
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        results: list[dict[str, Any]] = data.get("results", [])

        records: list[ConnectorRecord] = []
        latest = since
        for page in results:
            page_id = str(page["id"])
            when = str(page.get("version", {}).get("when", "")) or None
            body = strip_html(str(page.get("body", {}).get("storage", {}).get("value", "")))
            records.append(
                ConnectorRecord(
                    external_id=f"confluence:{self._space}:{page_id}",
                    title=str(page.get("title", "")) or None,
                    body=body,
                    knowledge_type="text",
                    metadata={"url": f"{self._base_url}/pages/{page_id}", "page_id": page_id},
                    reference_time=parse_iso(when),
                )
            )
            if when and (latest is None or when > latest):
                latest = when

        next_cursor: JsonDict = {"since": latest} if latest else {}
        has_more = bool(data.get("_links", {}).get("next"))
        return ConnectorBatch(records=tuple(records), next_cursor=next_cursor, has_more=has_more)
