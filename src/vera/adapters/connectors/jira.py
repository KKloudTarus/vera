"""Jira connector: issues in a project become text artifacts with status metadata.

Incremental via a JQL ``updated`` filter in the cursor. The external id is the stable
issue key, so an updated issue replaces its prior version rather than duplicating.
"""

from __future__ import annotations

from typing import Any

import httpx

from vera.adapters.connectors.base import parse_iso
from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.shared.types import JsonDict


class JiraConnector:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        base_url: str,
        project_key: str,
        page_size: int = 50,
    ) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._project = project_key
        self._page_size = page_size

    @property
    def kind(self) -> str:
        return "jira"

    def _jql(self, since: str | None) -> str:
        clauses = [f"project = {self._project}"]
        if since:
            clauses.append(f'updated > "{since}"')
        return " AND ".join(clauses) + " ORDER BY updated ASC"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        since = str(cursor["since"]) if cursor and cursor.get("since") else None
        response = await self._client.get(
            f"{self._base_url}/rest/api/2/search",
            params={
                "jql": self._jql(since),
                "maxResults": self._page_size,
                "fields": "summary,description,status,issuetype,updated",
            },
        )
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        issues: list[dict[str, Any]] = data.get("issues", [])

        records: list[ConnectorRecord] = []
        latest = since
        for issue in issues:
            key = str(issue["key"])
            fields: dict[str, Any] = issue.get("fields", {})
            summary = str(fields.get("summary", ""))
            description = str(fields.get("description", "") or "")
            status = str(fields.get("status", {}).get("name", "")) or "unknown"
            updated = str(fields.get("updated", "")) or None
            updated_at = parse_iso(updated)
            records.append(
                ConnectorRecord(
                    external_id=f"jira:{key}",
                    title=summary,
                    body=f"{summary}\n\n{description}".strip(),
                    knowledge_type="text",
                    metadata={"key": key, "status": status},
                    reference_time=updated_at,
                    source_updated_at=updated_at,
                    source_version_id=updated,
                )
            )
            if updated and (latest is None or updated > latest):
                latest = updated

        next_cursor: JsonDict = {"since": latest} if latest else {}
        total = int(data.get("total", len(issues)))
        return ConnectorBatch(
            records=tuple(records), next_cursor=next_cursor, has_more=len(issues) < total
        )
