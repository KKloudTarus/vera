"""Slack connector: channel messages become text artifacts.

Incremental via the ``oldest`` timestamp in the cursor. The external id is the stable
channel+ts, which is unique per message, so re-sync never duplicates a message.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.shared.types import JsonDict


class SlackConnector:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        base_url: str,
        channel_id: str,
        page_size: int = 200,
    ) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._channel = channel_id
        self._page_size = page_size

    @property
    def kind(self) -> str:
        return "slack"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        oldest = str(cursor["oldest"]) if cursor and cursor.get("oldest") else "0"
        response = await self._client.get(
            f"{self._base_url}/api/conversations.history",
            params={"channel": self._channel, "oldest": oldest, "limit": self._page_size},
        )
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        messages: list[dict[str, Any]] = data.get("messages", [])

        records: list[ConnectorRecord] = []
        latest = oldest
        for message in messages:
            ts = str(message.get("ts", ""))
            text = str(message.get("text", ""))
            if not ts or not text:
                continue
            records.append(
                ConnectorRecord(
                    external_id=f"slack:{self._channel}:{ts}",
                    body=text,
                    knowledge_type="text",
                    metadata={"channel": self._channel, "user": str(message.get("user", ""))},
                    reference_time=datetime.fromtimestamp(float(ts), tz=UTC),
                )
            )
            if ts > latest:
                latest = ts

        next_cursor: JsonDict = {"oldest": latest}
        return ConnectorBatch(
            records=tuple(records),
            next_cursor=next_cursor,
            has_more=bool(data.get("has_more", False)),
        )
