"""CMDB connector: configuration items become fact triples.

A CI's relations map to triples (subject = CI name), so structured infrastructure data
lands in the graph directly with no LLM extraction. Incremental via an ``updated_at``
watermark in the cursor. The external id is the stable CI id. The record source is
injectable, so it works over an export file or an API without changing the mapping.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from vera.adapters.connectors.base import parse_iso
from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.shared.types import JsonDict

RecordsProvider = Callable[[], Awaitable[list[JsonDict]]]


class CmdbConnector:
    """Each raw CI is ``{id, name, type?, updated_at?, relations: [{predicate, object}]}``."""

    def __init__(self, records_provider: RecordsProvider) -> None:
        self._records_provider = records_provider

    @property
    def kind(self) -> str:
        return "cmdb"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        since = str(cursor["since"]) if cursor and cursor.get("since") else None
        items = await self._records_provider()

        records: list[ConnectorRecord] = []
        latest = since
        for item in items:
            updated = str(item.get("updated_at", "")) or None
            if since and updated and updated <= since:
                continue  # unchanged since the last sync
            ci_id = str(item["id"])
            name = str(item.get("name", ci_id))
            entity_type = str(item.get("type", "Entity"))
            triples = [
                {
                    "subject": name,
                    "predicate": str(relation["predicate"]),
                    "object": str(relation["object"]),
                    "entity_type": entity_type,
                }
                for relation in item.get("relations", [])
            ]
            records.append(
                ConnectorRecord(
                    external_id=f"cmdb:{ci_id}",
                    title=name,
                    body="",
                    knowledge_type="fact_triple",
                    metadata={"triples": triples},
                    reference_time=parse_iso(updated),
                )
            )
            if updated and (latest is None or updated > latest):
                latest = updated

        next_cursor: JsonDict = {"since": latest} if latest else (cursor or {})
        return ConnectorBatch(records=tuple(records), next_cursor=next_cursor)
