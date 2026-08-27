"""A no-op ``MemoryEngine`` for local dev and tests (no graph required).

Lets the API boot and the retrieval read model run end-to-end against an empty
result set before the real Graphiti adapter is wired.
"""

from __future__ import annotations

from collections.abc import Sequence

from vera.domain.ports.memory_engine import (
    EpisodeSpec,
    GraphHit,
    GraphQuery,
    IngestReceipt,
)
from vera.shared.ids import deterministic_id


class NullMemoryEngine:
    """Satisfies the ``MemoryEngine`` protocol; stores nothing, returns nothing."""

    async def ingest_episode(self, episode: EpisodeSpec) -> IngestReceipt:
        return IngestReceipt(episode_uuid=str(deterministic_id(str(episode.source_id))))

    async def search(self, query: GraphQuery) -> Sequence[GraphHit]:
        return []

    async def health(self) -> bool:
        return True

    async def clear_group(self, group_id: str) -> None:
        return None
