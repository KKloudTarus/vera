"""Shared contract every MemoryEngine implementation must satisfy.

Run against the null fake (unit) and the real Graphiti adapter (integration) so both
honor the same port behavior.
"""

from __future__ import annotations

from vera.domain.ports.memory_engine import EpisodeSpec, GraphQuery, MemoryEngine
from vera.shared.time import utc_now
from vera.shared.types import GroupId, SourceId


async def assert_memory_contract(engine: MemoryEngine, *, group: str) -> None:
    receipt = await engine.ingest_episode(
        EpisodeSpec(
            source_id=SourceId(f"contract:{group}"),
            group_id=GroupId(group),
            body="alpha relates to beta",
            reference_time=utc_now(),
            metadata={
                "triples": [{"subject": "alpha", "predicate": "RELATES_TO", "object": "beta"}]
            },
        )
    )
    assert isinstance(receipt.episode_uuid, str)
    assert receipt.episode_uuid

    hits = await engine.search(GraphQuery(text="alpha", group_ids=(GroupId(group),), limit=5))
    assert isinstance(list(hits), list)

    assert await engine.health() is True
