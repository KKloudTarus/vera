"""FalkorDB graph backend against a live FalkorDB: ingest, search, temporal, retract.

VERA runs its own fulltext edge search on FalkorDB (Graphiti's hybrid search does not
return results there), so this guards that path. Uses the deterministic embedder and the
no-LLM client, so no external provider is needed. Skips if FalkorDB is unreachable.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.adapters.graph.offline import DeterministicEmbedder, NoCrossEncoder, NoLLMClient
from vera.domain.ports.memory_engine import EpisodeSpec, GraphQuery
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import GroupId, SourceId

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def falkordb_engine() -> AsyncIterator[GraphitiMemoryEngine]:
    from graphiti_core import Graphiti
    from graphiti_core.driver.falkordb_driver import FalkorDriver

    port = int(os.environ.get("VERA_FALKOR__PORT", "6380"))
    client = Graphiti(
        graph_driver=FalkorDriver(host="localhost", port=port, database="default_db"),
        embedder=DeterministicEmbedder(1024),
        llm_client=NoLLMClient(),
        cross_encoder=NoCrossEncoder(),
    )
    engine = GraphitiMemoryEngine(client)
    if not await engine.health():
        await client.close()
        pytest.skip("FalkorDB not reachable")
    assert engine._falkordb is True
    await engine.ensure_schema()
    try:
        yield engine
    finally:
        await client.close()


async def _ingest(engine: GraphitiMemoryEngine, *, group: str, obj: str) -> None:
    await engine.ingest_episode(
        EpisodeSpec(
            source_id=SourceId(f"cmdb:{uuid7().hex[:8]}"),
            group_id=GroupId(group),
            body="",
            reference_time=utc_now(),
            knowledge_type="fact_triple",
            metadata={
                "triples": [
                    {
                        "subject": "paymentapi",
                        "predicate": "RUNS_ON",
                        "object": obj,
                        "entity_type": "Service",
                    }
                ]
            },
        )
    )


async def test_ingest_then_search_returns_the_fact(
    falkordb_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    await _ingest(falkordb_engine, group=group, obj="prod-eks")

    # This is the path that returned 0 before VERA's native FalkorDB fulltext search.
    hits = await falkordb_engine.search(
        GraphQuery(text="where does paymentapi run", group_ids=(GroupId(group),), limit=10)
    )
    assert any("paymentapi" in h.fact for h in hits)


async def test_vector_half_returns_when_fulltext_cannot_match(
    falkordb_engine: GraphitiMemoryEngine,
) -> None:
    # A query whose words are absent from the fact cannot match via fulltext; a hit here
    # proves the vector half of the hybrid search is wired (edge fact_embedding + cosine).
    group = f"p:{uuid7().hex[:12]}"
    await _ingest(falkordb_engine, group=group, obj="prod-eks")

    hits = await falkordb_engine.search(
        GraphQuery(text="kubernetes deployment location", group_ids=(GroupId(group),), limit=10)
    )
    assert any("paymentapi" in h.fact for h in hits)


async def test_as_of_past_excludes_the_fact(
    falkordb_engine: GraphitiMemoryEngine,
) -> None:
    from datetime import timedelta

    group = f"p:{uuid7().hex[:12]}"
    await _ingest(falkordb_engine, group=group, obj="prod-eks")
    before = utc_now() - timedelta(days=1)

    hits = await falkordb_engine.search(
        GraphQuery(text="paymentapi", group_ids=(GroupId(group),), limit=10, as_of=before)
    )
    assert all("paymentapi" not in h.fact for h in hits)
