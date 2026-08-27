"""The ontology registry, pipeline versions, and that extraction is given the types."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.domain.ontology import (
    CURRENT_PIPELINE_VERSIONS,
    EDGE_TYPE_MAP,
    EDGE_TYPES,
    ENTITY_TYPES,
    ONTOLOGY_VERSION,
    edge_type_names,
    entity_type_names,
)
from vera.domain.ports.memory_engine import EpisodeSpec
from vera.shared.time import utc_now
from vera.shared.types import GroupId, SourceId


def test_registry_covers_the_core_domain() -> None:
    assert {"Service", "Environment", "Team", "Repository", "Incident", "Decision"} <= set(
        entity_type_names()
    )
    assert {"RUNS_ON", "DEPENDS_ON", "OWNS"} <= set(edge_type_names())
    # A catch-all pair lets any listed edge type connect generic entities.
    assert ("Entity", "Entity") in EDGE_TYPE_MAP


def test_pipeline_versions_record_every_stage() -> None:
    versions = CURRENT_PIPELINE_VERSIONS.as_dict()
    assert set(versions) == {
        "ontology",
        "parser",
        "normalizer",
        "extractor",
        "prompt",
        "model",
    }
    assert versions["ontology"] == str(ONTOLOGY_VERSION)


class _FakeResults:
    nodes: ClassVar[list[Any]] = []
    edges: ClassVar[list[Any]] = []


class _CapturingClient:
    def __init__(self) -> None:
        self.add_episode_kwargs: dict[str, Any] | None = None

    async def add_episode(self, **kwargs: Any) -> _FakeResults:
        self.add_episode_kwargs = kwargs
        return _FakeResults()


@pytest.mark.asyncio
async def test_text_ingestion_passes_the_ontology_to_the_extractor() -> None:
    client = _CapturingClient()
    engine = GraphitiMemoryEngine(client)  # type: ignore[arg-type]
    await engine.ingest_episode(
        EpisodeSpec(
            source_id=SourceId("doc:1"),
            group_id=GroupId("p:x"),
            body="paymentapi runs on prod",
            reference_time=utc_now(),
            knowledge_type="text",
            metadata={},
        )
    )
    assert client.add_episode_kwargs is not None
    assert client.add_episode_kwargs["entity_types"] is ENTITY_TYPES
    assert client.add_episode_kwargs["edge_types"] is EDGE_TYPES
    assert client.add_episode_kwargs["edge_type_map"] is EDGE_TYPE_MAP
