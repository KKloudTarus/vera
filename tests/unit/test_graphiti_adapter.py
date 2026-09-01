from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from graphiti_core.nodes import EntityNode
from graphiti_core.utils.maintenance import community_operations

from vera.adapters.graph.graphiti_adapter import (
    GraphitiMemoryEngine,
    _label_propagation_capped,  # pyright: ignore[reportPrivateUsage]
)


class _Driver:
    async def execute_query(self, _query: str, **_values: Any) -> SimpleNamespace:
        return SimpleNamespace(records=[])


class _Client:
    def __init__(self, driver: Any | None = None) -> None:
        self.driver: Any = driver or _Driver()
        self.llm_client = object()
        self.embedder = object()

    async def build_communities(self, **_values: Any) -> None:
        raise AssertionError("native unbounded clustering must not run")


def test_capped_label_propagation_terminates_for_oscillating_pair() -> None:
    projection = {"a": [("b", 2)], "b": [("a", 2)]}

    clusters = _label_propagation_capped(projection)

    assert [sorted(cluster) for cluster in clusters] == [["a", "b"]]


@pytest.mark.asyncio
async def test_neo4j_community_build_uses_capped_clustering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = GraphitiMemoryEngine(_Client())  # type: ignore[arg-type]
    groups: list[str] = []

    async def capped(group_id: str) -> tuple[()]:
        groups.append(group_id)
        return ()

    monkeypatch.setattr(engine, "_build_communities_capped", capped)

    assert await engine.build_communities(group_id="p:test") == ()
    assert groups == ["p_test"]


@pytest.mark.asyncio
async def test_capped_community_build_normalizes_names_and_runs_clusters_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {"a": "a", "b": "b", "c": 1},
        {"a": "b", "b": "a", "c": 1},
        {"a": "c", "b": "d", "c": 1},
        {"a": "d", "b": "c", "c": 1},
    ]

    class _CommunityDriver:
        async def execute_query(self, _query: str, **_values: Any) -> SimpleNamespace:
            return SimpleNamespace(records=rows)

    class _Community:
        name = ""
        summary = "Fallback community summary"

        async def generate_name_embedding(self, _embedder: object) -> None:
            assert self.name == self.summary

        async def save(self, _driver: object) -> None:
            return None

    class _Edge:
        async def save(self, _driver: object) -> None:
            return None

    active = 0
    max_active = 0

    async def remove_communities(_driver: object, *, group_ids: list[str]) -> None:
        assert group_ids == ["p_test"]

    async def get_nodes(_driver: object, node_ids: list[str]) -> list[SimpleNamespace]:
        return [SimpleNamespace(uuid=value, name=value) for value in node_ids]

    async def build_community(
        _llm_client: object, _nodes: list[SimpleNamespace]
    ) -> tuple[_Community, list[_Edge]]:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return _Community(), [_Edge()]

    engine = GraphitiMemoryEngine(_Client(_CommunityDriver()))  # type: ignore[arg-type]

    async def results(_nodes: list[Any], _edges: list[Any]) -> tuple[()]:
        return ()

    monkeypatch.setattr(community_operations, "remove_communities", remove_communities)
    monkeypatch.setattr(community_operations, "build_community", build_community)
    monkeypatch.setattr(EntityNode, "get_by_uuids", staticmethod(get_nodes))
    monkeypatch.setattr(engine, "_community_results", results)

    assert (
        await engine._build_communities_capped("p_test")  # pyright: ignore[reportPrivateUsage]
        == ()
    )
    assert max_active == 2
