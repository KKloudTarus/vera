"""Derived community summaries retain deterministic authoritative fact lineage."""

from __future__ import annotations

from typing import cast

import pytest

from vera.application.community import CommunityLineageService
from vera.domain.ports.community import CommunityFact, CommunityLineageRepository
from vera.domain.ports.memory_engine import GraphCommunity, MemoryEngine
from vera.shared.ids import uuid7

pytestmark = pytest.mark.asyncio


class _Memory:
    def __init__(self) -> None:
        self.annotations: list[dict[str, str]] = []

    async def build_communities(self, *, group_id: str) -> tuple[GraphCommunity, ...]:
        return (
            GraphCommunity(
                community_id=str(uuid7()),
                name="runtime",
                summary="derived runtime summary",
                member_names=("payments", "prod"),
            ),
        )

    async def annotate_community(self, **values: str) -> None:
        self.annotations.append(values)


class _Lineage:
    def __init__(self, facts: tuple[CommunityFact, ...]) -> None:
        self.facts = facts
        self.records: list[dict[str, object]] = []

    async def active_facts(self, *, group_id: str) -> tuple[CommunityFact, ...]:
        return self.facts

    async def record(self, **values: object) -> None:
        self.records.append(values)


async def test_build_records_and_tags_deterministic_fact_lineage() -> None:
    facts = (
        CommunityFact(uuid7(), "fact-b", "payments", "RUNS_ON", "prod"),
        CommunityFact(uuid7(), "fact-a", "payments", "DEPENDS_ON", "postgres"),
    )
    memory = _Memory()
    lineage = _Lineage(facts)
    service = CommunityLineageService(
        memory=cast("MemoryEngine", memory),
        lineage=cast("CommunityLineageRepository", lineage),
    )

    first = await service.build(group_id="p:test")
    first_annotation = memory.annotations[-1]
    second = await service.build(group_id="p:test")
    second_annotation = memory.annotations[-1]

    assert first.communities == 1
    assert first.lineage_rows == 2
    assert first.projection_checkpoint == second.projection_checkpoint
    assert first_annotation["source_fact_set_hash"] == second_annotation["source_fact_set_hash"]
    assert first.derivation_run_id != second.derivation_run_id
    assert set(cast("tuple[object, ...]", lineage.records[0]["fact_ids"])) == {
        fact.fact_id for fact in facts
    }
    assert first_annotation["projection_checkpoint"] == first.projection_checkpoint
