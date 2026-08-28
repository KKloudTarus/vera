"""Persist fact lineage for LLM-derived graph community summaries."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

from vera.domain.ports.community import CommunityFact, CommunityLineageRepository
from vera.domain.ports.memory_engine import MemoryEngine
from vera.shared.ids import uuid7


def _fact_set_hash(facts: tuple[CommunityFact, ...]) -> str:
    material = "\n".join(sorted(str(fact.fact_id) for fact in facts))
    return hashlib.sha256(material.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CommunityBuildReport:
    communities: int
    lineage_rows: int
    derivation_run_id: UUID
    projection_checkpoint: str


class CommunityLineageService:
    def __init__(self, *, memory: MemoryEngine, lineage: CommunityLineageRepository) -> None:
        self._memory = memory
        self._lineage = lineage

    async def build(self, *, group_id: str) -> CommunityBuildReport:
        facts = await self._lineage.active_facts(group_id=group_id)
        projection_checkpoint = _fact_set_hash(facts)
        communities = await self._memory.build_communities(group_id=group_id)
        run_id = uuid7()
        lineage_rows = 0
        governed = 0

        for community in communities:
            members = set(community.member_names)
            source_facts = tuple(
                fact
                for fact in facts
                if fact.subject_name in members or fact.object_name in members
            )
            if not source_facts:
                continue
            source_hash = _fact_set_hash(source_facts)
            community_id = UUID(community.community_id)
            fact_ids = tuple(fact.fact_id for fact in source_facts)
            await self._lineage.record(
                group_id=group_id,
                community_id=community_id,
                derivation_run_id=run_id,
                fact_ids=fact_ids,
            )
            await self._memory.annotate_community(
                group_id=group_id,
                community_id=community.community_id,
                derivation_run_id=str(run_id),
                source_fact_set_hash=source_hash,
                projection_checkpoint=projection_checkpoint,
            )
            lineage_rows += len(fact_ids)
            governed += 1

        return CommunityBuildReport(
            communities=governed,
            lineage_rows=lineage_rows,
            derivation_run_id=run_id,
            projection_checkpoint=projection_checkpoint,
        )
