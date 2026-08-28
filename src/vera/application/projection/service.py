"""FactProjectionService: rebuild the graph projection from Postgres and verify equivalence.

A rebuild clears the group's graph and re-projects every active fact, so the projection is
provably reconstructable from the authoritative store (ADR-0003, invariant 10). Verification
compares the two fact-key sets and reports drift, which is how a projection-health check and
the rebuild-equivalence tests detect divergence.
"""

from __future__ import annotations

from vera.domain.ports.projection import (
    FactProjection,
    ProjectionDrift,
    ProjectionSource,
)


class FactProjectionService:
    def __init__(self, *, source: ProjectionSource, projection: FactProjection) -> None:
        self._source = source
        self._projection = projection

    async def rebuild_group(self, group_id: str) -> int:
        """Clear and re-project a group's active facts. Returns the number projected."""
        await self._projection.clear(group_id=group_id)
        facts = await self._source.active_facts(group_id=group_id)
        for fact in facts:
            await self._projection.project(fact)
        return len(facts)

    async def project_group(self, group_id: str) -> int:
        """Converge the graph to the authoritative active fact set without a full rebuild."""
        facts = await self._source.active_facts(group_id=group_id)
        active_keys = {fact.fact_key for fact in facts}
        projected_keys = await self._projection.projected_fact_keys(group_id=group_id)
        for fact_key in sorted(projected_keys - active_keys):
            await self._projection.remove(group_id=group_id, fact_key=fact_key)
        for fact in facts:
            await self._projection.project(fact)
        return len(facts)

    async def verify_group(self, group_id: str) -> ProjectionDrift:
        authoritative = await self._source.active_fact_keys(group_id=group_id)
        projected = await self._projection.projected_fact_keys(group_id=group_id)
        return ProjectionDrift(
            group_id=group_id,
            missing_in_graph=frozenset(authoritative - projected),
            extra_in_graph=frozenset(projected - authoritative),
        )
