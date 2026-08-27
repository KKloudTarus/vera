"""SupersedePolicy: the one authority for which prior facts a new claim replaces.

Every published claim flows through ``publish_claim`` and this policy, whether it came
from a structured triple or from free text the LLM extractor turned into a triple, so
contradiction handling is uniform across ingestion paths. The graph adapter never
decides contradictions on its own for curated writes; it only closes the edges this
policy names. A functional predicate (one value at a time, e.g. RUNS_ON) supersedes
every earlier value; a multi-valued predicate keeps coexisting values unless the
contradiction judge marks specific ones as truly replaced, and stays additive when no
judge is wired.
"""

from __future__ import annotations

from vera.domain.curation.models import ClaimRecord
from vera.domain.ontology import is_single_valued
from vera.domain.ports.curation import ContradictionJudge


class SupersedePolicy:
    def __init__(self, judge: ContradictionJudge | None = None) -> None:
        self._judge = judge

    async def contradicted(
        self,
        *,
        subject: str,
        predicate: str,
        new_object: str,
        conflicts: list[ClaimRecord],
    ) -> list[ClaimRecord]:
        """The subset of existing verified claims the new one actually replaces."""
        if not conflicts:
            return []
        if is_single_valued(predicate):
            return conflicts
        if self._judge is None:
            return []
        contradicted_objects = await self._judge.contradictions(
            subject=subject,
            predicate=predicate,
            new_object=new_object,
            existing_objects=[c.object for c in conflicts if c.object],
        )
        return [c for c in conflicts if c.object in contradicted_objects]
