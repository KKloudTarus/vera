"""Ports for projecting the authoritative fact store into the graph (Phase 3).

Graphiti is a rebuildable projection, never a source of truth (ADR-0003). ``ProjectionSource``
reads the active fact set from Postgres; ``FactProjection`` writes it into the graph as
temporal edges carrying fact provenance. ``FactProjectionService`` (application) drives a
rebuild and verifies equivalence between the two.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ProjectedFact:
    """One active fact as it should appear in the graph: a temporal edge between the subject
    and object entities, carrying its content key, validity, aggregates, and the ids of the
    source episodes that currently support it.
    """

    group_id: str
    fact_key: str
    subject_name: str
    predicate: str
    object_name: str
    fact_text: str
    authority: float
    confidence: float
    valid_at: datetime | None = None
    invalid_at: datetime | None = None
    supporting_episode_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ProjectionDrift:
    """The difference between the authoritative active fact set and what the graph holds."""

    group_id: str
    missing_in_graph: frozenset[str]  # active in Postgres, absent from the projection
    extra_in_graph: frozenset[str]  # present in the projection, not active in Postgres

    @property
    def in_sync(self) -> bool:
        return not self.missing_in_graph and not self.extra_in_graph


class ProjectionSource(Protocol):
    """The authoritative side: the active facts that the graph must reproduce."""

    async def active_facts(self, *, group_id: str) -> list[ProjectedFact]: ...

    async def active_fact_keys(self, *, group_id: str) -> set[str]: ...


class FactProjection(Protocol):
    """The graph side: a non-authoritative projection of approved facts."""

    async def project(self, fact: ProjectedFact) -> None:
        """Upsert a fact's temporal edge idempotently by (group_id, fact_key)."""
        ...

    async def remove(self, *, group_id: str, fact_key: str) -> None: ...

    async def projected_fact_keys(self, *, group_id: str) -> set[str]: ...

    async def clear(self, *, group_id: str) -> None: ...
