"""Ports for the Knowledge Fabric persistence (Phase 1).

Repository Protocols for the authoritative fact model. Kept structural (typing.Protocol) so
adapters need only match the shape, and minimal to what Phase 1 tests and Phase 2
reconciliation require. Every method is group-scoped: callers pass a server-resolved
group_id and never an arbitrary tenant id.
"""

from __future__ import annotations

from typing import Protocol

from vera.domain.knowledge.fabric import (
    Assertion,
    Chunk,
    Evidence,
    Fact,
    FactLifecycle,
    FactRelation,
    KnowledgeEvent,
)


class ChunkRepository(Protocol):
    async def upsert(self, chunk: Chunk) -> Chunk:
        """Insert a chunk, or return the existing one on chunk_key conflict (idempotent)."""
        ...

    async def by_artifact_version(
        self, *, group_id: str, artifact_version_id: str
    ) -> list[Chunk]: ...

    async def get(self, *, group_id: str, chunk_id: str) -> Chunk | None: ...


class FactRepository(Protocol):
    async def upsert(self, fact: Fact) -> Fact:
        """Insert a fact, or return the existing active fact for its fact_key (idempotent)."""
        ...

    async def active_by_fact_key(self, *, group_id: str, fact_key: str) -> Fact | None: ...

    async def by_fact_key(self, *, group_id: str, fact_key: str) -> Fact | None:
        """The most recent fact for this key in any lifecycle state, or None."""
        ...

    async def active_by_slot_key(self, *, group_id: str, slot_key: str) -> list[Fact]: ...

    async def get(self, *, group_id: str, fact_id: str) -> Fact | None: ...

    async def set_lifecycle(self, *, group_id: str, fact_id: str, state: FactLifecycle) -> None: ...

    async def set_aggregates(
        self, *, group_id: str, fact_id: str, authority: float, confidence: float
    ) -> None: ...


class AssertionRepository(Protocol):
    async def upsert(self, assertion: Assertion) -> Assertion:
        """Insert, or reaffirm (update recorded_at, reactivate) on the unique source key."""
        ...

    async def active_for_fact(self, *, group_id: str, fact_id: str) -> list[Assertion]: ...

    async def active_for_artifact(self, *, group_id: str, artifact_id: str) -> list[Assertion]: ...

    async def withdraw(self, *, group_id: str, assertion_id: str) -> None: ...


class EvidenceRepository(Protocol):
    async def add(self, evidence: Evidence) -> Evidence:
        """Add evidence, or return the existing row on (assertion_id, content_hash) conflict."""
        ...

    async def for_assertion(self, *, group_id: str, assertion_id: str) -> list[Evidence]: ...


class FactRelationRepository(Protocol):
    async def add(self, relation: FactRelation) -> FactRelation: ...

    async def from_fact(self, *, group_id: str, fact_id: str) -> list[FactRelation]: ...


class KnowledgeEventLog(Protocol):
    async def append(self, event: KnowledgeEvent) -> KnowledgeEvent: ...

    async def recent(self, *, group_id: str, limit: int = 100) -> list[KnowledgeEvent]: ...
