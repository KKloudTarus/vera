"""Ports for the Knowledge Fabric persistence (Phase 1).

Repository Protocols for the authoritative fact model. Kept structural (typing.Protocol) so
adapters need only match the shape, and minimal to what Phase 1 tests and Phase 2
reconciliation require. Every method is group-scoped: callers pass a server-resolved
group_id and never an arbitrary tenant id.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from vera.domain.knowledge.fabric import (
    Assertion,
    Chunk,
    ChunkEmbedding,
    Evidence,
    ExtractionRun,
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


class ChunkEmbeddingRepository(Protocol):
    async def upsert(self, embedding: ChunkEmbedding) -> None: ...

    async def set_active_model(
        self,
        *,
        group_id: str,
        provider: str,
        model: str,
        model_version: str,
    ) -> None: ...


class ExtractionRunRepository(Protocol):
    async def add(self, run: ExtractionRun) -> ExtractionRun: ...

    async def get(self, *, group_id: str, run_id: str) -> ExtractionRun | None: ...


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

    async def set_expiry(
        self, *, group_id: str, fact_id: str, expires_at: datetime | None
    ) -> None: ...


class FactExpiryRepository(Protocol):
    async def expire_due(self, *, at: datetime, limit: int = 1000) -> list[Fact]:
        """Expire active facts across scopes in the privileged worker path."""
        ...


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
