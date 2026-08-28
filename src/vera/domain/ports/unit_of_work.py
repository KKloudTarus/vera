"""The ``UnitOfWork`` port: the transaction boundary.

A use-case runs inside one UoW = one transaction. Repositories add and read
aggregates; they never commit. The application layer commits exactly once, so an
aggregate change and its outbox row are written atomically. ``use_tenant`` sets the
row-level-security tenant for the transaction before touching group-scoped tables.
"""

from __future__ import annotations

from types import TracebackType
from typing import Protocol, runtime_checkable

from vera.domain.ports.curation import (
    ArtifactRepository,
    CandidateClaimRepository,
    KnowledgeSourceRepository,
    PublishedEpisodeRepository,
    ReviewRepository,
)
from vera.domain.ports.fabric import ChunkRepository, ExtractionRunRepository
from vera.domain.ports.identity import IdentityRepository
from vera.domain.ports.ontology import OntologyRepository
from vera.domain.ports.repositories import (
    CanonicalEntityRepository,
    OutboxRepository,
    TenancyRepository,
)
from vera.domain.ports.retrieval import RetrievalFeedbackRepository


@runtime_checkable
class UnitOfWork(Protocol):
    tenancy: TenancyRepository
    identity: IdentityRepository
    outbox: OutboxRepository
    canonical: CanonicalEntityRepository
    sources: KnowledgeSourceRepository
    artifacts: ArtifactRepository
    claims: CandidateClaimRepository
    reviews: ReviewRepository
    episodes: PublishedEpisodeRepository
    feedback: RetrievalFeedbackRepository
    ontology: OntologyRepository
    chunks: ChunkRepository
    extraction_runs: ExtractionRunRepository

    async def __aenter__(self) -> UnitOfWork: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def use_tenant(self, group_id: str) -> None:
        """Bind the RLS tenant for this transaction (SET LOCAL vera.group_id)."""
        ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...
