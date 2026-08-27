"""SqlAlchemyUnitOfWork: one session, one transaction, commit once.

Repositories are attached on entry and share the session, so an aggregate change and
its outbox row commit together. ``use_tenant`` sets the RLS tenant for the
transaction before any group-scoped table is touched.
"""

from __future__ import annotations

from types import TracebackType

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories import (
    SqlAlchemyArtifactRepository,
    SqlAlchemyCandidateClaimRepository,
    SqlAlchemyCanonicalEntityRepository,
    SqlAlchemyIdentityRepository,
    SqlAlchemyKnowledgeSourceRepository,
    SqlAlchemyOntologyRepository,
    SqlAlchemyOutboxRepository,
    SqlAlchemyPublishedEpisodeRepository,
    SqlAlchemyRetrievalFeedbackRepository,
    SqlAlchemyReviewRepository,
    SqlAlchemyTenancyRepository,
)
from vera.domain.ports.curation import (
    ArtifactRepository,
    CandidateClaimRepository,
    KnowledgeSourceRepository,
    PublishedEpisodeRepository,
    ReviewRepository,
)
from vera.domain.ports.identity import IdentityRepository
from vera.domain.ports.ontology import OntologyRepository
from vera.domain.ports.repositories import (
    CanonicalEntityRepository,
    OutboxRepository,
    TenancyRepository,
)
from vera.domain.ports.retrieval import RetrievalFeedbackRepository


class SqlAlchemyUnitOfWork:
    """Concrete ``UnitOfWork`` (satisfies the port structurally).

    Attributes are typed as the port protocols so the whole object matches the
    ``UnitOfWork`` protocol (whose mutable members are invariant).
    """

    session: AsyncSession
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

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def __aenter__(self) -> SqlAlchemyUnitOfWork:
        self.session = self._session_factory()
        self.tenancy = SqlAlchemyTenancyRepository(self.session)
        self.identity = SqlAlchemyIdentityRepository(self.session)
        self.outbox = SqlAlchemyOutboxRepository(self.session)
        self.canonical = SqlAlchemyCanonicalEntityRepository(self.session)
        self.sources = SqlAlchemyKnowledgeSourceRepository(self.session)
        self.artifacts = SqlAlchemyArtifactRepository(self.session)
        self.claims = SqlAlchemyCandidateClaimRepository(self.session)
        self.reviews = SqlAlchemyReviewRepository(self.session)
        self.episodes = SqlAlchemyPublishedEpisodeRepository(self.session)
        self.feedback = SqlAlchemyRetrievalFeedbackRepository(self.session)
        self.ontology = SqlAlchemyOntologyRepository(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            if exc_type is not None:
                await self.session.rollback()
        finally:
            await self.session.close()

    async def use_tenant(self, group_id: str) -> None:
        # Switch to the non-superuser app role so row-level security is enforced even
        # when the connection logs in as a superuser (local dev). In production the
        # login role is already vera_app, so this is a no-op. Both statements are
        # transaction-scoped and safe under PgBouncer transaction pooling.
        await self.session.execute(text("SET LOCAL ROLE vera_app"))
        await self.session.execute(
            text("SELECT set_config('vera.group_id', :gid, true)"), {"gid": group_id}
        )

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
