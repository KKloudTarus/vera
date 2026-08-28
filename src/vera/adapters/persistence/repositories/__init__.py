"""Concrete repositories. Each maps ORM rows to domain objects at its boundary so
the application and domain never see SQLAlchemy.
"""

from vera.adapters.persistence.repositories.canonical import (
    SqlAlchemyCanonicalEntityRepository,
)
from vera.adapters.persistence.repositories.chunk_embedding import (
    SqlAlchemyChunkEmbeddingRepository,
)
from vera.adapters.persistence.repositories.curation import (
    SqlAlchemyArtifactRepository,
    SqlAlchemyCandidateClaimRepository,
    SqlAlchemyKnowledgeSourceRepository,
    SqlAlchemyPublishedEpisodeRepository,
    SqlAlchemyReviewRepository,
)
from vera.adapters.persistence.repositories.fabric import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyChunkRepository,
    SqlAlchemyEvidenceRepository,
    SqlAlchemyExtractionRunRepository,
    SqlAlchemyFactRelationRepository,
    SqlAlchemyFactRepository,
    SqlAlchemyKnowledgeEventLog,
)
from vera.adapters.persistence.repositories.graph_map import SqlAlchemyGraphMapRepository
from vera.adapters.persistence.repositories.identity import SqlAlchemyIdentityRepository
from vera.adapters.persistence.repositories.ontology import SqlAlchemyOntologyRepository
from vera.adapters.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from vera.adapters.persistence.repositories.retrieval import (
    SqlAlchemyRetrievalFeedbackRepository,
    SqlAlchemyRetrievalReadModel,
)
from vera.adapters.persistence.repositories.tenancy import SqlAlchemyTenancyRepository

__all__ = [
    "SqlAlchemyArtifactRepository",
    "SqlAlchemyAssertionRepository",
    "SqlAlchemyCandidateClaimRepository",
    "SqlAlchemyCanonicalEntityRepository",
    "SqlAlchemyChunkEmbeddingRepository",
    "SqlAlchemyChunkRepository",
    "SqlAlchemyEvidenceRepository",
    "SqlAlchemyExtractionRunRepository",
    "SqlAlchemyFactRelationRepository",
    "SqlAlchemyFactRepository",
    "SqlAlchemyGraphMapRepository",
    "SqlAlchemyIdentityRepository",
    "SqlAlchemyKnowledgeEventLog",
    "SqlAlchemyKnowledgeSourceRepository",
    "SqlAlchemyOntologyRepository",
    "SqlAlchemyOutboxRepository",
    "SqlAlchemyPublishedEpisodeRepository",
    "SqlAlchemyRetrievalFeedbackRepository",
    "SqlAlchemyRetrievalReadModel",
    "SqlAlchemyReviewRepository",
    "SqlAlchemyTenancyRepository",
]
