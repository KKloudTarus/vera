"""Register every ORM table on the metadata used by Alembic and the application."""

from vera.adapters.persistence.models.canonical import (
    CanonicalEntityRow,
    EntityAliasRow,
    GraphEdgeMapRow,
    GraphNodeMapRow,
)
from vera.adapters.persistence.models.community import CommunityFactLineageRow
from vera.adapters.persistence.models.fabric import (
    AssertionRow,
    ChunkRow,
    EvidenceRow,
    ExtractionRunRow,
    FactRelationRow,
    FactRevisionRow,
    FactRow,
    KnowledgeEventRow,
)
from vera.adapters.persistence.models.identity import (
    CredentialRow,
    MembershipRow,
    PrincipalRow,
    ServiceAccountRow,
)
from vera.adapters.persistence.models.ingestion import IngestionJobRow
from vera.adapters.persistence.models.knowledge import (
    ArtifactRow,
    ArtifactVersionRow,
    CandidateClaimRow,
    KnowledgeSourceRow,
    PublishedEpisodeRow,
    ReviewRow,
)
from vera.adapters.persistence.models.legal_hold import LegalHoldRow
from vera.adapters.persistence.models.ops import (
    AuditEventRow,
    LlmUsageRow,
    OntologyVersionRow,
    ProposalAttemptRow,
    RetrievalFeedbackRow,
    SyncCursorRow,
    SyncJobRow,
)
from vera.adapters.persistence.models.tenancy import (
    OrganizationRow,
    ProjectRow,
    WorkspaceRow,
)

__all__ = [
    "ArtifactRow",
    "ArtifactVersionRow",
    "AssertionRow",
    "AuditEventRow",
    "CandidateClaimRow",
    "CanonicalEntityRow",
    "ChunkRow",
    "CommunityFactLineageRow",
    "CredentialRow",
    "EntityAliasRow",
    "EvidenceRow",
    "ExtractionRunRow",
    "FactRelationRow",
    "FactRevisionRow",
    "FactRow",
    "GraphEdgeMapRow",
    "GraphNodeMapRow",
    "IngestionJobRow",
    "KnowledgeEventRow",
    "KnowledgeSourceRow",
    "LegalHoldRow",
    "LlmUsageRow",
    "MembershipRow",
    "OntologyVersionRow",
    "OrganizationRow",
    "PrincipalRow",
    "ProjectRow",
    "ProposalAttemptRow",
    "PublishedEpisodeRow",
    "RetrievalFeedbackRow",
    "ReviewRow",
    "ServiceAccountRow",
    "SyncCursorRow",
    "SyncJobRow",
    "WorkspaceRow",
]
