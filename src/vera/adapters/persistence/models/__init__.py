"""ORM tables. Importing this package registers every table on ``Base.metadata``,
which is what Alembic autogenerate and the app rely on.
"""

from vera.adapters.persistence.models.canonical import (
    CanonicalEntityRow,
    EntityAliasRow,
    GraphEdgeMapRow,
    GraphNodeMapRow,
)
from vera.adapters.persistence.models.fabric import (
    AssertionRow,
    ChunkRow,
    EvidenceRow,
    FactRelationRow,
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
from vera.adapters.persistence.models.ops import (
    AuditEventRow,
    LlmUsageRow,
    OntologyVersionRow,
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
    "CredentialRow",
    "EntityAliasRow",
    "EvidenceRow",
    "FactRelationRow",
    "FactRow",
    "GraphEdgeMapRow",
    "GraphNodeMapRow",
    "IngestionJobRow",
    "KnowledgeEventRow",
    "KnowledgeSourceRow",
    "LlmUsageRow",
    "MembershipRow",
    "OntologyVersionRow",
    "OrganizationRow",
    "PrincipalRow",
    "ProjectRow",
    "PublishedEpisodeRow",
    "RetrievalFeedbackRow",
    "ReviewRow",
    "ServiceAccountRow",
    "SyncCursorRow",
    "SyncJobRow",
    "WorkspaceRow",
]
