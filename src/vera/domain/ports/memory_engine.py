"""The ``MemoryEngine`` port: VERA's anti-corruption boundary around Graphiti.

The port speaks **only VERA vocabulary**. No ``graphiti_core`` type ever crosses
it. The adapter (``vera.adapters.graph``) is the single place that imports
Graphiti and owns the mapping between Graphiti UUIDs and VERA's durable keys
``(source_id, canonical_entity_id)``.

Swap test: replacing Graphiti means rewriting ``adapters/graph/`` and rebuilding
the graph from Postgres+S3, with zero changes to domain or application.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from vera.shared.types import (
    CanonicalEntityId,
    GroupId,
    JsonDict,
    SourceId,
    empty_json,
)


@dataclass(frozen=True, slots=True)
class EpisodeSpec:
    """A verified, ready-to-ingest episode (candidate curation already happened)."""

    source_id: SourceId
    group_id: GroupId
    body: str
    reference_time: datetime
    knowledge_type: str = "text"
    metadata: JsonDict = field(default_factory=empty_json)


@dataclass(frozen=True, slots=True)
class GraphNodeRef:
    """A node the engine created or matched, enough to stitch it to a canonical entity."""

    uuid: str
    name: str
    entity_type: str


@dataclass(frozen=True, slots=True)
class IngestReceipt:
    """What the engine created/matched, so VERA can update its durable-key index."""

    episode_uuid: str
    nodes: tuple[GraphNodeRef, ...] = ()
    edge_uuids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GraphQuery:
    """A retrieval request in VERA terms (no Graphiti search-config knobs leak here)."""

    text: str
    group_ids: tuple[GroupId, ...]
    limit: int = 30
    as_of: datetime | None = None  # valid-time point-in-time; adapter builds the filter


@dataclass(frozen=True, slots=True)
class GraphHit:
    """A single candidate from stage-1 retrieval (VERA re-ranks these in stage 2)."""

    fact: str
    score: float
    edge_uuid: str | None = None
    canonical_entity_id: CanonicalEntityId | None = None
    valid_at: datetime | None = None
    invalid_at: datetime | None = None
    source_id: SourceId | None = None


class MemoryEngine(Protocol):
    """Candidate generation + ingestion. Idempotency and re-rank live in VERA."""

    async def ingest_episode(self, episode: EpisodeSpec) -> IngestReceipt:
        """Ingest one verified episode. Callers guarantee idempotency upstream."""
        ...

    async def search(self, query: GraphQuery) -> Sequence[GraphHit]:
        """Stage-1 hybrid retrieval. Returns candidates; VERA owns the final rank."""
        ...

    async def health(self) -> bool:
        """Cheap liveness probe of the underlying graph."""
        ...

    async def clear_group(self, group_id: str) -> None:
        """Delete all graph data for one group, so it can be rebuilt from Postgres."""
        ...
