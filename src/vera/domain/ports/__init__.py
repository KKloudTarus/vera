"""Ports: the interfaces the application depends on, implemented by adapters.

Defined as :class:`typing.Protocol` (structural typing): adapters satisfy a port
by shape, never by inheritance, so nothing in the domain needs to know an adapter
exists and test fakes inherit nothing.
"""

from vera.domain.ports.job_queue import JobQueue, QueuedJob
from vera.domain.ports.memory_engine import (
    EpisodeSpec,
    GraphHit,
    GraphNodeRef,
    GraphQuery,
    IngestReceipt,
    MemoryEngine,
)
from vera.domain.ports.object_store import ObjectStore, StoredObject
from vera.domain.ports.repositories import (
    CanonicalEntityRepository,
    OutboxRepository,
    TenancyRepository,
)
from vera.domain.ports.unit_of_work import UnitOfWork

__all__ = [
    "CanonicalEntityRepository",
    "EpisodeSpec",
    "GraphHit",
    "GraphNodeRef",
    "GraphQuery",
    "IngestReceipt",
    "JobQueue",
    "MemoryEngine",
    "ObjectStore",
    "OutboxRepository",
    "QueuedJob",
    "StoredObject",
    "TenancyRepository",
    "UnitOfWork",
]
