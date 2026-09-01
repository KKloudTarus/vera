"""Ports for immutable knowledge snapshots and persisted context packs (Phase 5).

A snapshot freezes the set of active fact revisions (plus the ontology/policy versions and
source-revision boundaries) so a workflow can query reproducibly even after newer knowledge
arrives. A context pack is a bounded, cited response assembled for one task, stored for
audit and replay. Ports stay free of application types: the pack repository takes primitive
fields, and the application layer serializes the assembled result before saving.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import TracebackType
from typing import Protocol

from vera.shared.types import JsonDict, empty_json


def _empty_results() -> list[JsonDict]:
    return []


@dataclass(frozen=True, slots=True)
class Snapshot:
    id: str
    group_id: str
    created_at: datetime
    frozen_at_system_time: datetime
    as_of_valid_time: datetime
    policy_version: str
    fact_count: int
    retrieval_frozen: bool
    ontology_version_id: str | None = None
    source_boundaries: JsonDict = field(default_factory=empty_json)
    embedding_version: JsonDict = field(default_factory=empty_json)
    retrieval_index_version: str = "fts-v1"
    assembler_version: str = "context-assembler-v3"
    graph_projection_checkpoint: str | None = None


@dataclass(frozen=True, slots=True)
class ContextPack:
    id: str | None
    group_id: str
    created_at: datetime
    query: str
    token_estimate: int
    result_count: int
    omitted: int
    conflicts: int
    freshness_warnings: int
    request_hash: str
    result_references: list[str]
    expires_at: datetime
    assembler_version: str
    request: JsonDict = field(default_factory=empty_json)
    snapshot_id: str | None = None
    results: list[JsonDict] = field(default_factory=_empty_results)


class SnapshotRepository(Protocol):
    async def create(
        self,
        *,
        group_id: str,
        policy_version: str,
        as_of: datetime | None = None,
        ontology_version_id: str | None = None,
        embedding_version: JsonDict | None = None,
        retrieval_index_version: str = "fts-v1",
        assembler_version: str = "context-assembler-v3",
        actor: str | None = None,
    ) -> Snapshot:
        """Freeze the active fact set and record the snapshot; append SNAPSHOT_CREATED."""
        ...

    async def get(self, *, group_id: str, snapshot_id: str) -> Snapshot | None: ...

    async def fact_ids(self, *, group_id: str, snapshot_id: str) -> set[str]: ...


class ContextPackRepository(Protocol):
    async def prepare_save(self, *, group_id: str) -> int:
        """Serialize writers, prune expired packs, and return the remaining pack count."""
        ...

    async def equivalent(
        self,
        *,
        group_id: str,
        request_hash: str,
        result_references: list[str],
        results: list[JsonDict],
        assembler_version: str,
    ) -> ContextPack | None:
        """Return the same unexpired response produced by the same assembler contract."""
        ...

    async def save(
        self,
        *,
        group_id: str,
        query: str,
        token_estimate: int,
        result_count: int,
        omitted: int,
        conflicts: int,
        freshness_warnings: int,
        results: list[JsonDict],
        request_hash: str,
        result_references: list[str],
        expires_at: datetime,
        assembler_version: str,
        request: JsonDict,
        snapshot_id: str | None = None,
        hints: JsonDict | None = None,
        actor: str | None = None,
    ) -> ContextPack:
        """Persist a context pack; append CONTEXT_PACK_CREATED."""
        ...

    async def get(self, *, group_id: str, pack_id: str) -> ContextPack | None: ...


class SnapshotUnitOfWork(Protocol):
    snapshots: SnapshotRepository
    context_packs: ContextPackRepository

    async def __aenter__(self) -> SnapshotUnitOfWork: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def set_repeatable_read(self) -> None: ...

    async def use_tenant(self, group_id: str) -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...
