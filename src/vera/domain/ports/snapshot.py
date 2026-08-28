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
from typing import Protocol

from vera.shared.types import JsonDict, empty_json


def _empty_results() -> list[JsonDict]:
    return []


@dataclass(frozen=True, slots=True)
class Snapshot:
    id: str
    group_id: str
    created_at: datetime
    as_of_system_time: datetime
    policy_version: str
    fact_count: int
    as_of_valid_time: datetime | None = None
    ontology_version_id: str | None = None
    source_boundaries: JsonDict = field(default_factory=empty_json)


@dataclass(frozen=True, slots=True)
class ContextPack:
    id: str
    group_id: str
    created_at: datetime
    query: str
    token_estimate: int
    result_count: int
    omitted: int
    conflicts: int
    freshness_warnings: int
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
        actor: str | None = None,
    ) -> Snapshot:
        """Freeze the active fact set and record the snapshot; append SNAPSHOT_CREATED."""
        ...

    async def get(self, *, group_id: str, snapshot_id: str) -> Snapshot | None: ...

    async def fact_ids(self, *, group_id: str, snapshot_id: str) -> set[str]: ...


class ContextPackRepository(Protocol):
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
        snapshot_id: str | None = None,
        hints: JsonDict | None = None,
        actor: str | None = None,
    ) -> ContextPack:
        """Persist a context pack; append CONTEXT_PACK_CREATED."""
        ...

    async def get(self, *, group_id: str, pack_id: str) -> ContextPack | None: ...
