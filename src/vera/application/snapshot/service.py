"""Snapshot and context-pack application services (Phase 5).

SnapshotService freezes and reads immutable snapshots. ContextPackService assembles a bounded,
cited pack for a task, optionally against a snapshot (so the pack is reproducible after newer
knowledge arrives), serializes the assembled result, and persists it. The pack carries the
conflict and freshness counts the assembler reports.
"""

from __future__ import annotations

from datetime import datetime

from vera.application.retrieval.context_assembler import ContextAssembler, ScoredCandidate
from vera.domain.ports.snapshot import (
    ContextPack,
    ContextPackRepository,
    Snapshot,
    SnapshotRepository,
)
from vera.shared.types import JsonDict

_POLICY_VERSION = "ontology-v1"


def _serialize(candidate: ScoredCandidate) -> JsonDict:
    citation = candidate.citation
    return {
        "kind": candidate.kind,
        "ref": candidate.ref,
        "text": candidate.text,
        "score": candidate.score,
        "conflict": candidate.conflict,
        "reason": candidate.reason,
        "signals": {
            "relevance": candidate.signals.relevance,
            "authority": candidate.signals.authority,
            "verification": candidate.signals.verification,
            "recency": candidate.signals.recency,
            "confidence": candidate.signals.confidence,
        },
        "citation": {
            "kind": citation.kind,
            "ref": citation.ref,
            "excerpt": citation.excerpt,
            "heading_path": citation.heading_path,
            "artifact_version_id": citation.artifact_version_id,
            "start_offset": citation.start_offset,
            "end_offset": citation.end_offset,
        },
    }


class SnapshotService:
    def __init__(self, *, snapshots: SnapshotRepository) -> None:
        self._snapshots = snapshots

    async def create(
        self,
        *,
        group_id: str,
        as_of: datetime | None = None,
        ontology_version_id: str | None = None,
        embedding_version: JsonDict | None = None,
        retrieval_index_version: str = "fts-v1",
        actor: str | None = None,
    ) -> Snapshot:
        return await self._snapshots.create(
            group_id=group_id,
            policy_version=_POLICY_VERSION,
            as_of=as_of,
            ontology_version_id=ontology_version_id,
            embedding_version=embedding_version,
            retrieval_index_version=retrieval_index_version,
            actor=actor,
        )

    async def get(self, *, group_id: str, snapshot_id: str) -> Snapshot | None:
        return await self._snapshots.get(group_id=group_id, snapshot_id=snapshot_id)


class ContextPackService:
    def __init__(
        self,
        *,
        assembler: ContextAssembler,
        snapshots: SnapshotRepository,
        packs: ContextPackRepository,
    ) -> None:
        self._assembler = assembler
        self._snapshots = snapshots
        self._packs = packs

    async def create(
        self,
        *,
        group_id: str,
        query: str,
        snapshot_id: str | None = None,
        hints: JsonDict | None = None,
        limit: int = 10,
        token_budget: int = 2000,
        as_of: datetime | None = None,
        actor: str | None = None,
    ) -> ContextPack:
        fact_ids: set[str] | None = None
        passage_cutoff: datetime | None = None
        if snapshot_id is not None:
            fact_ids = await self._snapshots.fact_ids(group_id=group_id, snapshot_id=snapshot_id)
            snapshot = await self._snapshots.get(group_id=group_id, snapshot_id=snapshot_id)
            if snapshot is not None:
                # Freeze passages to what existed when the snapshot was taken. System time (the
                # snapshot's transaction time) is the right cutoff: chunks ingested later did not
                # exist then, so excluding them reproduces the passages retrieval saw at snapshot.
                passage_cutoff = snapshot.frozen_at_system_time
        assembled = await self._assembler.assemble(
            query=query,
            group_id=group_id,
            limit=limit,
            token_budget=token_budget,
            as_of=as_of,
            snapshot_fact_ids=fact_ids,
            passage_cutoff=passage_cutoff,
        )
        return await self._packs.save(
            group_id=group_id,
            query=query,
            snapshot_id=snapshot_id,
            hints=hints,
            token_estimate=assembled.token_estimate,
            result_count=len(assembled.results),
            omitted=assembled.omitted,
            conflicts=assembled.conflicts,
            freshness_warnings=assembled.freshness_warnings,
            results=[_serialize(c) for c in assembled.results],
            actor=actor,
        )

    async def get(self, *, group_id: str, pack_id: str) -> ContextPack | None:
        return await self._packs.get(group_id=group_id, pack_id=pack_id)
