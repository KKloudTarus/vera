"""Snapshot and context-pack application services (Phase 5).

SnapshotService freezes and reads immutable snapshots. ContextPackService assembles a bounded,
cited pack for a task, optionally against a snapshot (so the pack is reproducible after newer
knowledge arrives), serializes the assembled result, and persists it. The pack carries the
conflict and freshness counts the assembler reports.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Literal

from vera.application.retrieval.context_assembler import ContextAssembler, ScoredCandidate
from vera.domain.ports.retrieval_index import RetrievalFilters
from vera.domain.ports.snapshot import (
    ContextPack,
    ContextPackRepository,
    Snapshot,
    SnapshotRepository,
)
from vera.shared.time import utc_now
from vera.shared.types import JsonDict

_POLICY_VERSION = "ontology-v1"
_ASSEMBLER_VERSION = "context-assembler-v1"
_PACK_TTL = timedelta(days=30)


class ContextPackExpiredError(Exception):
    pass


def _serialize(
    candidate: ScoredCandidate, *, citation_mode: Literal["full", "compact"]
) -> JsonDict:
    citation = candidate.citation
    citation_payload: JsonDict = {
        "kind": citation.kind,
        "ref": citation.ref,
        "artifact_version_id": citation.artifact_version_id,
    }
    if citation_mode == "full":
        citation_payload.update(
            {
                "excerpt": citation.excerpt,
                "heading_path": citation.heading_path,
                "start_offset": citation.start_offset,
                "end_offset": citation.end_offset,
            }
        )
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
        "citation": citation_payload,
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
        filters: RetrievalFilters | None = None,
        citation_mode: Literal["full", "compact"] = "full",
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
            filters=filters,
        )
        results = [
            _serialize(candidate, citation_mode=citation_mode) for candidate in assembled.results
        ]
        request = {
            "group_id": group_id,
            "query": query,
            "snapshot_id": snapshot_id,
            "hints": hints or {},
            "limit": limit,
            "token_budget": token_budget,
            "as_of": as_of.isoformat() if as_of else None,
            "filters": asdict(filters) if filters else {},
            "citation_mode": citation_mode,
        }
        request_hash = hashlib.sha256(
            json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
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
            results=results,
            request_hash=request_hash,
            result_references=[str(result["ref"]) for result in results],
            expires_at=utc_now() + _PACK_TTL,
            assembler_version=_ASSEMBLER_VERSION,
            actor=actor,
        )

    async def get(self, *, group_id: str, pack_id: str) -> ContextPack | None:
        pack = await self._packs.get(group_id=group_id, pack_id=pack_id)
        if pack is not None and pack.expires_at <= utc_now():
            raise ContextPackExpiredError(f"context pack {pack_id} expired")
        return pack
