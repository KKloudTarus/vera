"""Snapshot and context-pack application services (Phase 5).

SnapshotService freezes and reads immutable snapshots. ContextPackService assembles a bounded,
cited pack for a task, optionally against a snapshot (so the pack is reproducible after newer
knowledge arrives), serializes the assembled result, and persists it. The pack carries the
conflict and freshness counts the assembler reports.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Literal, cast
from uuid import UUID

from vera.application.retrieval.context_assembler import ContextAssembler, ScoredCandidate
from vera.domain.ontology.registry import ONTOLOGY_VERSION
from vera.domain.ports.retrieval_index import RetrievalFilters
from vera.domain.ports.snapshot import ContextPack, Snapshot, SnapshotUnitOfWork
from vera.shared.time import utc_now
from vera.shared.types import JsonDict

_POLICY_VERSION = f"ontology-v{ONTOLOGY_VERSION}"
# Bump for any scoring algorithm, default RetrievalWeights, or packing behavior change.
_ASSEMBLER_VERSION = "context-assembler-v2"
_PACK_TTL = timedelta(days=30)


class ContextPackExpiredError(Exception):
    pass


class SnapshotNotFoundError(Exception):
    pass


class SnapshotNotReproducibleError(Exception):
    pass


def serialize_candidate(
    candidate: ScoredCandidate, *, citation_mode: Literal["full", "compact"]
) -> JsonDict:
    citation = candidate.citation
    citation_payload: JsonDict = {
        "kind": citation.kind,
        "ref": citation.ref,
        "evidence_id": citation.evidence_id,
        "assertion_id": citation.assertion_id,
        "source_id": citation.source_id,
        "chunk_id": citation.chunk_id,
        "artifact_version_id": citation.artifact_version_id,
    }
    if citation_mode == "full":
        citation_payload.update(
            {
                "excerpt": citation.excerpt,
                "heading_path": citation.heading_path,
                "start_offset": citation.start_offset,
                "end_offset": citation.end_offset,
                "quote_hash": citation.quote_hash,
                "content_hash": citation.content_hash,
                "extraction_run_id": citation.extraction_run_id,
                "source_coordinates": citation.source_coordinates,
                "structured_record": citation.structured_record,
                "citation_uri": citation.citation_uri,
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
    def __init__(self, *, uow_factory: Callable[[], SnapshotUnitOfWork]) -> None:
        self._uow_factory = uow_factory

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
        version = embedding_version or {}
        required = {"provider", "model", "model_version", "dimension"}
        if version and set(version) != required:
            raise ValueError(
                "embedding_version must identify provider, model, version, and dimension"
            )
        async with self._uow_factory() as uow:
            await uow.set_repeatable_read()
            await uow.use_tenant(group_id)
            snapshot = await uow.snapshots.create(
                group_id=group_id,
                policy_version=_POLICY_VERSION,
                as_of=as_of,
                ontology_version_id=ontology_version_id,
                embedding_version=version,
                retrieval_index_version=retrieval_index_version,
                assembler_version=_ASSEMBLER_VERSION,
                actor=actor,
            )
            await uow.commit()
            return snapshot

    async def get(self, *, group_id: str, snapshot_id: str) -> Snapshot | None:
        async with self._uow_factory() as uow:
            await uow.use_tenant(group_id)
            return await uow.snapshots.get(group_id=group_id, snapshot_id=snapshot_id)


class ContextPackService:
    def __init__(
        self,
        *,
        assembler: ContextAssembler,
        uow_factory: Callable[[], SnapshotUnitOfWork],
    ) -> None:
        self._assembler = assembler
        self._uow_factory = uow_factory

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
        active_embedding_version: JsonDict | None = None,
        active_retrieval_index_version: str | None = None,
        actor: str | None = None,
    ) -> ContextPack:
        passage_cutoff: datetime | None = None
        if snapshot_id is not None:
            try:
                snapshot_id = str(UUID(snapshot_id))
            except ValueError as exc:
                raise SnapshotNotFoundError(f"snapshot {snapshot_id} was not found") from exc
            async with self._uow_factory() as uow:
                await uow.use_tenant(group_id)
                snapshot = await uow.snapshots.get(group_id=group_id, snapshot_id=snapshot_id)
            if snapshot is None:
                raise SnapshotNotFoundError(f"snapshot {snapshot_id} was not found")
            if not snapshot.retrieval_frozen:
                raise SnapshotNotReproducibleError(
                    f"snapshot {snapshot_id} predates frozen retrieval inputs"
                )
            if (
                active_embedding_version is not None
                and snapshot.embedding_version != active_embedding_version
            ):
                raise SnapshotNotReproducibleError(
                    f"snapshot {snapshot_id} requires a different embedding version"
                )
            if (
                active_retrieval_index_version is not None
                and snapshot.retrieval_index_version != active_retrieval_index_version
            ):
                raise SnapshotNotReproducibleError(
                    f"snapshot {snapshot_id} requires a different retrieval index version"
                )
            if snapshot.assembler_version != _ASSEMBLER_VERSION:
                raise SnapshotNotReproducibleError(
                    f"snapshot {snapshot_id} requires a different assembler version"
                )
            if as_of is not None and as_of != snapshot.as_of_valid_time:
                raise SnapshotNotReproducibleError(
                    f"snapshot {snapshot_id} is pinned to a different valid-time boundary"
                )
            as_of = snapshot.as_of_valid_time
            passage_cutoff = snapshot.frozen_at_system_time
        assembled = await self._assembler.assemble(
            query=query,
            group_id=group_id,
            limit=limit,
            token_budget=token_budget,
            as_of=as_of,
            snapshot_fact_ids=None,
            snapshot_id=snapshot_id,
            passage_cutoff=passage_cutoff,
            filters=filters,
            citation_mode=citation_mode,
        )
        results = [
            serialize_candidate(candidate, citation_mode=citation_mode)
            for candidate in assembled.results
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
        canonical_request = json.dumps(request, sort_keys=True, separators=(",", ":"))
        normalized_request = cast("JsonDict", json.loads(canonical_request))
        request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
        async with self._uow_factory() as uow:
            await uow.use_tenant(group_id)
            pack = await uow.context_packs.save(
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
                request=normalized_request,
                actor=actor,
            )
            await uow.commit()
            return pack

    async def get(self, *, group_id: str, pack_id: str) -> ContextPack | None:
        async with self._uow_factory() as uow:
            await uow.use_tenant(group_id)
            pack = await uow.context_packs.get(group_id=group_id, pack_id=pack_id)
        if pack is not None and pack.expires_at <= utc_now():
            raise ContextPackExpiredError(f"context pack {pack_id} expired")
        return pack
