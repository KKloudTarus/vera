"""Snapshot and context-pack persistence (Phase 5).

Runs on a trusted connection with explicit ``group_id`` filters (like the retrieval read
model). Snapshot creation captures the active fact revisions, the source-revision
boundaries, and a SNAPSHOT_CREATED event in one transaction; pack save persists the pack and
a CONTEXT_PACK_CREATED event in one transaction, so the ledger never diverges from the row.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from vera.domain.ports.snapshot import ContextPack, Snapshot
from vera.shared.types import JsonDict

_INSERT_SNAPSHOT = text(
    "INSERT INTO knowledge_snapshots "
    "(group_id, policy_version, as_of_valid_time, ontology_version_id, "
    "embedding_version, retrieval_index_version, assembler_version, "
    "graph_projection_checkpoint, retrieval_frozen) "
    "VALUES (:g, :pv, COALESCE(:vt, now()), :ov, CAST(:ev AS jsonb), :iv, :av, :gc, false) "
    "RETURNING id, created_at, frozen_at_system_time, as_of_valid_time"
)
_CAPTURE_ACTIVE = text(
    "INSERT INTO snapshot_facts "
    "(snapshot_id, fact_id, group_id, fact_key, subject_name, predicate, object_name, "
    "normalized_object, object_scalar, authority, confidence, lifecycle_state, valid_from) "
    "SELECT :sid, f.id, f.group_id, f.fact_key, cs.canonical_name, f.predicate, "
    "coalesce(co.canonical_name, f.object_scalar, ''), f.normalized_object, f.object_scalar, "
    "f.authority, f.confidence, "
    "f.lifecycle_state, f.valid_from FROM facts f "
    "JOIN canonical_entities cs ON cs.id = f.subject_entity_id AND cs.group_id = f.group_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id AND co.group_id = f.group_id "
    "WHERE f.group_id = :g AND f.lifecycle_state IN ('active', 'disputed') "
    "AND (f.valid_from IS NULL OR f.valid_from <= :vt) "
    "AND (f.valid_to IS NULL OR f.valid_to > :vt)"
)
_CAPTURE_AS_OF = text(
    "INSERT INTO snapshot_facts "
    "(snapshot_id, fact_id, group_id, fact_key, subject_name, predicate, object_name, "
    "normalized_object, object_scalar, authority, confidence, lifecycle_state, valid_from) "
    "SELECT :sid, f.id, f.group_id, f.fact_key, cs.canonical_name, f.predicate, "
    "coalesce(co.canonical_name, f.object_scalar, ''), f.normalized_object, f.object_scalar, "
    "f.authority, f.confidence, "
    "f.lifecycle_state, f.valid_from FROM facts f "
    "JOIN canonical_entities cs ON cs.id = f.subject_entity_id AND cs.group_id = f.group_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id AND co.group_id = f.group_id "
    "WHERE f.group_id = :g AND f.lifecycle_state <> 'proposed' "
    "AND (f.valid_from IS NULL OR f.valid_from <= :vt) "
    "AND (f.valid_to IS NULL OR f.valid_to > :vt) "
    "AND EXISTS (SELECT 1 FROM assertions a WHERE a.fact_id = f.id "
    "AND a.group_id = f.group_id AND a.polarity = 'supports' "
    "AND a.state <> 'needs_review' AND a.recorded_at <= :vt "
    "AND (a.withdrawn_at IS NULL OR a.withdrawn_at > :vt))"
)
_CAPTURE_CHUNKS = text(
    "INSERT INTO snapshot_chunks "
    "(snapshot_id, chunk_id, group_id, knowledge_source_id, artifact_version_id, text, "
    "content_hash, heading_path, symbol_name, start_offset, end_offset, page_number, "
    "start_line, end_line, created_at, search_vector) "
    "SELECT :sid, c.id, c.group_id, art.source_id, c.artifact_version_id, c.text, "
    "c.content_hash, c.heading_path, c.symbol_name, c.start_offset, c.end_offset, "
    "c.page_number, c.start_line, c.end_line, c.created_at, c.search_vector FROM chunks c "
    "JOIN artifact_versions av ON av.id = c.artifact_version_id "
    "JOIN artifacts art ON art.id = av.artifact_id "
    "JOIN knowledge_sources src ON src.id = art.source_id "
    "LEFT JOIN projects p ON p.id = src.project_id "
    "JOIN workspaces w ON w.id = src.workspace_id "
    "WHERE c.group_id = CAST(:g AS varchar) AND ("
    "(src.project_id IS NOT NULL AND p.group_id = c.group_id) OR "
    "(src.project_id IS NULL AND (w.group_id = c.group_id OR EXISTS ("
    "SELECT 1 FROM projects wp WHERE wp.workspace_id = src.workspace_id "
    "AND wp.group_id = c.group_id))))"
)
_CAPTURE_SOURCE_CONFIGS = text(
    "INSERT INTO snapshot_sources "
    "(snapshot_id, knowledge_source_id, group_id, repository, branch, document_type, "
    "source_type, trust_tier) SELECT :sid, src.id, CAST(:g AS varchar), "
    "src.config->>'repository', "
    "src.config->>'branch', src.config->>'document_type', src.kind, src.trust_tier "
    "FROM knowledge_sources src WHERE EXISTS ("
    "SELECT 1 FROM snapshot_chunks sc WHERE sc.snapshot_id = :sid "
    "AND sc.group_id = CAST(:g AS varchar) AND sc.knowledge_source_id = src.id)"
)
_CAPTURE_EMBEDDINGS = text(
    "INSERT INTO snapshot_chunk_embeddings "
    "(snapshot_id, chunk_id, group_id, provider, model, model_version, dimension, "
    "embedding, content_hash, created_at) "
    "SELECT sc.snapshot_id, sc.chunk_id, sc.group_id, ce.provider, ce.model, "
    "ce.model_version, ce.dimension, ce.embedding::text, ce.content_hash, ce.created_at "
    "FROM snapshot_chunks sc JOIN chunk_embeddings ce ON ce.chunk_id = sc.chunk_id "
    "AND ce.group_id = sc.group_id AND ce.content_hash = sc.content_hash "
    "WHERE sc.snapshot_id = :sid AND sc.group_id = CAST(:g AS varchar) "
    "AND ce.provider = :provider AND ce.model = :model "
    "AND ce.model_version = :model_version AND ce.dimension = :dimension AND ce.active"
)
_CAPTURE_SOURCES = text(
    "INSERT INTO snapshot_fact_sources "
    "(snapshot_id, fact_id, assertion_id, group_id, knowledge_source_id, "
    "artifact_version_id, assertion_recorded_at, repository, branch, document_type, "
    "source_type, trust_tier) "
    "SELECT sf.snapshot_id, sf.fact_id, a.id, sf.group_id, a.knowledge_source_id, "
    "a.artifact_version_id, a.recorded_at, src.config->>'repository', src.config->>'branch', "
    "src.config->>'document_type', "
    "src.kind, src.trust_tier FROM snapshot_facts sf "
    "JOIN assertions a ON a.fact_id = sf.fact_id AND a.group_id = sf.group_id "
    "AND a.polarity = 'supports' "
    "LEFT JOIN artifact_versions av ON av.id = a.artifact_version_id "
    "LEFT JOIN artifacts art ON art.id = av.artifact_id "
    "LEFT JOIN knowledge_sources src ON src.id = a.knowledge_source_id "
    "LEFT JOIN projects p ON p.id = src.project_id "
    "LEFT JOIN workspaces w ON w.id = src.workspace_id "
    "WHERE sf.snapshot_id = :sid AND sf.group_id = :g "
    "AND ((CAST(:historical AS boolean) AND a.state <> 'needs_review' "
    "AND a.recorded_at <= :vt AND (a.withdrawn_at IS NULL OR a.withdrawn_at > :vt)) "
    "OR (NOT CAST(:historical AS boolean) AND a.state = 'active')) "
    "AND ((a.knowledge_source_id IS NULL AND a.artifact_id IS NULL "
    "AND a.artifact_version_id IS NULL) OR (av.id IS NOT NULL AND art.id = a.artifact_id "
    "AND art.source_id = a.knowledge_source_id)) "
    "AND (a.knowledge_source_id IS NULL OR "
    "(src.project_id IS NOT NULL AND p.group_id = sf.group_id) OR "
    "(src.project_id IS NULL AND (w.group_id = sf.group_id OR EXISTS ("
    "SELECT 1 FROM projects wp WHERE wp.workspace_id = src.workspace_id "
    "AND wp.group_id = sf.group_id))))"
)
_CAPTURE_CITATIONS = text(
    "INSERT INTO snapshot_fact_citations "
    "(snapshot_id, evidence_id, fact_id, assertion_id, group_id, knowledge_source_id, "
    "excerpt, chunk_id, "
    "artifact_version_id, heading_path, quote_start, quote_end, quote_hash, content_hash, "
    "extraction_run_id, structured_record, citation_uri, source_coordinates, "
    "assertion_recorded_at, evidence_created_at) "
    "SELECT sfs.snapshot_id, e.id, sfs.fact_id, a.id, sfs.group_id, "
    "sfs.knowledge_source_id, COALESCE("
    "CASE WHEN c.id IS NOT NULL THEN "
    "substring(c.text FROM e.quote_start + 1 FOR e.quote_end - e.quote_start) END, "
    "e.excerpt, e.structured_record::text, e.citation_override, e.citation_uri), "
    "c.id, e.artifact_version_id, c.heading_path, e.quote_start, e.quote_end, e.quote_hash, "
    "e.content_hash, e.extraction_run_id, e.structured_record, "
    "COALESCE(e.citation_override, e.citation_uri), e.source_coordinates, "
    "a.recorded_at, e.created_at "
    "FROM snapshot_fact_sources sfs "
    "JOIN assertions a ON a.id = sfs.assertion_id AND a.group_id = sfs.group_id "
    "JOIN evidence e ON e.assertion_id = a.id AND e.group_id = sfs.group_id "
    "JOIN artifact_versions av ON av.id = e.artifact_version_id "
    "JOIN artifacts art ON art.id = av.artifact_id "
    "LEFT JOIN chunks c ON c.id = e.chunk_id AND c.group_id = sfs.group_id "
    "AND c.artifact_version_id = e.artifact_version_id "
    "WHERE sfs.snapshot_id = :sid AND sfs.group_id = :g "
    "AND e.created_at <= :vt AND ((c.id IS NOT NULL AND e.quote_start IS NOT NULL "
    "AND e.quote_end IS NOT NULL AND e.quote_hash IS NOT NULL) "
    "OR (e.chunk_id IS NULL AND (e.structured_record IS NOT NULL "
    "OR e.citation_override IS NOT NULL OR e.citation_uri IS NOT NULL))) "
    "AND a.artifact_version_id = e.artifact_version_id "
    "AND a.artifact_id = art.id AND a.knowledge_source_id = art.source_id"
)
_COUNT_FACTS = text(
    "SELECT count(*) FROM snapshot_facts WHERE snapshot_id = :sid AND group_id = :g"
)
_SOURCE_BOUNDARIES = text(
    "SELECT coalesce(jsonb_object_agg(src, ver), '{}'::jsonb) FROM ("
    "  SELECT DISTINCT ON (source_id) source_id::text AS src, version_id::text AS ver "
    "  FROM ("
    "    SELECT s.knowledge_source_id AS source_id, s.artifact_version_id AS version_id, "
    "           s.assertion_recorded_at AS boundary_at "
    "    FROM snapshot_fact_sources s "
    "    WHERE s.snapshot_id = :sid AND s.group_id = :g "
    "      AND s.knowledge_source_id IS NOT NULL AND s.artifact_version_id IS NOT NULL "
    "    UNION ALL "
    "    SELECT c.knowledge_source_id, c.artifact_version_id, c.created_at "
    "    FROM snapshot_chunks c WHERE c.snapshot_id = :sid AND c.group_id = :g"
    "  ) boundaries "
    "  ORDER BY source_id, boundary_at DESC, version_id DESC"
    ") t"
)
_GRAPH_CHECKPOINT = text(
    "SELECT id FROM ingestion_jobs WHERE group_id = :g AND status = 'done' "
    "AND payload->>'job_kind' = 'project_facts' ORDER BY created_at DESC, id DESC LIMIT 1"
)
_ACTIVE_ONTOLOGY = text("SELECT id FROM ontology_versions ORDER BY version DESC LIMIT 1")
_SET_META = text(
    "UPDATE knowledge_snapshots SET fact_count = :n, source_boundaries = CAST(:sb AS jsonb), "
    "retrieval_frozen = true WHERE id = :sid AND group_id = :g AND retrieval_frozen = false "
    "RETURNING id"
)
_GET_SNAPSHOT = text(
    "SELECT id, group_id, created_at, frozen_at_system_time, as_of_valid_time, policy_version, "
    "fact_count, ontology_version_id, source_boundaries, embedding_version, "
    "retrieval_index_version, assembler_version, graph_projection_checkpoint, retrieval_frozen "
    "FROM knowledge_snapshots WHERE id = :sid AND group_id = :g"
)
_SNAPSHOT_FACT_IDS = text(
    "SELECT fact_id::text FROM snapshot_facts WHERE snapshot_id = :sid AND group_id = :g"
)
_EVENT = text(
    "INSERT INTO knowledge_events (group_id, event_type, actor, next_state, policy_version) "
    "VALUES (:g, :et, :actor, CAST(:ns AS jsonb), :pv)"
)

_INSERT_PACK = text(
    "INSERT INTO context_packs (group_id, snapshot_id, query, hints, token_estimate, "
    "result_count, omitted, conflicts, freshness_warnings, results, request_hash, "
    "result_references, expires_at, assembler_version, request) "
    "VALUES (:g, :sid, :q, CAST(:hints AS jsonb), :tokens, :rc, :om, :cf, :fw, "
    "CAST(:res AS jsonb), :rh, CAST(:refs AS jsonb), :expires, :av, CAST(:request AS jsonb)) "
    "RETURNING id, created_at"
)
_GET_PACK = text(
    "SELECT id, group_id, snapshot_id, created_at, query, token_estimate, result_count, "
    "omitted, conflicts, freshness_warnings, results, request_hash, result_references, "
    "expires_at, assembler_version, request "
    "FROM context_packs WHERE id = :pid AND group_id = :g"
)


class SqlAlchemySnapshotRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        group_id: str,
        policy_version: str,
        as_of: datetime | None = None,
        ontology_version_id: str | None = None,
        embedding_version: JsonDict | None = None,
        retrieval_index_version: str = "fts-v1",
        assembler_version: str = "context-assembler-v2",
        actor: str | None = None,
    ) -> Snapshot:
        session = self._session
        graph_checkpoint = await session.scalar(_GRAPH_CHECKPOINT, {"g": group_id})
        if ontology_version_id is None:
            active_ontology_id = await session.scalar(_ACTIVE_ONTOLOGY)
            ontology_version_id = str(active_ontology_id) if active_ontology_id else None
        row = (
            await session.execute(
                _INSERT_SNAPSHOT,
                {
                    "g": group_id,
                    "pv": policy_version,
                    "vt": as_of,
                    "ov": ontology_version_id,
                    "ev": json.dumps(embedding_version or {}),
                    "iv": retrieval_index_version,
                    "av": assembler_version,
                    "gc": graph_checkpoint,
                },
            )
        ).one()
        snapshot_id = row.id
        await session.execute(_CAPTURE_CHUNKS, {"sid": snapshot_id, "g": group_id})
        await session.execute(_CAPTURE_SOURCE_CONFIGS, {"sid": snapshot_id, "g": group_id})
        if embedding_version:
            await session.execute(
                _CAPTURE_EMBEDDINGS,
                {
                    "sid": snapshot_id,
                    "g": group_id,
                    "provider": embedding_version["provider"],
                    "model": embedding_version["model"],
                    "model_version": embedding_version["model_version"],
                    "dimension": embedding_version["dimension"],
                },
            )
        capture = _CAPTURE_AS_OF if as_of is not None else _CAPTURE_ACTIVE
        await session.execute(
            capture,
            {"sid": snapshot_id, "g": group_id, "vt": row.as_of_valid_time},
        )
        await session.execute(
            _CAPTURE_SOURCES,
            {
                "sid": snapshot_id,
                "g": group_id,
                "historical": as_of is not None,
                "vt": row.as_of_valid_time,
            },
        )
        await session.execute(
            _CAPTURE_CITATIONS,
            {"sid": snapshot_id, "g": group_id, "vt": row.as_of_valid_time},
        )
        count = await session.scalar(_COUNT_FACTS, {"sid": snapshot_id, "g": group_id}) or 0
        boundaries: JsonDict = cast(
            "JsonDict",
            await session.scalar(_SOURCE_BOUNDARIES, {"sid": snapshot_id, "g": group_id}) or {},
        )
        sealed_id = await session.scalar(
            _SET_META,
            {
                "n": count,
                "sb": json.dumps(boundaries),
                "sid": snapshot_id,
                "g": group_id,
            },
        )
        if sealed_id != snapshot_id:
            raise RuntimeError("snapshot could not be sealed")
        await session.execute(
            _EVENT,
            {
                "g": group_id,
                "et": "SNAPSHOT_CREATED",
                "actor": actor,
                "ns": json.dumps({"snapshot_id": str(snapshot_id), "fact_count": count}),
                "pv": policy_version,
            },
        )
        return Snapshot(
            id=str(snapshot_id),
            group_id=group_id,
            created_at=row.created_at,
            frozen_at_system_time=row.frozen_at_system_time,
            as_of_valid_time=row.as_of_valid_time,
            policy_version=policy_version,
            fact_count=count,
            ontology_version_id=ontology_version_id,
            source_boundaries=dict(boundaries),
            embedding_version=dict(embedding_version or {}),
            retrieval_index_version=retrieval_index_version,
            assembler_version=assembler_version,
            graph_projection_checkpoint=str(graph_checkpoint) if graph_checkpoint else None,
            retrieval_frozen=True,
        )

    async def get(self, *, group_id: str, snapshot_id: str) -> Snapshot | None:
        row = (
            (await self._session.execute(_GET_SNAPSHOT, {"sid": snapshot_id, "g": group_id}))
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return Snapshot(
            id=str(row["id"]),
            group_id=row["group_id"],
            created_at=row["created_at"],
            frozen_at_system_time=row["frozen_at_system_time"],
            as_of_valid_time=row["as_of_valid_time"],
            policy_version=row["policy_version"],
            fact_count=row["fact_count"],
            ontology_version_id=str(row["ontology_version_id"])
            if row["ontology_version_id"]
            else None,
            source_boundaries=dict(row["source_boundaries"]),
            embedding_version=dict(row["embedding_version"]),
            retrieval_index_version=row["retrieval_index_version"],
            assembler_version=row["assembler_version"],
            graph_projection_checkpoint=str(row["graph_projection_checkpoint"])
            if row["graph_projection_checkpoint"]
            else None,
            retrieval_frozen=bool(row["retrieval_frozen"]),
        )

    async def fact_ids(self, *, group_id: str, snapshot_id: str) -> set[str]:
        rows = await self._session.scalars(_SNAPSHOT_FACT_IDS, {"sid": snapshot_id, "g": group_id})
        return set(rows)


class SqlAlchemyContextPackRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

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
        row = (
            await self._session.execute(
                _INSERT_PACK,
                {
                    "g": group_id,
                    "sid": snapshot_id,
                    "q": query,
                    "hints": json.dumps(hints or {}),
                    "tokens": token_estimate,
                    "rc": result_count,
                    "om": omitted,
                    "cf": conflicts,
                    "fw": freshness_warnings,
                    "res": json.dumps(results),
                    "rh": request_hash,
                    "refs": json.dumps(result_references),
                    "expires": expires_at,
                    "av": assembler_version,
                    "request": json.dumps(request),
                },
            )
        ).one()
        await self._session.execute(
            _EVENT,
            {
                "g": group_id,
                "et": "CONTEXT_PACK_CREATED",
                "actor": actor,
                "ns": json.dumps({"pack_id": str(row.id), "snapshot_id": snapshot_id}),
                "pv": None,
            },
        )
        return ContextPack(
            id=str(row.id),
            group_id=group_id,
            created_at=row.created_at,
            query=query,
            token_estimate=token_estimate,
            result_count=result_count,
            omitted=omitted,
            conflicts=conflicts,
            freshness_warnings=freshness_warnings,
            request_hash=request_hash,
            result_references=result_references,
            expires_at=expires_at,
            assembler_version=assembler_version,
            request=request,
            snapshot_id=snapshot_id,
            results=results,
        )

    async def get(self, *, group_id: str, pack_id: str) -> ContextPack | None:
        row = (
            (await self._session.execute(_GET_PACK, {"pid": pack_id, "g": group_id}))
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return ContextPack(
            id=str(row["id"]),
            group_id=row["group_id"],
            snapshot_id=str(row["snapshot_id"]) if row["snapshot_id"] else None,
            created_at=row["created_at"],
            query=row["query"],
            token_estimate=row["token_estimate"],
            result_count=row["result_count"],
            omitted=row["omitted"],
            conflicts=row["conflicts"],
            freshness_warnings=row["freshness_warnings"],
            request_hash=row["request_hash"],
            result_references=[str(ref) for ref in row["result_references"]],
            expires_at=row["expires_at"],
            assembler_version=row["assembler_version"],
            request=dict(row["request"]),
            results=list(row["results"]),
        )
