"""Read model for the generic knowledge contracts (Phase 6).

Cross-scope reads over the authoritative fact store, on a trusted connection with an explicit
``group_id = ANY(...)`` filter over the server-resolved scopes (never a client-chosen scope).
Returns plain dicts so the API and MCP surfaces can serialize them directly.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_FACT = text(
    "SELECT f.id::text AS fact_id, f.fact_key, f.group_id, cs.canonical_name AS subject, "
    "f.predicate, COALESCE(co.canonical_name, f.object_scalar) AS object, f.qualifiers, "
    "f.lifecycle_state, f.authority, f.confidence, f.valid_from, f.valid_to, f.expires_at "
    "FROM facts f JOIN canonical_entities cs ON cs.id = f.subject_entity_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
    "WHERE f.group_id = ANY(CAST(:gids AS text[])) AND f.fact_key = :fk "
    "ORDER BY f.system_from DESC LIMIT 1"
)
_ASSERTIONS = text(
    "SELECT a.id::text AS assertion_id, a.polarity, a.verification_state, a.source_authority, "
    "a.extractor_confidence, a.knowledge_source_id::text AS source_id, "
    "a.artifact_version_id::text AS artifact_version_id, a.recorded_at "
    "FROM assertions a WHERE a.group_id = ANY(CAST(:gids AS text[])) "
    "AND a.fact_id = CAST(:fid AS uuid) AND a.state = 'active' "
    "ORDER BY a.recorded_at DESC"
)
_EVIDENCE = text(
    "SELECT e.id::text AS evidence_id, "
    "COALESCE(CASE WHEN e.quote_start IS NOT NULL THEN "
    "substring(c.text FROM e.quote_start + 1 FOR e.quote_end - e.quote_start) END, "
    "e.excerpt) AS excerpt, COALESCE(e.citation_override, e.citation_uri) AS citation_uri, "
    "e.chunk_id::text AS chunk_id, e.artifact_version_id::text AS artifact_version_id, "
    "e.quote_start, e.quote_end, e.quote_hash, "
    "e.extraction_run_id::text AS extraction_run_id, e.confidentiality "
    "FROM evidence e LEFT JOIN chunks c ON c.id = e.chunk_id AND c.group_id = e.group_id "
    "WHERE e.group_id = ANY(CAST(:gids AS text[])) "
    "AND e.assertion_id = CAST(:aid AS uuid)"
)
# Evidence for a fact, flattened across its active supporting assertions, for citation. The
# per-assertion verification and source travel with each evidence row so a caller can weigh it.
_FACT_EVIDENCE = text(
    "SELECT e.id::text AS evidence_id, "
    "COALESCE(CASE WHEN e.quote_start IS NOT NULL THEN "
    "substring(c.text FROM e.quote_start + 1 FOR e.quote_end - e.quote_start) END, "
    "e.excerpt) AS excerpt, COALESCE(e.citation_override, e.citation_uri) AS citation_uri, "
    "e.chunk_id::text AS chunk_id, e.artifact_version_id::text AS artifact_version_id, "
    "e.quote_start, e.quote_end, e.quote_hash, "
    "e.extraction_run_id::text AS extraction_run_id, e.confidentiality, "
    "a.id::text AS assertion_id, a.polarity, a.verification_state, "
    "a.knowledge_source_id::text AS source_id "
    "FROM evidence e "
    "LEFT JOIN chunks c ON c.id = e.chunk_id AND c.group_id = e.group_id "
    "JOIN assertions a ON a.id = e.assertion_id AND a.state = 'active' "
    "JOIN facts f ON f.id = a.fact_id "
    "WHERE f.group_id = ANY(CAST(:gids AS text[])) AND f.fact_key = :fk "
    "ORDER BY a.recorded_at DESC"
)
_RELATIONS = text(
    "SELECT r.relation_type, r.to_fact_id::text AS to_fact_id "
    "FROM fact_relations r WHERE r.group_id = ANY(CAST(:gids AS text[])) "
    "AND r.from_fact_id = CAST(:fid AS uuid)"
)
_CHANGES = text(
    "SELECT event_type, occurred_at, actor, source_id, fact_id::text AS fact_id, reason "
    "FROM knowledge_events WHERE group_id = ANY(CAST(:gids AS text[])) "
    "ORDER BY occurred_at DESC LIMIT :lim"
)
_CONFLICTS = text(
    "SELECT f.fact_key, cs.canonical_name AS subject, f.predicate, "
    "COALESCE(co.canonical_name, f.object_scalar) AS object, f.slot_key, f.authority "
    "FROM facts f JOIN canonical_entities cs ON cs.id = f.subject_entity_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
    "WHERE f.group_id = ANY(CAST(:gids AS text[])) AND f.lifecycle_state = 'disputed' "
    "ORDER BY f.updated_at DESC LIMIT :lim"
)
_REVIEW = text(
    "SELECT f.fact_key, f.group_id, cs.canonical_name AS subject, f.predicate, "
    "COALESCE(co.canonical_name, f.object_scalar) AS object, f.authority, f.confidence, "
    "f.created_at "
    "FROM facts f JOIN canonical_entities cs ON cs.id = f.subject_entity_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
    "WHERE f.group_id = ANY(CAST(:gids AS text[])) AND f.lifecycle_state = 'proposed' "
    "ORDER BY f.created_at DESC LIMIT :lim"
)
_PROPOSAL_REPORT = text(
    "WITH matching AS MATERIALIZED ("
    "  SELECT pa.id, pa.id::text AS attempt_ref, pa.fact_key, pa.proposal_ref::text, "
    "  pa.outcome, pa.operation, pa.context, pa.detail, pa.created_at, f.lifecycle_state, "
    "  f.predicate, f.object_scalar AS object "
    "  FROM proposal_attempts pa LEFT JOIN LATERAL ("
    "  SELECT latest.lifecycle_state, latest.predicate, latest.object_scalar "
    "  FROM facts latest WHERE latest.group_id = pa.group_id "
    "  AND latest.fact_key = pa.fact_key "
    "  ORDER BY latest.system_from DESC, latest.id DESC LIMIT 1"
    "  ) f ON TRUE WHERE pa.group_id = :group_id "
    "  AND pa.context @> CAST(:context AS jsonb)"
    "), page AS ("
    "  SELECT * FROM matching "
    "  WHERE (CAST(:cursor AS uuid) IS NULL OR id > CAST(:cursor AS uuid)) "
    "  ORDER BY id LIMIT :limit"
    "), counts AS ("
    "  SELECT count(*) FILTER (WHERE outcome = 'created') AS created_count, "
    "  count(*) FILTER (WHERE outcome = 'deduplicated') AS deduplicated_count, "
    "  count(*) FILTER (WHERE outcome = 'conflicted') AS conflicted_count, "
    "  count(*) FILTER (WHERE outcome = 'rejected') AS rejected_count, "
    "  count(*) FILTER (WHERE outcome = 'skipped') + "
    "  count(*) FILTER (WHERE operation = 'skipped' AND outcome <> 'skipped') "
    "  AS skipped_count FROM matching"
    "), state_totals AS ("
    "  SELECT CASE WHEN lifecycle_state = 'active' THEN 'accepted' "
    "  WHEN lifecycle_state = 'retracted' THEN 'rejected' ELSE 'pending' "
    "  END AS current_state, count(*) AS total FROM ("
    "    SELECT DISTINCT fact_key, lifecycle_state FROM matching "
    "    WHERE fact_key IS NOT NULL AND lifecycle_state IS NOT NULL"
    "  ) latest_facts GROUP BY current_state"
    "), states AS ("
    "  SELECT COALESCE(jsonb_object_agg(current_state, total), '{}'::jsonb) AS state_counts "
    "  FROM state_totals"
    ") SELECT page.attempt_ref, page.fact_key, page.proposal_ref, page.outcome, "
    "page.operation, page.context, page.detail, page.created_at, page.lifecycle_state, "
    "page.predicate, page.object, counts.created_count, counts.deduplicated_count, "
    "counts.conflicted_count, counts.rejected_count, counts.skipped_count, "
    "states.state_counts FROM counts CROSS JOIN states LEFT JOIN page ON TRUE ORDER BY page.id"
)
_TIMELINE = text(
    "SELECT event_type, occurred_at, actor, reason FROM knowledge_events "
    "WHERE group_id = ANY(CAST(:gids AS text[])) AND fact_id IN ("
    "  SELECT id FROM facts WHERE group_id = ANY(CAST(:gids AS text[])) AND fact_key = :fk"
    ") ORDER BY occurred_at ASC LIMIT :lim"
)
_PROJECT_GROUP = text(
    "SELECT group_id FROM projects WHERE group_id = ANY(CAST(:gids AS text[])) "
    "AND (slug = :project OR name = :project) ORDER BY group_id LIMIT 1"
)
_PROJECTS = text(
    "SELECT p.id::text AS project_id, p.workspace_id::text AS workspace_id, "
    "p.slug, p.name, p.group_id, w.slug AS workspace_slug, w.name AS workspace_name, "
    "COALESCE(array_agg(DISTINCT s.config->>'repository') "
    "FILTER (WHERE s.config->>'repository' IS NOT NULL), '{}'::text[]) AS repositories "
    "FROM projects p JOIN workspaces w ON w.id = p.workspace_id "
    "LEFT JOIN knowledge_sources s ON s.workspace_id = p.workspace_id AND s.enabled "
    "AND (s.project_id = p.id OR s.project_id IS NULL) "
    "WHERE p.group_id = ANY(CAST(:gids AS text[])) "
    "GROUP BY p.id, p.workspace_id, p.slug, p.name, p.group_id, w.slug, w.name "
    "ORDER BY w.slug, p.slug"
)
_ENTITY = text(
    "SELECT id::text AS entity_id, group_id, entity_type, canonical_name, summary, attributes, "
    "created_at, updated_at FROM canonical_entities "
    "WHERE group_id = ANY(CAST(:gids AS text[])) AND id::text = :eid"
)
_ENTITY_ALIASES = text(
    "SELECT alias FROM entity_aliases WHERE group_id = ANY(CAST(:gids AS text[])) "
    "AND canonical_entity_id::text = :eid ORDER BY alias"
)
_ENTITY_FACTS = text(
    "SELECT f.fact_key, cs.canonical_name AS subject, f.predicate, "
    "COALESCE(co.canonical_name, f.object_scalar) AS object, f.lifecycle_state, "
    "f.authority, f.confidence, f.valid_from, f.valid_to, f.expires_at "
    "FROM facts f JOIN canonical_entities cs ON cs.id = f.subject_entity_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
    "WHERE f.group_id = ANY(CAST(:gids AS text[])) "
    "AND (f.subject_entity_id::text = :eid OR f.object_entity_id::text = :eid) "
    "ORDER BY f.updated_at DESC LIMIT :lim"
)
_SOURCE = text(
    "SELECT s.id::text AS source_id, s.kind, s.name, s.trust_tier, s.enabled, "
    "s.created_at, s.updated_at, COALESCE(p.group_id, w.group_id) AS scope_id "
    "FROM knowledge_sources s JOIN workspaces w ON w.id = s.workspace_id "
    "LEFT JOIN projects p ON p.id = s.project_id "
    "WHERE s.id::text = :sid AND (p.group_id = ANY(CAST(:gids AS text[])) "
    "OR w.group_id = ANY(CAST(:gids AS text[])))"
)
_SOURCE_VERSIONS = text(
    "SELECT a.id::text AS artifact_id, a.external_id, a.title, a.current_version, "
    "a.reference_time AS artifact_reference_time, av.id::text AS artifact_version_id, "
    "av.version, av.source_revision, av.source_version_id, av.source_updated_at, "
    "av.observed_at, av.reference_time, av.content_hash "
    "FROM artifacts a LEFT JOIN artifact_versions av ON av.artifact_id = a.id "
    "WHERE a.source_id::text = :sid ORDER BY a.external_id, av.version DESC"
)


class SqlAlchemyKnowledgeReadModel:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def resolve_project(self, *, group_ids: list[str], project: str) -> str | None:
        async with self._session_factory() as session:
            return await session.scalar(_PROJECT_GROUP, {"gids": group_ids, "project": project})

    async def list_projects(self, *, group_ids: list[str]) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (await session.execute(_PROJECTS, {"gids": group_ids})).mappings().all()
        return [dict(row) for row in rows]

    async def proposal_report(
        self,
        *,
        group_id: str,
        context: dict[str, object],
        cursor: UUID | None,
        limit: int,
    ) -> dict[str, Any]:
        params = {
            "group_id": group_id,
            "context": json.dumps(context),
            "cursor": str(cursor) if cursor else None,
            "limit": limit + 1,
        }
        async with self._session_factory() as session:
            records = (await session.execute(_PROPOSAL_REPORT, params)).mappings().all()
        metadata = records[0]
        row_keys = (
            "attempt_ref",
            "fact_key",
            "proposal_ref",
            "outcome",
            "operation",
            "context",
            "detail",
            "created_at",
            "lifecycle_state",
            "predicate",
            "object",
        )
        rows = [
            {key: record[key] for key in row_keys}
            for record in records
            if record["attempt_ref"] is not None
        ]
        page = rows[:limit]
        return {
            "rows": page,
            "counts": {
                "created": int(metadata["created_count"]),
                "skipped": int(metadata["skipped_count"]),
                "deduplicated": int(metadata["deduplicated_count"]),
                "conflicted": int(metadata["conflicted_count"]),
                "rejected": int(metadata["rejected_count"]),
            },
            "states": dict(metadata["state_counts"]),
            "next_cursor": str(page[-1]["attempt_ref"]) if len(rows) > limit and page else None,
        }

    async def get_entity(
        self, *, group_ids: list[str], entity_id: str, limit: int = 100
    ) -> dict[str, Any] | None:
        async with self._session_factory() as session:
            row = (
                (await session.execute(_ENTITY, {"gids": group_ids, "eid": entity_id}))
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            entity = dict(row)
            aliases = await session.scalars(_ENTITY_ALIASES, {"gids": group_ids, "eid": entity_id})
            facts = (
                (
                    await session.execute(
                        _ENTITY_FACTS,
                        {"gids": group_ids, "eid": entity_id, "lim": limit},
                    )
                )
                .mappings()
                .all()
            )
            entity["aliases"] = list(aliases)
            entity["facts"] = [dict(fact) for fact in facts]
            return entity

    async def get_source(self, *, group_ids: list[str], source_id: str) -> dict[str, Any] | None:
        async with self._session_factory() as session:
            row = (
                (await session.execute(_SOURCE, {"gids": group_ids, "sid": source_id}))
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            source = dict(row)
            versions = (
                (await session.execute(_SOURCE_VERSIONS, {"sid": source_id})).mappings().all()
            )
        artifacts: dict[str, dict[str, Any]] = {}
        latest_observed_at: datetime | None = None
        for version in versions:
            artifact_id = str(version["artifact_id"])
            artifact = artifacts.setdefault(
                artifact_id,
                {
                    "artifact_id": artifact_id,
                    "external_id": version["external_id"],
                    "title": version["title"],
                    "current_version": version["current_version"],
                    "reference_time": version["artifact_reference_time"],
                    "versions": [],
                },
            )
            if version["artifact_version_id"] is not None:
                artifact["versions"].append(
                    {
                        key: version[key]
                        for key in (
                            "artifact_version_id",
                            "version",
                            "source_revision",
                            "source_version_id",
                            "source_updated_at",
                            "observed_at",
                            "reference_time",
                            "content_hash",
                        )
                    }
                )
                observed_at = version["observed_at"]
                if latest_observed_at is None or observed_at > latest_observed_at:
                    latest_observed_at = observed_at
        source["artifacts"] = list(artifacts.values())
        source["freshness"] = {
            "latest_observed_at": latest_observed_at,
            "artifact_count": len(artifacts),
        }
        return source

    async def get_fact(self, *, group_ids: list[str], fact_key: str) -> dict[str, Any] | None:
        async with self._session_factory() as session:
            row = (
                (await session.execute(_FACT, {"gids": group_ids, "fk": fact_key}))
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            fact = dict(row)
            relations = (
                (await session.execute(_RELATIONS, {"gids": group_ids, "fid": fact["fact_id"]}))
                .mappings()
                .all()
            )
            fact["relations"] = [dict(r) for r in relations]
            return fact

    async def explain_fact(self, *, group_ids: list[str], fact_key: str) -> dict[str, Any] | None:
        async with self._session_factory() as session:
            row = (
                (await session.execute(_FACT, {"gids": group_ids, "fk": fact_key}))
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            fact = dict(row)
            assertion_rows = (
                (await session.execute(_ASSERTIONS, {"gids": group_ids, "fid": fact["fact_id"]}))
                .mappings()
                .all()
            )
            assertions: list[dict[str, Any]] = []
            for a in assertion_rows:
                assertion = dict(a)
                ev = (
                    (
                        await session.execute(
                            _EVIDENCE, {"gids": group_ids, "aid": assertion["assertion_id"]}
                        )
                    )
                    .mappings()
                    .all()
                )
                assertion["evidence"] = [dict(e) for e in ev]
                assertions.append(assertion)
            fact["assertions"] = assertions
            return fact

    async def get_evidence(
        self, *, group_ids: list[str], fact_key: str
    ) -> list[dict[str, Any]] | None:
        """The evidence supporting a fact, flattened across its active assertions. None when
        the fact does not exist in the caller's scopes; an empty list when it has no evidence.
        """
        async with self._session_factory() as session:
            fact = await session.scalar(_FACT, {"gids": group_ids, "fk": fact_key})
            if fact is None:
                return None
            rows = (
                (await session.execute(_FACT_EVIDENCE, {"gids": group_ids, "fk": fact_key}))
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]

    async def recent_changes(
        self, *, group_ids: list[str], limit: int = 50
    ) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                (await session.execute(_CHANGES, {"gids": group_ids, "lim": limit}))
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]

    async def conflicts(self, *, group_ids: list[str], limit: int = 50) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                (await session.execute(_CONFLICTS, {"gids": group_ids, "lim": limit}))
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]

    async def review_queue(self, *, group_ids: list[str], limit: int = 50) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                (await session.execute(_REVIEW, {"gids": group_ids, "lim": limit})).mappings().all()
            )
        return [dict(r) for r in rows]

    async def fact_timeline(
        self, *, group_ids: list[str], fact_key: str, limit: int = 200
    ) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                (
                    await session.execute(
                        _TIMELINE, {"gids": group_ids, "fk": fact_key, "lim": limit}
                    )
                )
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]
