"""Postgres full-text candidate sources for combined retrieval (Phase 4).

Passage and code search run over the rebuildable ``chunks.search_vector``; fact search runs
over ``facts.search_vector`` plus the subject entity's name, and applies the lifecycle and
valid-time filters. All run on a trusted connection with an explicit ``group_id`` filter,
like the retrieval read model. These are the default adapters; a vector backend implements
the same ports later.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.domain.knowledge.fabric import fact_semantic_text
from vera.domain.ports.retrieval_index import (
    ContentAvailability,
    FactHit,
    PassageHit,
    RetrievalFilters,
)

# Candidate generation favors recall: OR the query lexemes (plainto_tsquery ANDs them, so a
# multi-word natural query would never match a terse fact doc). ts_rank still orders by how
# well each row matches, and the downstream blend and diversity handle precision.
_ORQ = "CAST(replace(CAST(plainto_tsquery('english', :q) AS text), ' & ', ' | ') AS tsquery)"
_FACT_LEXICAL_SCORE = """(ts_rank(f.search_vector, q.q)
         + ts_rank(to_tsvector('english', cs.canonical_name), q.q)
         + ts_rank(to_tsvector('english', coalesce(co.canonical_name, '')), q.q))"""
_FACT_LEXICAL_MATCH = """f.search_vector @@ q.q
        OR to_tsvector('english', cs.canonical_name) @@ q.q
        OR to_tsvector('english', coalesce(co.canonical_name, '')) @@ q.q"""

_PASSAGE = f"""
SELECT c.id, c.artifact_version_id, c.text, c.content_hash, c.heading_path, c.symbol_name,
       c.start_offset, c.end_offset, c.page_number, c.start_line, c.end_line,
       ts_rank(c.search_vector, {_ORQ}) AS score
FROM chunks c
JOIN artifact_versions av ON av.id = c.artifact_version_id
JOIN artifacts a ON a.id = av.artifact_id
JOIN knowledge_sources s ON s.id = a.source_id
LEFT JOIN projects p ON p.id = s.project_id
JOIN workspaces w ON w.id = s.workspace_id
{{snapshot_join}}
WHERE c.group_id = :g AND c.search_vector @@ {_ORQ}
  AND ((s.project_id IS NOT NULL AND p.group_id = c.group_id)
       OR (s.project_id IS NULL AND (w.group_id = c.group_id OR EXISTS (
           SELECT 1 FROM projects wp
           WHERE wp.workspace_id = s.workspace_id AND wp.group_id = c.group_id))))
  AND av.version = (
      SELECT max(visible.version) FROM artifact_versions visible
      WHERE visible.artifact_id = av.artifact_id
        AND (CAST(:created_before AS timestamptz) IS NULL
             OR visible.observed_at <= :created_before))
  AND (CAST(:created_before AS timestamptz) IS NULL OR c.created_at <= :created_before)
{{source_filters}}
  AND (CAST(:code_path AS text) IS NULL
       OR coalesce(c.heading_path, '') LIKE '%' || :code_path || '%')
{{code_filter}}
ORDER BY score DESC, c.id ASC
LIMIT :lim
"""

_SNAPSHOT_PASSAGE = f"""
SELECT sc.chunk_id AS id, sc.artifact_version_id, sc.text, sc.content_hash,
       sc.heading_path, sc.symbol_name,
       sc.start_offset, sc.end_offset, sc.page_number, sc.start_line, sc.end_line,
       ts_rank(sc.search_vector, {_ORQ}) AS score
FROM snapshot_chunks sc
JOIN snapshot_sources ss ON ss.knowledge_source_id = sc.knowledge_source_id
 AND ss.snapshot_id = sc.snapshot_id AND ss.group_id = sc.group_id
WHERE sc.snapshot_id = CAST(:snapshot_id AS uuid) AND sc.group_id = :g
  AND sc.search_vector @@ {_ORQ}
{{source_filters}}
  AND (CAST(:code_path AS text) IS NULL
       OR coalesce(sc.heading_path, '') LIKE '%' || :code_path || '%')
{{code_filter}}
ORDER BY score DESC, sc.chunk_id ASC
LIMIT :lim
"""

_LIVE_SOURCE_FILTERS = """
  AND (CAST(:repository AS text) IS NULL OR s.config->>'repository' = :repository)
  AND (CAST(:branch AS text) IS NULL OR s.config->>'branch' = :branch)
  AND (CAST(:document_type AS text) IS NULL OR s.config->>'document_type' = :document_type)
  AND (CAST(:source_type AS text) IS NULL OR s.kind = :source_type)
  AND (CAST(:max_trust_tier AS integer) IS NULL OR s.trust_tier <= :max_trust_tier)
"""
_SNAPSHOT_SOURCE_FILTERS = """
  AND (CAST(:repository AS text) IS NULL OR ss.repository = :repository)
  AND (CAST(:branch AS text) IS NULL OR ss.branch = :branch)
  AND (CAST(:document_type AS text) IS NULL OR ss.document_type = :document_type)
  AND (CAST(:source_type AS text) IS NULL OR ss.source_type = :source_type)
  AND (CAST(:max_trust_tier AS integer) IS NULL OR ss.trust_tier <= :max_trust_tier)
"""

_CONTENT_AVAILABILITY = """
SELECT EXISTS (
           SELECT 1 FROM chunks WHERE group_id = :g
       ) AS passages,
       EXISTS (
           SELECT 1 FROM chunks WHERE group_id = :g AND symbol_name IS NOT NULL
       ) AS code
"""
_SNAPSHOT_CONTENT_AVAILABILITY = """
SELECT EXISTS (
           SELECT 1 FROM snapshot_chunks
           WHERE snapshot_id = CAST(:snapshot_id AS uuid) AND group_id = :g
       ) AS passages,
       EXISTS (
           SELECT 1 FROM snapshot_chunks
           WHERE snapshot_id = CAST(:snapshot_id AS uuid) AND group_id = :g
             AND symbol_name IS NOT NULL
       ) AS code
"""
_FACTS_TMPL = """
WITH candidates AS MATERIALIZED (
    SELECT f.fact_key AS fact_key, f.id AS fact_id, f.group_id AS group_id,
           cs.canonical_name AS subject_name, f.predicate AS predicate,
           COALESCE(co.canonical_name, f.object_scalar) AS object_name,
           f.object_type AS object_type, f.qualifiers AS qualifiers,
           fv.authority AS authority, fv.confidence AS confidence,
           fv.lifecycle_state AS lifecycle_state, fv.valid_from AS valid_from,
           {score} AS score
    FROM facts f
    JOIN canonical_entities cs ON cs.id = f.subject_entity_id AND cs.group_id = f.group_id
    LEFT JOIN canonical_entities co ON co.id = f.object_entity_id AND co.group_id = f.group_id
    JOIN LATERAL (
        SELECT fr.lifecycle_state, fr.authority, fr.confidence,
               fr.valid_from, fr.valid_to, fr.expires_at
        FROM fact_revisions fr
        WHERE fr.fact_id = f.id AND fr.group_id = f.group_id
          AND ((CAST(:known_as_of AS timestamptz) IS NULL AND fr.system_to IS NULL)
               OR (CAST(:known_as_of AS timestamptz) IS NOT NULL
                   AND fr.system_from <= :known_as_of
                   AND (fr.system_to IS NULL OR fr.system_to > :known_as_of)))
        ORDER BY fr.system_from DESC, fr.id DESC
        LIMIT 1
    ) fv ON true
    {query_join}
    WHERE f.group_id = :g
      AND ({match})
      AND {membership}
      AND {support_requirement}
      AND (CAST(:min_authority AS double precision) IS NULL OR fv.authority >= :min_authority)
      AND (cardinality(CAST(:include_predicates AS text[])) = 0
           OR f.predicate = ANY(CAST(:include_predicates AS text[])))
      AND NOT (f.predicate = ANY(CAST(:exclude_predicates AS text[])))
      AND (:conflict_handling = 'include'
           OR (:conflict_handling = 'exclude' AND fv.lifecycle_state <> 'disputed')
           OR (:conflict_handling = 'only' AND fv.lifecycle_state = 'disputed'))
      AND ((CAST(:repository AS text) IS NULL AND CAST(:branch AS text) IS NULL
            AND CAST(:code_path AS text) IS NULL AND CAST(:document_type AS text) IS NULL
            AND CAST(:source_type AS text) IS NULL
            AND CAST(:max_trust_tier AS integer) IS NULL) OR EXISTS (
          SELECT 1 FROM assertions af
          JOIN knowledge_sources fs ON fs.id = af.knowledge_source_id
          LEFT JOIN projects fp ON fp.id = fs.project_id
          JOIN workspaces fw ON fw.id = fs.workspace_id
          WHERE af.fact_id = f.id AND af.group_id = f.group_id
            AND {source_assertion_membership} AND af.polarity = 'supports'
            AND ((fs.project_id IS NOT NULL AND fp.group_id = f.group_id)
                 OR (fs.project_id IS NULL AND (fw.group_id = f.group_id OR EXISTS (
                     SELECT 1 FROM projects fwp
                     WHERE fwp.workspace_id = fs.workspace_id AND fwp.group_id = f.group_id))))
            AND (CAST(:repository AS text) IS NULL OR fs.config->>'repository' = :repository)
            AND (CAST(:branch AS text) IS NULL OR fs.config->>'branch' = :branch)
            AND (CAST(:document_type AS text) IS NULL
                 OR fs.config->>'document_type' = :document_type)
            AND (CAST(:source_type AS text) IS NULL OR fs.kind = :source_type)
            AND (CAST(:max_trust_tier AS integer) IS NULL
                 OR fs.trust_tier <= :max_trust_tier)
            AND (CAST(:code_path AS text) IS NULL OR EXISTS (
                SELECT 1 FROM evidence fe
                JOIN chunks fc ON fc.id = fe.chunk_id AND fc.group_id = f.group_id
                JOIN artifact_versions fav ON fav.id = fc.artifact_version_id
                JOIN artifacts fart ON fart.id = fav.artifact_id
                WHERE fe.assertion_id = af.id AND fe.group_id = f.group_id
                  AND fe.artifact_version_id = fc.artifact_version_id
                  AND af.artifact_version_id = fc.artifact_version_id
                  AND af.artifact_id = fart.id AND af.knowledge_source_id = fart.source_id
                  AND coalesce(fc.heading_path, '') LIKE '%' || :code_path || '%'
            ))
      ))
    ORDER BY score DESC, f.id ASC
    LIMIT :lim
)
SELECT f.fact_key AS fact_key, f.fact_id AS fact_id, f.subject_name AS subject_name,
       f.predicate AS predicate, f.object_name AS object_name,
       f.object_type AS object_type, f.qualifiers AS qualifiers,
       f.authority AS authority, f.confidence AS confidence,
       f.lifecycle_state AS lifecycle_state, f.valid_from AS valid_from,
       f.score AS score,
       sup.sources AS sources, ev.evidence_id, ev.assertion_id AS evidence_assertion_id,
       ev.source_id AS evidence_source_id, ev.excerpt AS evidence_excerpt,
       ev.chunk_id AS evidence_chunk_id,
       ev.artifact_version_id AS evidence_artifact_version_id,
        ev.quote_start AS evidence_start_offset, ev.quote_end AS evidence_end_offset,
        ev.quote_hash AS evidence_quote_hash, ev.content_hash AS evidence_content_hash,
        ev.extraction_run_id AS evidence_extraction_run_id,
       ev.source_coordinates AS evidence_source_coordinates,
       ev.structured_record AS evidence_structured_record,
       ev.citation_uri AS evidence_citation_uri
FROM candidates f
LEFT JOIN LATERAL (
    SELECT array_agg(DISTINCT a.knowledge_source_id::text ORDER BY a.knowledge_source_id::text)
           FILTER (WHERE a.knowledge_source_id IS NOT NULL) AS sources
    FROM assertions a
    LEFT JOIN knowledge_sources ss ON ss.id = a.knowledge_source_id
    LEFT JOIN projects sp ON sp.id = ss.project_id
    LEFT JOIN workspaces sw ON sw.id = ss.workspace_id
    WHERE CAST(:include_provenance AS boolean)
      AND a.fact_id = f.fact_id AND a.group_id = f.group_id
      AND a.polarity = 'supports' AND {assertion_membership}
      AND (a.knowledge_source_id IS NULL
           OR (ss.project_id IS NOT NULL AND sp.group_id = f.group_id)
           OR (ss.project_id IS NULL AND (sw.group_id = f.group_id OR EXISTS (
               SELECT 1 FROM projects swp
               WHERE swp.workspace_id = ss.workspace_id AND swp.group_id = f.group_id))))
) sup ON true
LEFT JOIN LATERAL (
    SELECT e.id::text AS evidence_id, a.id::text AS assertion_id,
           es.id::text AS source_id,
           COALESCE(CASE WHEN c.id IS NOT NULL THEN
               substring(c.text FROM e.quote_start + 1 FOR e.quote_end - e.quote_start) END,
               e.excerpt, e.structured_record::text, e.citation_override,
               e.citation_uri) AS excerpt,
           c.id::text AS chunk_id, e.artifact_version_id::text AS artifact_version_id,
             e.quote_start, e.quote_end, e.quote_hash, e.content_hash,
             e.extraction_run_id::text AS extraction_run_id, e.source_coordinates,
             e.structured_record, COALESCE(e.citation_override, e.citation_uri) AS citation_uri
    FROM assertions a
    JOIN evidence e ON e.assertion_id = a.id AND e.group_id = f.group_id
    JOIN artifact_versions eav ON eav.id = e.artifact_version_id
    JOIN artifacts ea ON ea.id = eav.artifact_id
    JOIN knowledge_sources es ON es.id = ea.source_id
    LEFT JOIN chunks c ON c.id = e.chunk_id AND c.group_id = f.group_id
      AND c.artifact_version_id = e.artifact_version_id
    LEFT JOIN projects ep ON ep.id = es.project_id
    JOIN workspaces ew ON ew.id = es.workspace_id
    WHERE CAST(:include_provenance AS boolean)
      AND a.fact_id = f.fact_id AND a.group_id = f.group_id
      AND a.polarity = 'supports' AND {assertion_membership}
      AND {evidence_membership}
      AND ((c.id IS NOT NULL AND e.quote_start IS NOT NULL AND e.quote_end IS NOT NULL
            AND e.quote_hash IS NOT NULL)
           OR (e.chunk_id IS NULL AND (e.structured_record IS NOT NULL
               OR e.citation_override IS NOT NULL OR e.citation_uri IS NOT NULL)))
      AND a.artifact_version_id = e.artifact_version_id
      AND a.artifact_id = ea.id AND a.knowledge_source_id = es.id
      AND ((es.project_id IS NOT NULL AND ep.group_id = f.group_id)
           OR (es.project_id IS NULL AND (ew.group_id = f.group_id OR EXISTS (
               SELECT 1 FROM projects ewp
               WHERE ewp.workspace_id = es.workspace_id AND ewp.group_id = f.group_id))))
      AND (CAST(:repository AS text) IS NULL OR es.config->>'repository' = :repository)
      AND (CAST(:branch AS text) IS NULL OR es.config->>'branch' = :branch)
      AND (CAST(:code_path AS text) IS NULL
           OR coalesce(c.heading_path, '') LIKE '%' || :code_path || '%')
      AND (CAST(:document_type AS text) IS NULL
           OR es.config->>'document_type' = :document_type)
      AND (CAST(:source_type AS text) IS NULL OR es.kind = :source_type)
      AND (CAST(:max_trust_tier AS integer) IS NULL
           OR es.trust_tier <= :max_trust_tier)
    ORDER BY a.recorded_at DESC, e.created_at DESC, a.id DESC, e.id DESC
    LIMIT 1
) ev ON true
ORDER BY f.score DESC, f.fact_id ASC
"""

# Latest view: currently active or disputed facts, honoring an optional as_of valid-time.
_FACTS_LATEST = _FACTS_TMPL.format(
    score=_FACT_LEXICAL_SCORE,
    query_join=f"CROSS JOIN (SELECT {_ORQ} AS q) q",
    match=_FACT_LEXICAL_MATCH,
    assertion_membership=(
        "((CAST(:known_as_of AS timestamptz) IS NULL AND a.state = 'active') OR "
        "(CAST(:known_as_of AS timestamptz) IS NOT NULL AND a.state <> 'needs_review' "
        "AND a.recorded_at <= :known_as_of "
        "AND (a.withdrawn_at IS NULL OR a.withdrawn_at > :known_as_of)))"
    ),
    source_assertion_membership=(
        "((CAST(:known_as_of AS timestamptz) IS NULL AND af.state = 'active') OR "
        "(CAST(:known_as_of AS timestamptz) IS NOT NULL AND af.state <> 'needs_review' "
        "AND af.recorded_at <= :known_as_of "
        "AND (af.withdrawn_at IS NULL OR af.withdrawn_at > :known_as_of)))"
    ),
    evidence_membership=(
        "(CAST(:known_as_of AS timestamptz) IS NULL OR e.created_at <= :known_as_of)"
    ),
    support_requirement=(
        "(CAST(:known_as_of AS timestamptz) IS NULL OR EXISTS ("
        "SELECT 1 FROM assertions am WHERE am.fact_id = f.id AND am.group_id = f.group_id "
        "AND am.polarity = 'supports' AND am.state <> 'needs_review' "
        "AND am.recorded_at <= :known_as_of "
        "AND (am.withdrawn_at IS NULL OR am.withdrawn_at > :known_as_of)))"
    ),
    membership=(
        "((CAST(:as_of AS timestamptz) IS NULL "
        "  AND fv.lifecycle_state IN ('active', 'disputed') "
        "  AND (fv.valid_from IS NULL OR fv.valid_from <= now()) "
        "  AND (fv.valid_to IS NULL OR fv.valid_to > now())) "
        " OR (CAST(:as_of AS timestamptz) IS NOT NULL "
        "  AND fv.lifecycle_state <> 'proposed' "
        "  AND (fv.valid_from IS NULL OR fv.valid_from <= :as_of) "
        "  AND (fv.valid_to IS NULL OR fv.valid_to > :as_of)))"
    ),
)
_FACTS_RESTRICTED = _FACTS_TMPL.format(
    score=_FACT_LEXICAL_SCORE,
    query_join=f"CROSS JOIN (SELECT {_ORQ} AS q) q",
    match=_FACT_LEXICAL_MATCH,
    assertion_membership="a.state = 'active'",
    source_assertion_membership="af.state = 'active'",
    evidence_membership="true",
    support_requirement="true",
    membership="f.id = ANY(CAST(:ids AS uuid[]))",
)
_FACTS_RESTRICTED_MATCHED = _FACTS_TMPL.format(
    score="matched.score",
    query_join=(
        "JOIN unnest(CAST(:match_ids AS uuid[]), CAST(:match_scores AS double precision[])) "
        "AS matched(fact_id, score) ON matched.fact_id = f.id"
    ),
    match="true",
    assertion_membership="a.state = 'active'",
    source_assertion_membership="af.state = 'active'",
    evidence_membership="true",
    support_requirement="true",
    membership="f.id = ANY(CAST(:ids AS uuid[]))",
)
_FACTS_MATCHED = _FACTS_TMPL.format(
    score="matched.score",
    query_join=(
        "JOIN unnest(CAST(:match_ids AS uuid[]), CAST(:match_scores AS double precision[])) "
        "AS matched(fact_id, score) ON matched.fact_id = f.id"
    ),
    match="true",
    assertion_membership=(
        "((CAST(:known_as_of AS timestamptz) IS NULL AND a.state = 'active') OR "
        "(CAST(:known_as_of AS timestamptz) IS NOT NULL AND a.state <> 'needs_review' "
        "AND a.recorded_at <= :known_as_of "
        "AND (a.withdrawn_at IS NULL OR a.withdrawn_at > :known_as_of)))"
    ),
    source_assertion_membership=(
        "((CAST(:known_as_of AS timestamptz) IS NULL AND af.state = 'active') OR "
        "(CAST(:known_as_of AS timestamptz) IS NOT NULL AND af.state <> 'needs_review' "
        "AND af.recorded_at <= :known_as_of "
        "AND (af.withdrawn_at IS NULL OR af.withdrawn_at > :known_as_of)))"
    ),
    evidence_membership=(
        "(CAST(:known_as_of AS timestamptz) IS NULL OR e.created_at <= :known_as_of)"
    ),
    support_requirement=(
        "(CAST(:known_as_of AS timestamptz) IS NULL OR EXISTS ("
        "SELECT 1 FROM assertions am WHERE am.fact_id = f.id AND am.group_id = f.group_id "
        "AND am.polarity = 'supports' AND am.state <> 'needs_review' "
        "AND am.recorded_at <= :known_as_of "
        "AND (am.withdrawn_at IS NULL OR am.withdrawn_at > :known_as_of)))"
    ),
    membership=(
        "((CAST(:as_of AS timestamptz) IS NULL "
        "  AND fv.lifecycle_state IN ('active', 'disputed') "
        "  AND (fv.valid_from IS NULL OR fv.valid_from <= now()) "
        "  AND (fv.valid_to IS NULL OR fv.valid_to > now())) "
        " OR (CAST(:as_of AS timestamptz) IS NOT NULL "
        "  AND fv.lifecycle_state <> 'proposed' "
        "  AND (fv.valid_from IS NULL OR fv.valid_from <= :as_of) "
        "  AND (fv.valid_to IS NULL OR fv.valid_to > :as_of)))"
    ),
)

_FACTS_SNAPSHOT_TMPL = """
SELECT sf.fact_key, sf.fact_id, sf.subject_name, sf.predicate, sf.object_name,
       sf.object_type, sf.qualifiers,
       sf.authority, sf.confidence, sf.lifecycle_state, sf.valid_from,
       {score} AS score,
       sup.sources, cit.evidence_id, cit.assertion_id AS evidence_assertion_id,
       cit.source_id AS evidence_source_id, cit.excerpt AS evidence_excerpt,
       cit.chunk_id AS evidence_chunk_id,
       cit.artifact_version_id AS evidence_artifact_version_id,
        cit.quote_start AS evidence_start_offset, cit.quote_end AS evidence_end_offset,
        cit.quote_hash AS evidence_quote_hash, cit.content_hash AS evidence_content_hash,
        cit.extraction_run_id::text AS evidence_extraction_run_id,
         cit.source_coordinates AS evidence_source_coordinates,
         cit.structured_record AS evidence_structured_record,
         cit.citation_uri AS evidence_citation_uri
FROM snapshot_facts sf
{query_join}
LEFT JOIN LATERAL (
    SELECT array_agg(DISTINCT s.knowledge_source_id::text ORDER BY s.knowledge_source_id::text)
           FILTER (WHERE s.knowledge_source_id IS NOT NULL) AS sources
    FROM snapshot_fact_sources s
    WHERE CAST(:include_provenance AS boolean)
      AND s.snapshot_id = sf.snapshot_id AND s.fact_id = sf.fact_id
      AND s.group_id = sf.group_id
) sup ON true
LEFT JOIN LATERAL (
    SELECT c.evidence_id::text AS evidence_id, c.assertion_id::text AS assertion_id,
           c.knowledge_source_id::text AS source_id, c.excerpt, c.chunk_id::text AS chunk_id,
           c.artifact_version_id::text AS artifact_version_id, c.quote_start, c.quote_end,
            c.quote_hash, c.content_hash, c.extraction_run_id, c.source_coordinates,
            c.structured_record, c.citation_uri
    FROM snapshot_fact_citations c
    JOIN snapshot_fact_sources s ON s.snapshot_id = c.snapshot_id
      AND s.assertion_id = c.assertion_id AND s.fact_id = c.fact_id
      AND s.group_id = c.group_id
    WHERE CAST(:include_provenance AS boolean)
      AND c.snapshot_id = sf.snapshot_id AND c.fact_id = sf.fact_id
      AND c.group_id = sf.group_id
      AND (CAST(:repository AS text) IS NULL OR s.repository = :repository)
      AND (CAST(:branch AS text) IS NULL OR s.branch = :branch)
      AND (CAST(:code_path AS text) IS NULL
           OR coalesce(c.heading_path, '') LIKE '%' || :code_path || '%')
      AND (CAST(:document_type AS text) IS NULL OR s.document_type = :document_type)
      AND (CAST(:source_type AS text) IS NULL OR s.source_type = :source_type)
      AND (CAST(:max_trust_tier AS integer) IS NULL OR s.trust_tier <= :max_trust_tier)
    ORDER BY c.assertion_recorded_at DESC, c.evidence_created_at DESC,
             c.assertion_id DESC, c.evidence_id DESC
    LIMIT 1
) cit ON true
WHERE sf.snapshot_id = CAST(:snapshot_id AS uuid) AND sf.group_id = :g
  AND (CAST(:restrict_ids AS uuid[]) IS NULL OR sf.fact_id = ANY(CAST(:restrict_ids AS uuid[])))
  AND ({match})
  AND (CAST(:min_authority AS double precision) IS NULL OR sf.authority >= :min_authority)
  AND (cardinality(CAST(:include_predicates AS text[])) = 0
       OR sf.predicate = ANY(CAST(:include_predicates AS text[])))
  AND NOT (sf.predicate = ANY(CAST(:exclude_predicates AS text[])))
  AND (:conflict_handling = 'include'
       OR (:conflict_handling = 'exclude' AND sf.lifecycle_state <> 'disputed')
       OR (:conflict_handling = 'only' AND sf.lifecycle_state = 'disputed'))
  AND ((CAST(:repository AS text) IS NULL AND CAST(:branch AS text) IS NULL
        AND CAST(:code_path AS text) IS NULL AND CAST(:document_type AS text) IS NULL
        AND CAST(:source_type AS text) IS NULL
        AND CAST(:max_trust_tier AS integer) IS NULL) OR EXISTS (
      SELECT 1 FROM snapshot_fact_sources fs
      WHERE fs.snapshot_id = sf.snapshot_id AND fs.fact_id = sf.fact_id
        AND fs.group_id = sf.group_id
        AND (CAST(:repository AS text) IS NULL OR fs.repository = :repository)
        AND (CAST(:branch AS text) IS NULL OR fs.branch = :branch)
        AND (CAST(:document_type AS text) IS NULL OR fs.document_type = :document_type)
        AND (CAST(:source_type AS text) IS NULL OR fs.source_type = :source_type)
        AND (CAST(:max_trust_tier AS integer) IS NULL OR fs.trust_tier <= :max_trust_tier)
        AND (CAST(:code_path AS text) IS NULL OR EXISTS (
            SELECT 1 FROM snapshot_fact_citations fc
            WHERE fc.snapshot_id = fs.snapshot_id AND fc.assertion_id = fs.assertion_id
              AND fc.group_id = fs.group_id
              AND coalesce(fc.heading_path, '') LIKE '%' || :code_path || '%'
        ))
  ))
ORDER BY score DESC, sf.fact_id ASC
LIMIT :lim
"""
_SNAPSHOT_LEXICAL_SCORE = """(ts_rank(to_tsvector('english', sf.predicate || ' ' ||
                             sf.normalized_object || ' ' || coalesce(sf.object_scalar, '')), q.q)
        + ts_rank(to_tsvector('english', sf.subject_name), q.q)
        + ts_rank(to_tsvector('english', sf.object_name), q.q))"""
_SNAPSHOT_LEXICAL_MATCH = """to_tsvector('english', sf.predicate || ' ' ||
                   sf.normalized_object || ' ' || coalesce(sf.object_scalar, '')) @@ q.q
       OR to_tsvector('english', sf.subject_name) @@ q.q
       OR to_tsvector('english', sf.object_name) @@ q.q"""
_FACTS_SNAPSHOT = _FACTS_SNAPSHOT_TMPL.format(
    score=_SNAPSHOT_LEXICAL_SCORE,
    query_join=f"CROSS JOIN (SELECT {_ORQ} AS q) q",
    match=_SNAPSHOT_LEXICAL_MATCH,
)
_FACTS_SNAPSHOT_MATCHED = _FACTS_SNAPSHOT_TMPL.format(
    score="matched.score",
    query_join=(
        "JOIN unnest(CAST(:match_ids AS uuid[]), CAST(:match_scores AS double precision[])) "
        "AS matched(fact_id, score) ON matched.fact_id = sf.fact_id"
    ),
    match="true",
)


def passage_hit(row: Any) -> PassageHit:
    return PassageHit(
        chunk_id=str(row["id"]),
        artifact_version_id=str(row["artifact_version_id"]),
        text=row["text"],
        score=float(row["score"]),
        content_hash=row["content_hash"],
        heading_path=row["heading_path"],
        symbol_name=row["symbol_name"],
        start_offset=row["start_offset"],
        end_offset=row["end_offset"],
        page_number=row["page_number"],
        start_line=row["start_line"],
        end_line=row["end_line"],
    )


def retrieval_filter_params(filters: RetrievalFilters | None) -> dict[str, object]:
    selected = filters or RetrievalFilters()
    return {
        "repository": selected.repository,
        "branch": selected.branch,
        "code_path": selected.code_path,
        "document_type": selected.document_type,
        "source_type": selected.source_type,
        "include_predicates": list(selected.include_predicates),
        "exclude_predicates": list(selected.exclude_predicates),
        "min_authority": selected.min_authority,
        "max_trust_tier": selected.max_trust_tier,
        "conflict_handling": selected.conflict_handling,
    }


class SqlAlchemyContentAvailability:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(self, *, group_id: str, snapshot_id: str | None = None) -> ContentAvailability:
        sql = _SNAPSHOT_CONTENT_AVAILABILITY if snapshot_id is not None else _CONTENT_AVAILABILITY
        async with self._session_factory() as session:
            row = (
                (await session.execute(text(sql), {"g": group_id, "snapshot_id": snapshot_id}))
                .mappings()
                .one()
            )
        return ContentAvailability(passages=bool(row["passages"]), code=bool(row["code"]))


class SqlAlchemyPassageIndex:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None = None,
        snapshot_id: str | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        async with self._session_factory() as session:
            rows = (
                (
                    await session.execute(
                        text(
                            (_SNAPSHOT_PASSAGE if snapshot_id is not None else _PASSAGE).format(
                                code_filter="",
                                snapshot_join="",
                                source_filters=(
                                    _SNAPSHOT_SOURCE_FILTERS
                                    if snapshot_id is not None
                                    else _LIVE_SOURCE_FILTERS
                                ),
                            )
                        ),
                        {
                            "g": group_id,
                            "q": query,
                            "lim": limit,
                            "created_before": created_before,
                            "snapshot_id": snapshot_id,
                            **retrieval_filter_params(filters),
                        },
                    )
                )
                .mappings()
                .all()
            )
        return [passage_hit(r) for r in rows]


class SqlAlchemyCodeIndex:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None = None,
        snapshot_id: str | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        async with self._session_factory() as session:
            rows = (
                (
                    await session.execute(
                        text(
                            (_SNAPSHOT_PASSAGE if snapshot_id is not None else _PASSAGE).format(
                                code_filter=(
                                    "AND sc.symbol_name IS NOT NULL"
                                    if snapshot_id is not None
                                    else "AND c.symbol_name IS NOT NULL"
                                ),
                                snapshot_join="",
                                source_filters=(
                                    _SNAPSHOT_SOURCE_FILTERS
                                    if snapshot_id is not None
                                    else _LIVE_SOURCE_FILTERS
                                ),
                            )
                        ),
                        {
                            "g": group_id,
                            "q": query,
                            "lim": limit,
                            "created_before": created_before,
                            "snapshot_id": snapshot_id,
                            **retrieval_filter_params(filters),
                        },
                    )
                )
                .mappings()
                .all()
            )
        return [passage_hit(r) for r in rows]


class SqlAlchemyFactCandidateSource:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        include_provenance: bool = True,
    ) -> None:
        self._session_factory = session_factory
        self._include_provenance = include_provenance

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        as_of: datetime | None = None,
        known_as_of: datetime | None = None,
        restrict_fact_ids: set[str] | None = None,
        snapshot_id: str | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[FactHit]:
        if snapshot_id is not None:
            if restrict_fact_ids is not None and not restrict_fact_ids:
                return []
            sql = text(_FACTS_SNAPSHOT)
            params: dict[str, object] = {
                "g": group_id,
                "q": query,
                "lim": limit,
                "snapshot_id": snapshot_id,
                "restrict_ids": list(restrict_fact_ids) if restrict_fact_ids is not None else None,
                "include_provenance": self._include_provenance,
                **retrieval_filter_params(filters),
            }
        elif restrict_fact_ids is not None:
            if not restrict_fact_ids:
                return []  # an empty snapshot contains no facts
            sql = text(_FACTS_RESTRICTED)
            params = {
                "g": group_id,
                "q": query,
                "lim": limit,
                "ids": list(restrict_fact_ids),
                "known_as_of": None,
                "include_provenance": self._include_provenance,
                **retrieval_filter_params(filters),
            }
        else:
            sql = text(_FACTS_LATEST)
            params = {
                "g": group_id,
                "q": query,
                "lim": limit,
                "as_of": as_of,
                "known_as_of": known_as_of,
                "include_provenance": self._include_provenance,
                **retrieval_filter_params(filters),
            }
        async with self._session_factory() as session:
            rows = (await session.execute(sql, params)).mappings().all()
        return fact_hits(rows)

    async def hydrate(
        self,
        *,
        group_id: str,
        matches: list[tuple[str, float]],
        limit: int,
        as_of: datetime | None = None,
        known_as_of: datetime | None = None,
        restrict_fact_ids: set[str] | None = None,
        snapshot_id: str | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[FactHit]:
        if not matches:
            return []
        params: dict[str, object] = {
            "g": group_id,
            "lim": limit,
            "as_of": as_of,
            "known_as_of": known_as_of,
            "match_ids": [fact_id for fact_id, _ in matches],
            "match_scores": [score for _, score in matches],
            "include_provenance": self._include_provenance,
            **retrieval_filter_params(filters),
        }
        sql = _FACTS_MATCHED
        if snapshot_id is not None:
            sql = _FACTS_SNAPSHOT_MATCHED
            params |= {
                "snapshot_id": snapshot_id,
                "restrict_ids": (
                    list(restrict_fact_ids) if restrict_fact_ids is not None else None
                ),
            }
        elif restrict_fact_ids is not None:
            if not restrict_fact_ids:
                return []
            sql = _FACTS_RESTRICTED_MATCHED
            params["ids"] = list(restrict_fact_ids)
        async with self._session_factory() as session:
            rows = (await session.execute(text(sql), params)).mappings().all()
        return fact_hits(rows)


def fact_hits(rows: Any) -> list[FactHit]:
    hits: list[FactHit] = []
    for row in rows:
        object_name = row["object_name"] or ""
        sources: Any = row["sources"] or []
        hits.append(
            FactHit(
                fact_key=row["fact_key"],
                fact_id=str(row["fact_id"]),
                subject_name=row["subject_name"],
                predicate=row["predicate"],
                object_name=object_name,
                text=fact_semantic_text(
                    subject_name=row["subject_name"],
                    predicate=row["predicate"],
                    object_name=object_name,
                    object_type=row["object_type"],
                    qualifiers=dict(row["qualifiers"] or {}),
                ),
                authority=float(row["authority"]),
                confidence=float(row["confidence"]),
                lifecycle_state=row["lifecycle_state"],
                score=float(row["score"]),
                valid_from=row["valid_from"],
                supporting_source_ids=tuple(str(s) for s in sources),
                evidence_id=row["evidence_id"],
                evidence_assertion_id=row["evidence_assertion_id"],
                evidence_source_id=row["evidence_source_id"],
                evidence_excerpt=row["evidence_excerpt"],
                evidence_chunk_id=row["evidence_chunk_id"],
                evidence_artifact_version_id=row["evidence_artifact_version_id"],
                evidence_start_offset=row["evidence_start_offset"],
                evidence_end_offset=row["evidence_end_offset"],
                evidence_quote_hash=row["evidence_quote_hash"],
                evidence_content_hash=row["evidence_content_hash"],
                evidence_extraction_run_id=row["evidence_extraction_run_id"],
                evidence_source_coordinates=row["evidence_source_coordinates"],
                evidence_structured_record=row["evidence_structured_record"],
                evidence_citation_uri=row["evidence_citation_uri"],
            )
        )
    return hits


def fact_candidate_queries() -> tuple[str, str, str, str, str, str]:
    return (
        _FACTS_LATEST,
        _FACTS_MATCHED,
        _FACTS_RESTRICTED,
        _FACTS_RESTRICTED_MATCHED,
        _FACTS_SNAPSHOT,
        _FACTS_SNAPSHOT_MATCHED,
    )
