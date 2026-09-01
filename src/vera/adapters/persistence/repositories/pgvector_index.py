"""pgvector passage and code candidates for one active embedding model."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories.passage_index import (
    SqlAlchemyFactCandidateSource,
    fact_candidate_queries,
    fact_hits,
    passage_hit,
    retrieval_filter_params,
)
from vera.domain.ports.embedder import Embedder
from vera.domain.ports.reranker import Reranker
from vera.domain.ports.retrieval_index import (
    FactCandidateSets,
    FactHit,
    PassageHit,
    RetrievalFilters,
)
from vera.observability import get_logger

log = get_logger(__name__)

(
    _FACTS_LATEST,
    _FACTS_MATCHED,
    _FACTS_RESTRICTED,
    _FACTS_RESTRICTED_MATCHED,
    _FACTS_SNAPSHOT,
    _FACTS_SNAPSHOT_MATCHED,
) = fact_candidate_queries()

_ANN = """
SELECT c.id, c.artifact_version_id, c.text, c.content_hash, c.heading_path, c.symbol_name,
       c.start_offset, c.end_offset, c.page_number, c.start_line, c.end_line,
       1 - (ce.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension}))) AS score
FROM chunk_embeddings ce
JOIN chunks c ON c.id = ce.chunk_id
JOIN artifact_versions av ON av.id = c.artifact_version_id
JOIN artifacts a ON a.id = av.artifact_id
JOIN knowledge_sources s ON s.id = a.source_id
LEFT JOIN projects p ON p.id = s.project_id
JOIN workspaces w ON w.id = s.workspace_id
WHERE ce.group_id = :g AND c.group_id = :g
  AND ((s.project_id IS NOT NULL AND p.group_id = c.group_id)
       OR (s.project_id IS NULL AND (w.group_id = c.group_id OR EXISTS (
           SELECT 1 FROM projects wp
           WHERE wp.workspace_id = s.workspace_id AND wp.group_id = c.group_id))))
  AND av.version = (
      SELECT max(visible.version) FROM artifact_versions visible
      WHERE visible.artifact_id = av.artifact_id
        AND (CAST(:created_before AS timestamptz) IS NULL
             OR visible.observed_at <= :created_before))
  AND ce.active
  AND ce.provider = :provider AND ce.model = :model
  AND ce.model_version = :model_version AND ce.dimension = :dimension
  AND (CAST(:created_before AS timestamptz) IS NULL OR c.created_at <= :created_before)
{source_filters}
  AND (CAST(:code_path AS text) IS NULL
       OR coalesce(c.heading_path, '') LIKE '%' || :code_path || '%')
{code_filter}
ORDER BY ce.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension})), c.id ASC
LIMIT :lim
"""

_ANN_SNAPSHOT = """
SELECT sc.chunk_id AS id, sc.artifact_version_id, sc.text, sc.content_hash,
       sc.heading_path, sc.symbol_name,
       sc.start_offset, sc.end_offset, sc.page_number, sc.start_line, sc.end_line,
       1 - (CAST(sce.embedding AS vector({dimension}))
            <=> CAST(:qvec AS vector({dimension}))) AS score
FROM snapshot_chunk_embeddings sce
JOIN snapshot_chunks sc ON sc.snapshot_id = sce.snapshot_id
 AND sc.chunk_id = sce.chunk_id AND sc.group_id = sce.group_id
JOIN snapshot_sources ss ON ss.snapshot_id = sc.snapshot_id
 AND ss.knowledge_source_id = sc.knowledge_source_id AND ss.group_id = sc.group_id
WHERE sce.snapshot_id = CAST(:snapshot_id AS uuid) AND sce.group_id = :g
  AND sce.provider = :provider AND sce.model = :model
  AND sce.model_version = :model_version AND sce.dimension = :dimension
{source_filters}
  AND (CAST(:code_path AS text) IS NULL
       OR coalesce(sc.heading_path, '') LIKE '%' || :code_path || '%')
{code_filter}
ORDER BY CAST(sce.embedding AS vector({dimension}))
         <=> CAST(:qvec AS vector({dimension})), sc.chunk_id ASC
LIMIT :lim
"""

_FACT_ANN = """
SELECT fe.fact_id::text AS fact_id,
       1 - (fe.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension}))) AS score
FROM fact_embeddings fe
WHERE fe.group_id = :g AND fe.active
  AND fe.provider = :provider AND fe.model = :model
  AND fe.model_version = :model_version AND fe.dimension = :dimension
  AND (CAST(:restrict_ids AS uuid[]) IS NULL
       OR fe.fact_id = ANY(CAST(:restrict_ids AS uuid[])))
ORDER BY fe.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension})), fe.fact_id
LIMIT :lim
"""

_FACT_ANN_SNAPSHOT = """
SELECT sfe.fact_id::text AS fact_id,
       1 - (sfe.embedding::vector({dimension})
            <=> CAST(:qvec AS vector({dimension}))) AS score
FROM snapshot_fact_embeddings sfe
WHERE sfe.snapshot_id = CAST(:snapshot_id AS uuid) AND sfe.group_id = :g
  AND sfe.provider = :provider AND sfe.model = :model
  AND sfe.model_version = :model_version AND sfe.dimension = :dimension
  AND (CAST(:restrict_ids AS uuid[]) IS NULL
       OR sfe.fact_id = ANY(CAST(:restrict_ids AS uuid[])))
ORDER BY sfe.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension})), sfe.fact_id
LIMIT :lim
"""

_MATCHED_JOIN = (
    "JOIN unnest(CAST(:match_ids AS uuid[]), CAST(:match_scores AS double precision[])) "
    "AS matched(fact_id, score) ON matched.fact_id = f.id"
)
_SNAPSHOT_MATCHED_JOIN = (
    "JOIN unnest(CAST(:match_ids AS uuid[]), CAST(:match_scores AS double precision[])) "
    "AS matched(fact_id, score) ON matched.fact_id = sf.fact_id"
)
_FACT_ANN_JOIN = """
JOIN (
    SELECT fe.fact_id,
           1 - (fe.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension}))) AS score
    FROM fact_embeddings fe
    WHERE fe.group_id = :g AND fe.active
      AND fe.provider = :provider AND fe.model = :model
      AND fe.model_version = :model_version AND fe.dimension = :dimension
      AND (CAST(:restrict_ids AS uuid[]) IS NULL
           OR fe.fact_id = ANY(CAST(:restrict_ids AS uuid[])))
    ORDER BY fe.embedding::vector({dimension}) <=> CAST(:qvec AS vector({dimension})), fe.fact_id
    LIMIT :semantic_lim
) AS matched ON matched.fact_id = f.id
"""
_FACT_ANN_SNAPSHOT_JOIN = """
JOIN (
    SELECT sfe.fact_id,
           1 - (sfe.embedding::vector({dimension})
                <=> CAST(:qvec AS vector({dimension}))) AS score
    FROM snapshot_fact_embeddings sfe
    WHERE sfe.snapshot_id = CAST(:snapshot_id AS uuid) AND sfe.group_id = :g
      AND sfe.provider = :provider AND sfe.model = :model
      AND sfe.model_version = :model_version AND sfe.dimension = :dimension
      AND (CAST(:restrict_ids AS uuid[]) IS NULL
           OR sfe.fact_id = ANY(CAST(:restrict_ids AS uuid[])))
    ORDER BY sfe.embedding::vector({dimension})
             <=> CAST(:qvec AS vector({dimension})), sfe.fact_id
    LIMIT :semantic_lim
) AS matched ON matched.fact_id = sf.fact_id
"""

_FACT_OR_QUERY = (
    "CAST(replace(CAST(plainto_tsquery('english', :q) AS text), ' & ', ' | ') AS tsquery)"
)
_FACT_LEXICAL_SCORE = """(ts_rank(f.search_vector, q.q)
         + ts_rank(to_tsvector('english', cs.canonical_name), q.q)
         + ts_rank(to_tsvector('english', coalesce(co.canonical_name, '')), q.q))"""
_FACT_LEXICAL_MATCH = """f.search_vector @@ q.q
        OR to_tsvector('english', cs.canonical_name) @@ q.q
        OR to_tsvector('english', coalesce(co.canonical_name, '')) @@ q.q"""
_FACTS_COMBINED_LIVE_LIGHTWEIGHT = f"""
WITH ann AS MATERIALIZED (
    SELECT fe.fact_id,
           1 - (fe.embedding::vector({{dimension}})
                <=> CAST(:qvec AS vector({{dimension}}))) AS score
    FROM fact_embeddings fe
    WHERE fe.group_id = :g AND fe.active
      AND fe.provider = :provider AND fe.model = :model
      AND fe.model_version = :model_version AND fe.dimension = :dimension
    ORDER BY fe.embedding::vector({{dimension}})
             <=> CAST(:qvec AS vector({{dimension}})), fe.fact_id
    LIMIT :semantic_lim
), candidates AS MATERIALIZED (
    SELECT f.fact_key AS fact_key, f.id AS fact_id, f.group_id AS group_id,
           cs.canonical_name AS subject_name, f.predicate AS predicate,
           COALESCE(co.canonical_name, f.object_scalar) AS object_name,
           f.object_type AS object_type, f.qualifiers AS qualifiers,
           fv.authority AS authority, fv.confidence AS confidence,
           fv.lifecycle_state AS lifecycle_state, fv.valid_from AS valid_from,
           CASE WHEN ({_FACT_LEXICAL_MATCH}) THEN {_FACT_LEXICAL_SCORE} END
               AS lexical_score,
           ann.score AS semantic_score
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
    CROSS JOIN (SELECT {_FACT_OR_QUERY} AS q) q
    LEFT JOIN ann ON ann.fact_id = f.id
    WHERE f.group_id = :g
      AND (({_FACT_LEXICAL_MATCH}) OR ann.fact_id IS NOT NULL)
      AND ((CAST(:as_of AS timestamptz) IS NULL
            AND fv.lifecycle_state IN ('active', 'disputed')
            AND (fv.valid_from IS NULL OR fv.valid_from <= now())
            AND (fv.valid_to IS NULL OR fv.valid_to > now()))
           OR (CAST(:as_of AS timestamptz) IS NOT NULL
            AND fv.lifecycle_state <> 'proposed'
            AND (fv.valid_from IS NULL OR fv.valid_from <= :as_of)
            AND (fv.valid_to IS NULL OR fv.valid_to > :as_of)))
      AND (CAST(:known_as_of AS timestamptz) IS NULL OR EXISTS (
          SELECT 1 FROM assertions am
          WHERE am.fact_id = f.id AND am.group_id = f.group_id
            AND am.polarity = 'supports' AND am.state <> 'needs_review'
            AND am.recorded_at <= :known_as_of
            AND (am.withdrawn_at IS NULL OR am.withdrawn_at > :known_as_of)))
      AND (CAST(:min_authority AS double precision) IS NULL
           OR fv.authority >= :min_authority)
      AND (cardinality(CAST(:include_predicates AS text[])) = 0
           OR f.predicate = ANY(CAST(:include_predicates AS text[])))
      AND NOT (f.predicate = ANY(CAST(:exclude_predicates AS text[])))
      AND (:conflict_handling = 'include'
           OR (:conflict_handling = 'exclude' AND fv.lifecycle_state <> 'disputed')
           OR (:conflict_handling = 'only' AND fv.lifecycle_state = 'disputed'))
      AND ((CAST(:repository AS text) IS NULL AND CAST(:branch AS text) IS NULL
            AND CAST(:code_path AS text) IS NULL
            AND CAST(:document_type AS text) IS NULL
            AND CAST(:source_type AS text) IS NULL
            AND CAST(:max_trust_tier AS integer) IS NULL) OR EXISTS (
          SELECT 1 FROM assertions af
          JOIN knowledge_sources fs ON fs.id = af.knowledge_source_id
          LEFT JOIN projects fp ON fp.id = fs.project_id
          JOIN workspaces fw ON fw.id = fs.workspace_id
          WHERE af.fact_id = f.id AND af.group_id = f.group_id
            AND ((CAST(:known_as_of AS timestamptz) IS NULL AND af.state = 'active')
                 OR (CAST(:known_as_of AS timestamptz) IS NOT NULL
                     AND af.state <> 'needs_review'
                     AND af.recorded_at <= :known_as_of
                     AND (af.withdrawn_at IS NULL OR af.withdrawn_at > :known_as_of)))
            AND af.polarity = 'supports'
            AND ((fs.project_id IS NOT NULL AND fp.group_id = f.group_id)
                 OR (fs.project_id IS NULL
                     AND (fw.group_id = f.group_id OR EXISTS (
                         SELECT 1 FROM projects fwp
                         WHERE fwp.workspace_id = fs.workspace_id
                           AND fwp.group_id = f.group_id))))
            AND (CAST(:repository AS text) IS NULL
                 OR canonical_repository_ref(fs.config->>'repository') = :repository)
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
                  AND af.artifact_id = fart.id
                  AND af.knowledge_source_id = fart.source_id
                  AND coalesce(fc.heading_path, '') LIKE '%' || :code_path || '%'
            ))
      ))
), lexical AS MATERIALIZED (
    SELECT 'lexical'::text AS candidate_source, candidates.*,
           lexical_score AS score
    FROM candidates
    WHERE lexical_score IS NOT NULL
    ORDER BY lexical_score DESC, fact_id ASC
    LIMIT :lim
), semantic AS MATERIALIZED (
    SELECT 'semantic'::text AS candidate_source, candidates.*,
           semantic_score AS score
    FROM candidates
    WHERE semantic_score IS NOT NULL
    ORDER BY semantic_score DESC, fact_id ASC
    LIMIT :semantic_lim
), combined AS (
    SELECT * FROM lexical
    UNION ALL
    SELECT * FROM semantic
)
SELECT candidate_source, fact_key, fact_id, subject_name, predicate, object_name,
       object_type, qualifiers, authority, confidence, lifecycle_state, valid_from, score,
       NULL::text[] AS sources, NULL::text AS evidence_id,
       NULL::text AS evidence_assertion_id, NULL::text AS evidence_source_id,
       NULL::text AS evidence_excerpt, NULL::text AS evidence_chunk_id,
       NULL::text AS evidence_artifact_version_id,
       NULL::integer AS evidence_start_offset, NULL::integer AS evidence_end_offset,
       NULL::text AS evidence_quote_hash, NULL::text AS evidence_content_hash,
       NULL::text AS evidence_extraction_run_id,
       NULL::jsonb AS evidence_source_coordinates,
       NULL::jsonb AS evidence_structured_record,
       NULL::text AS evidence_citation_uri
FROM combined
ORDER BY candidate_source, score DESC, fact_id ASC
"""


def _combined_fact_query(*, dimension: int, snapshot: bool, restricted: bool = False) -> str:
    if snapshot:
        lexical = _FACTS_SNAPSHOT
        semantic = _FACTS_SNAPSHOT_MATCHED.replace(
            _SNAPSHOT_MATCHED_JOIN, _FACT_ANN_SNAPSHOT_JOIN.format(dimension=dimension), 1
        )
    else:
        lexical = _FACTS_RESTRICTED if restricted else _FACTS_LATEST
        semantic_template = _FACTS_RESTRICTED_MATCHED if restricted else _FACTS_MATCHED
        semantic = semantic_template.replace(
            _MATCHED_JOIN, _FACT_ANN_JOIN.format(dimension=dimension), 1
        )
    if ":match_ids" in semantic or ":match_scores" in semantic:
        raise RuntimeError("semantic fact query did not replace the matched-candidate join")
    return (
        "SELECT * FROM (SELECT 'lexical' AS candidate_source, lexical.* FROM ("
        + lexical
        + ") AS lexical UNION ALL "
        + "SELECT 'semantic' AS candidate_source, semantic.* FROM ("
        + semantic
        + ") AS semantic) AS combined "
        + "ORDER BY candidate_source, score DESC, fact_id ASC"
    )


_LIVE_SOURCE_FILTERS = """
  AND (CAST(:repository AS text) IS NULL
       OR canonical_repository_ref(s.config->>'repository') = :repository)
  AND (CAST(:branch AS text) IS NULL OR s.config->>'branch' = :branch)
  AND (CAST(:document_type AS text) IS NULL OR s.config->>'document_type' = :document_type)
  AND (CAST(:source_type AS text) IS NULL OR s.kind = :source_type)
  AND (CAST(:max_trust_tier AS integer) IS NULL OR s.trust_tier <= :max_trust_tier)
"""
_SNAPSHOT_SOURCE_FILTERS = """
  AND (CAST(:repository AS text) IS NULL
       OR ss.repository = :repository OR canonical_repository_ref(ss.repository) = :repository)
  AND (CAST(:branch AS text) IS NULL OR ss.branch = :branch)
  AND (CAST(:document_type AS text) IS NULL OR ss.document_type = :document_type)
  AND (CAST(:source_type AS text) IS NULL OR ss.source_type = :source_type)
  AND (CAST(:max_trust_tier AS integer) IS NULL OR ss.trust_tier <= :max_trust_tier)
"""


def vector_literal(vector: list[float]) -> str:
    """pgvector text form, e.g. ``[0.1,0.2]``, cast to ``vector`` in the query."""
    return "[" + ",".join(repr(float(v)) for v in vector) + "]"


class _PgVectorSearch:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        embedder: Embedder,
        *,
        provider: str = "legacy",
        model: str = "legacy-1024",
        model_version: str = "1",
        dimension: int = 1024,
    ) -> None:
        if dimension <= 0:
            raise ValueError("embedding dimension must be positive")
        self._session_factory = session_factory
        self._embedder = embedder
        self._provider = provider
        self._model = model
        self._model_version = model_version
        self._dimension = dimension

    async def _search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        created_before: datetime | None,
        snapshot_id: str | None,
        code: bool,
        filters: RetrievalFilters | None,
    ) -> list[PassageHit]:
        vector = await self._embedder.embed(query)
        if len(vector) != self._dimension:
            raise ValueError(
                f"embedding dimension mismatch: expected {self._dimension}, got {len(vector)}"
            )
        qvec = vector_literal(vector)
        sql = (_ANN_SNAPSHOT if snapshot_id is not None else _ANN).format(
            code_filter=(
                "AND sc.symbol_name IS NOT NULL"
                if code and snapshot_id is not None
                else "AND c.symbol_name IS NOT NULL"
                if code
                else ""
            ),
            dimension=self._dimension,
            source_filters=(
                _SNAPSHOT_SOURCE_FILTERS if snapshot_id is not None else _LIVE_SOURCE_FILTERS
            ),
        )
        async with self._session_factory() as session:
            rows = (
                (
                    await session.execute(
                        text(sql),
                        {
                            "g": group_id,
                            "qvec": qvec,
                            "lim": limit,
                            "created_before": created_before,
                            "snapshot_id": snapshot_id,
                            "provider": self._provider,
                            "model": self._model,
                            "model_version": self._model_version,
                            "dimension": self._dimension,
                            **retrieval_filter_params(filters),
                        },
                    )
                )
                .mappings()
                .all()
            )
        return [passage_hit(r) for r in rows]


class PgVectorPassageIndex(_PgVectorSearch):
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
        return await self._search(
            group_id=group_id,
            query=query,
            limit=limit,
            created_before=created_before,
            snapshot_id=snapshot_id,
            code=False,
            filters=filters,
        )


class PgVectorCodeIndex(_PgVectorSearch):
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
        return await self._search(
            group_id=group_id,
            query=query,
            limit=limit,
            created_before=created_before,
            snapshot_id=snapshot_id,
            code=True,
            filters=filters,
        )


class PgVectorHybridFactCandidateSource:
    """Retrieve lexical and semantic fact sets in one trusted database transaction."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        embedder: Embedder,
        reranker: Reranker,
        *,
        provider: str,
        model: str,
        model_version: str,
        dimension: int,
        min_score: float,
        top_n: int,
        include_provenance: bool = True,
    ) -> None:
        if dimension <= 0:
            raise ValueError("embedding dimension must be positive")
        if not 0 <= min_score <= 1:
            raise ValueError("cross-encoder minimum score must be between zero and one")
        if top_n <= 0:
            raise ValueError("cross-encoder candidate count must be positive")
        self._session_factory = session_factory
        self._lexical = SqlAlchemyFactCandidateSource(session_factory)
        self._embedder = embedder
        self._reranker = reranker
        self._provider = provider
        self._model = model
        self._model_version = model_version
        self._dimension = dimension
        self._min_score = min_score
        self._top_n = top_n
        self._include_provenance = include_provenance
        self._live_query = (
            _combined_fact_query(dimension=dimension, snapshot=False)
            if include_provenance
            else _FACTS_COMBINED_LIVE_LIGHTWEIGHT.format(dimension=dimension)
        )
        self._restricted_query = _combined_fact_query(
            dimension=dimension, snapshot=False, restricted=True
        )
        self._snapshot_query = _combined_fact_query(dimension=dimension, snapshot=True)

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
    ) -> FactCandidateSets:
        if restrict_fact_ids is not None and not restrict_fact_ids:
            return FactCandidateSets(lexical=(), semantic=())
        try:
            vector = await self._embedder.embed(query)
            if len(vector) != self._dimension:
                raise ValueError(
                    f"embedding dimension mismatch: expected {self._dimension}, got {len(vector)}"
                )
            sql = (
                self._snapshot_query
                if snapshot_id is not None
                else self._restricted_query
                if restrict_fact_ids is not None
                else self._live_query
            )
            restricted = list(restrict_fact_ids) if restrict_fact_ids is not None else None
            async with self._session_factory() as session:
                rows = (
                    (
                        await session.execute(
                            text(sql),
                            {
                                "g": group_id,
                                "q": query,
                                "qvec": vector_literal(vector),
                                "lim": limit,
                                "semantic_lim": self._top_n,
                                "as_of": as_of,
                                "known_as_of": known_as_of,
                                "ids": restricted,
                                "restrict_ids": restricted,
                                "snapshot_id": snapshot_id,
                                "provider": self._provider,
                                "model": self._model,
                                "model_version": self._model_version,
                                "dimension": self._dimension,
                                "include_provenance": self._include_provenance,
                                **retrieval_filter_params(filters),
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
        except Exception as exc:
            log.warning("combined_fact_retrieval.failed", error=str(exc))
            lexical = await self._lexical.search(
                group_id=group_id,
                query=query,
                limit=limit,
                as_of=as_of,
                known_as_of=known_as_of,
                restrict_fact_ids=restrict_fact_ids,
                snapshot_id=snapshot_id,
                filters=filters,
            )
            return FactCandidateSets(lexical=tuple(lexical), semantic=())

        lexical = fact_hits(row for row in rows if row["candidate_source"] == "lexical")
        semantic = fact_hits(row for row in rows if row["candidate_source"] == "semantic")
        try:
            scores = await self._reranker.rerank(query=query, facts=[hit.text for hit in semantic])
        except Exception as exc:
            log.warning("semantic_fact_retrieval.failed", error=str(exc))
            return FactCandidateSets(lexical=tuple(lexical), semantic=())
        accepted = [
            replace(hit, score=score)
            for hit, score in zip(semantic, scores, strict=True)
            if score >= self._min_score
        ]
        accepted.sort(key=lambda hit: (-hit.score, hit.fact_key))
        return FactCandidateSets(lexical=tuple(lexical), semantic=tuple(accepted[:limit]))


class PgVectorFactIndex:
    """Semantic fact candidates, rejected by a cross-encoder before authoritative hydration."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        embedder: Embedder,
        reranker: Reranker,
        *,
        provider: str,
        model: str,
        model_version: str,
        dimension: int,
        min_score: float,
        top_n: int,
        include_provenance: bool = True,
    ) -> None:
        if dimension <= 0:
            raise ValueError("embedding dimension must be positive")
        if not 0 <= min_score <= 1:
            raise ValueError("cross-encoder minimum score must be between zero and one")
        if top_n <= 0:
            raise ValueError("cross-encoder candidate count must be positive")
        self._session_factory = session_factory
        self._facts = SqlAlchemyFactCandidateSource(
            session_factory, include_provenance=include_provenance
        )
        self._embedder = embedder
        self._reranker = reranker
        self._provider = provider
        self._model = model
        self._model_version = model_version
        self._dimension = dimension
        self._min_score = min_score
        self._top_n = top_n

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
        if restrict_fact_ids is not None and not restrict_fact_ids:
            return []
        try:
            vector = await self._embedder.embed(query)
            if len(vector) != self._dimension:
                raise ValueError(
                    f"embedding dimension mismatch: expected {self._dimension}, got {len(vector)}"
                )
            candidate_limit = self._top_n
            sql = _FACT_ANN_SNAPSHOT if snapshot_id is not None else _FACT_ANN
            async with self._session_factory() as session:
                rows = (
                    (
                        await session.execute(
                            text(sql.format(dimension=self._dimension)),
                            {
                                "g": group_id,
                                "qvec": vector_literal(vector),
                                "lim": candidate_limit,
                                "snapshot_id": snapshot_id,
                                "restrict_ids": (
                                    list(restrict_fact_ids)
                                    if restrict_fact_ids is not None
                                    else None
                                ),
                                "provider": self._provider,
                                "model": self._model,
                                "model_version": self._model_version,
                                "dimension": self._dimension,
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
            matches = [(str(row["fact_id"]), float(row["score"])) for row in rows]
            hits = await self._facts.hydrate(
                group_id=group_id,
                matches=matches,
                limit=candidate_limit,
                as_of=as_of,
                known_as_of=known_as_of,
                restrict_fact_ids=restrict_fact_ids,
                snapshot_id=snapshot_id,
                filters=filters,
            )
            scores = await self._reranker.rerank(query=query, facts=[hit.text for hit in hits])
            accepted = [
                replace(hit, score=score)
                for hit, score in zip(hits, scores, strict=True)
                if score >= self._min_score
            ]
        except Exception as exc:
            log.warning("semantic_fact_retrieval.failed", error=str(exc))
            return []
        accepted.sort(key=lambda hit: (-hit.score, hit.fact_key))
        return accepted[:limit]
