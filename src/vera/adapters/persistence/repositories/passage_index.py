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

from vera.domain.ports.retrieval_index import FactHit, PassageHit, RetrievalFilters

# Candidate generation favors recall: OR the query lexemes (plainto_tsquery ANDs them, so a
# multi-word natural query would never match a terse fact doc). ts_rank still orders by how
# well each row matches, and the downstream blend and diversity handle precision.
_ORQ = "CAST(replace(CAST(plainto_tsquery('english', :q) AS text), ' & ', ' | ') AS tsquery)"

_PASSAGE = f"""
SELECT c.id, c.artifact_version_id, c.text, c.heading_path, c.symbol_name,
       c.start_offset, c.end_offset, c.page_number, c.start_line, c.end_line,
       ts_rank(c.search_vector, {_ORQ}) AS score
FROM chunks c
JOIN artifact_versions av ON av.id = c.artifact_version_id
JOIN artifacts a ON a.id = av.artifact_id
JOIN knowledge_sources s ON s.id = a.source_id
WHERE c.group_id = :g AND c.search_vector @@ {_ORQ}
  AND (CAST(:created_before AS timestamptz) IS NULL OR c.created_at <= :created_before)
  AND (CAST(:repository AS text) IS NULL OR s.config->>'repository' = :repository)
  AND (CAST(:branch AS text) IS NULL OR s.config->>'branch' = :branch)
  AND (CAST(:code_path AS text) IS NULL
       OR coalesce(c.heading_path, '') LIKE '%' || :code_path || '%')
  AND (CAST(:document_type AS text) IS NULL OR s.config->>'document_type' = :document_type)
  AND (CAST(:source_type AS text) IS NULL OR s.kind = :source_type)
  AND (CAST(:max_trust_tier AS integer) IS NULL OR s.trust_tier <= :max_trust_tier)
{{code_filter}}
ORDER BY score DESC
LIMIT :lim
"""

_FACTS_TMPL = f"""
SELECT f.fact_key AS fact_key, f.id AS fact_id, cs.canonical_name AS subject_name,
       f.predicate AS predicate, COALESCE(co.canonical_name, f.object_scalar) AS object_name,
       f.authority AS authority, f.confidence AS confidence,
       f.lifecycle_state AS lifecycle_state, f.valid_from AS valid_from,
       (ts_rank(f.search_vector, q.q)
        + ts_rank(to_tsvector('english', cs.canonical_name), q.q)
        + ts_rank(to_tsvector('english', coalesce(co.canonical_name, '')), q.q)) AS score,
       sup.sources AS sources
FROM facts f
JOIN canonical_entities cs ON cs.id = f.subject_entity_id
LEFT JOIN canonical_entities co ON co.id = f.object_entity_id
CROSS JOIN (SELECT {_ORQ} AS q) q
LEFT JOIN LATERAL (
    SELECT array_agg(DISTINCT a.knowledge_source_id::text)
           FILTER (WHERE a.knowledge_source_id IS NOT NULL) AS sources
    FROM assertions a
    WHERE a.fact_id = f.id AND a.state = 'active' AND a.polarity = 'supports'
) sup ON true
WHERE f.group_id = :g
  AND (f.search_vector @@ q.q
       OR to_tsvector('english', cs.canonical_name) @@ q.q
       OR to_tsvector('english', coalesce(co.canonical_name, '')) @@ q.q)
  AND {{membership}}
  AND (CAST(:min_authority AS double precision) IS NULL OR f.authority >= :min_authority)
  AND (cardinality(CAST(:include_predicates AS text[])) = 0
       OR f.predicate = ANY(CAST(:include_predicates AS text[])))
  AND NOT (f.predicate = ANY(CAST(:exclude_predicates AS text[])))
  AND (:conflict_handling = 'include'
       OR (:conflict_handling = 'exclude' AND f.lifecycle_state <> 'disputed')
       OR (:conflict_handling = 'only' AND f.lifecycle_state = 'disputed'))
  AND ((CAST(:repository AS text) IS NULL AND CAST(:branch AS text) IS NULL
        AND CAST(:code_path AS text) IS NULL AND CAST(:document_type AS text) IS NULL
        AND CAST(:source_type AS text) IS NULL
        AND CAST(:max_trust_tier AS integer) IS NULL)
       OR EXISTS (
           SELECT 1 FROM assertions af
           LEFT JOIN knowledge_sources src ON src.id = af.knowledge_source_id
           LEFT JOIN artifact_versions fav ON fav.id = af.artifact_version_id
           LEFT JOIN chunks fc ON fc.artifact_version_id = fav.id
           WHERE af.fact_id = f.id AND af.state = 'active'
             AND (CAST(:repository AS text) IS NULL OR src.config->>'repository' = :repository)
             AND (CAST(:branch AS text) IS NULL OR src.config->>'branch' = :branch)
             AND (CAST(:code_path AS text) IS NULL
                  OR coalesce(fc.heading_path, '') LIKE '%' || :code_path || '%')
             AND (CAST(:document_type AS text) IS NULL
                  OR src.config->>'document_type' = :document_type)
             AND (CAST(:source_type AS text) IS NULL OR src.kind = :source_type)
             AND (CAST(:max_trust_tier AS integer) IS NULL
                  OR src.trust_tier <= :max_trust_tier)
       ))
ORDER BY score DESC
LIMIT :lim
"""

# Latest view: currently active or disputed facts, honoring an optional as_of valid-time.
_FACTS_LATEST = _FACTS_TMPL.format(
    membership=(
        "f.lifecycle_state IN ('active', 'disputed') "
        "AND (CAST(:as_of AS timestamptz) IS NULL OR f.valid_from IS NULL "
        "     OR f.valid_from <= :as_of) "
        "AND (CAST(:as_of AS timestamptz) IS NULL OR f.valid_to IS NULL "
        "     OR f.valid_to > :as_of)"
    )
)
# Snapshot view: exactly the frozen fact revisions, regardless of their current lifecycle.
_FACTS_SNAPSHOT = _FACTS_TMPL.format(membership="f.id = ANY(CAST(:ids AS uuid[]))")


def passage_hit(row: Any) -> PassageHit:
    return PassageHit(
        chunk_id=str(row["id"]),
        artifact_version_id=str(row["artifact_version_id"]),
        text=row["text"],
        score=float(row["score"]),
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
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        async with self._session_factory() as session:
            rows = (
                (
                    await session.execute(
                        text(_PASSAGE.format(code_filter="")),
                        {
                            "g": group_id,
                            "q": query,
                            "lim": limit,
                            "created_before": created_before,
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
        filters: RetrievalFilters | None = None,
    ) -> list[PassageHit]:
        async with self._session_factory() as session:
            rows = (
                (
                    await session.execute(
                        text(_PASSAGE.format(code_filter="AND symbol_name IS NOT NULL")),
                        {
                            "g": group_id,
                            "q": query,
                            "lim": limit,
                            "created_before": created_before,
                            **retrieval_filter_params(filters),
                        },
                    )
                )
                .mappings()
                .all()
            )
        return [passage_hit(r) for r in rows]


class SqlAlchemyFactCandidateSource:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        limit: int,
        as_of: datetime | None = None,
        restrict_fact_ids: set[str] | None = None,
        filters: RetrievalFilters | None = None,
    ) -> list[FactHit]:
        if restrict_fact_ids is not None:
            if not restrict_fact_ids:
                return []  # an empty snapshot contains no facts
            sql = text(_FACTS_SNAPSHOT)
            params: dict[str, object] = {
                "g": group_id,
                "q": query,
                "lim": limit,
                "ids": list(restrict_fact_ids),
                **retrieval_filter_params(filters),
            }
        else:
            sql = text(_FACTS_LATEST)
            params = {
                "g": group_id,
                "q": query,
                "lim": limit,
                "as_of": as_of,
                **retrieval_filter_params(filters),
            }
        async with self._session_factory() as session:
            rows = (await session.execute(sql, params)).mappings().all()
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
                    text=f"{row['subject_name']} {row['predicate']} {object_name}".strip(),
                    authority=float(row["authority"]),
                    confidence=float(row["confidence"]),
                    lifecycle_state=row["lifecycle_state"],
                    score=float(row["score"]),
                    valid_from=row["valid_from"],
                    supporting_source_ids=tuple(str(s) for s in sources),
                )
            )
        return hits
