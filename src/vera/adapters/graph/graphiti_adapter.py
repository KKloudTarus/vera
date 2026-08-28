"""GraphitiMemoryEngine: the anti-corruption layer around graphiti-core.

Only this module imports ``graphiti_core``. It translates VERA's EpisodeSpec and
GraphQuery to Graphiti calls and maps results back to VERA types. Two ingestion
paths: structured triples via ``add_triplet`` (no LLM, deterministic) when the
payload carries triples, and text via ``add_episode`` (LLM extraction) otherwise.
Search uses the RRF recipe (no cross-encoder) and builds a valid-time ``as_of``
filter, since Graphiti has no ``as_of`` parameter.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any, LiteralString, cast

from graphiti_core import Graphiti
from graphiti_core.driver.driver import GraphProvider
from graphiti_core.edges import EntityEdge
from graphiti_core.nodes import EntityNode, EpisodeType
from graphiti_core.search.search_config_recipes import EDGE_HYBRID_SEARCH_RRF
from graphiti_core.search.search_filters import (
    ComparisonOperator,
    DateFilter,
    SearchFilters,
)

from vera.domain.ontology import EDGE_TYPE_MAP, EDGE_TYPES, ENTITY_TYPES
from vera.domain.ports.memory_engine import (
    EpisodeSpec,
    GraphHit,
    GraphNodeRef,
    GraphQuery,
    IngestReceipt,
)
from vera.domain.ports.projection import FactProjection
from vera.observability import span
from vera.shared.ids import deterministic_id
from vera.shared.time import utc_now
from vera.shared.types import GroupId

_EPISODE_TYPES = {
    "message": EpisodeType.message,
    "json": EpisodeType.json,
    "text": EpisodeType.text,
}

# FalkorDB edge search, two halves fused with RRF to match Neo4j's hybrid stage 1. Both
# apply the same bi-temporal filter; ISO-8601 timestamps compare correctly as strings
# (all UTC, +00:00). Graphiti's own hybrid search returns nothing on FalkorDB, but its edge
# fulltext index and edge fact_embedding (populated by add_triplet) both work directly.
_FALKOR_EDGE_FULLTEXT: LiteralString = (
    "CALL db.idx.fulltext.queryRelationships('RELATES_TO', $q) YIELD relationship, score "
    "WITH relationship AS e, score "
    "WHERE e.group_id = $gid AND (e.valid_at IS NULL OR e.valid_at <= $asof) "
    "AND (e.invalid_at IS NULL OR e.invalid_at > $asof) "
    "RETURN e.fact AS fact, e.uuid AS uuid, e.valid_at AS valid_at, "
    "e.invalid_at AS invalid_at, score ORDER BY score DESC LIMIT $limit"
)
_FALKOR_EDGE_VECTOR: LiteralString = (
    "MATCH (n:Entity)-[e:RELATES_TO {group_id: $gid}]->(m:Entity) "
    "WHERE e.fact_embedding IS NOT NULL AND (e.valid_at IS NULL OR e.valid_at <= $asof) "
    "AND (e.invalid_at IS NULL OR e.invalid_at > $asof) "
    "WITH e, (2 - vec.cosineDistance(e.fact_embedding, vecf32($v)))/2 AS score "
    "RETURN e.fact AS fact, e.uuid AS uuid, e.valid_at AS valid_at, "
    "e.invalid_at AS invalid_at, score ORDER BY score DESC LIMIT $limit"
)
# Alphanumeric terms only, so a natural-language query is a safe RediSearch fulltext input.
_FULLTEXT_TOKEN = re.compile(r"[A-Za-z0-9]+")
_RRF_K = 1  # reciprocal-rank-fusion constant, matching the Neo4j RRF recipe


def _rrf_fuse(result_lists: list[list[Any]], limit: int) -> list[GraphHit]:
    """Fuse ranked edge-row lists with reciprocal rank fusion (score = sum 1/(k+rank))."""
    fused: dict[str, tuple[float, GraphHit]] = {}
    for rows in result_lists:
        for rank, row in enumerate(rows, start=1):
            uuid = row["uuid"]
            contribution = 1.0 / (_RRF_K + rank)
            if uuid in fused:
                accumulated, hit = fused[uuid]
                fused[uuid] = (accumulated + contribution, hit)
            else:
                fused[uuid] = (
                    contribution,
                    GraphHit(
                        fact=row["fact"],
                        score=0.0,
                        edge_uuid=uuid,
                        valid_at=_parse_iso(row.get("valid_at")),
                        invalid_at=_parse_iso(row.get("invalid_at")),
                    ),
                )
    ranked = sorted(fused.values(), key=lambda pair: pair[0], reverse=True)
    return [replace(hit, score=round(score, 6)) for score, hit in ranked[:limit]]


def _parse_iso(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _neighbors_query(hops: int) -> str:
    return (
        f"MATCH path=(s:Entity {{group_id: $gid, name: $center}})"
        f"-[:RELATES_TO*1..{hops}]-(:Entity) "
        "UNWIND relationships(path) AS r WITH DISTINCT r "
        "WHERE r.group_id = $gid AND r.invalid_at IS NULL "
        "RETURN r.fact AS fact, r.uuid AS uuid LIMIT $limit"
    )


# A LiteralString per allowed depth, so the driver's typed execute_query accepts it.
_NEIGHBORS_QUERIES: dict[int, str] = {
    1: _neighbors_query(1),
    2: _neighbors_query(2),
    3: _neighbors_query(3),
}


def _records(result: Any) -> list[Any]:
    # neo4j's execute_query returns an EagerResult(records, summary, keys); the driver is
    # untyped, so treat the records attribute as Any and fall back to a tuple's first item.
    records: Any = getattr(result, "records", None)
    if records is None:
        records = cast("Any", result)[0] if isinstance(result, tuple) and result else []
    return list(records)


def _graphiti_group(group_id: str) -> str:
    # Graphiti allows only [A-Za-z0-9_-]; VERA scopes use a colon (o:, w:, p:, u:).
    # The mapping is deterministic, so ingest and search stay consistent.
    return group_id.replace(":", "_")


def _entity_type(labels: list[str] | None) -> str:
    extra = [label for label in (labels or []) if label != "Entity"]
    return extra[0] if extra else "Entity"


def _as_of_filter(as_of: datetime | None) -> SearchFilters | None:
    if as_of is None:
        return None
    # valid_at <= T AND (invalid_at IS NULL OR invalid_at > T)
    return SearchFilters(
        valid_at=[[DateFilter(date=as_of, comparison_operator=ComparisonOperator.less_than_equal)]],
        invalid_at=[
            [DateFilter(date=as_of, comparison_operator=ComparisonOperator.is_null)],
            [DateFilter(date=as_of, comparison_operator=ComparisonOperator.greater_than)],
        ],
    )


class GraphitiMemoryEngine:
    def __init__(self, client: Graphiti) -> None:
        self._client = client
        driver = getattr(client, "driver", None)
        self._falkordb = getattr(driver, "provider", None) == GraphProvider.FALKORDB

    def fact_projection(self) -> FactProjection:
        """A FactProjection over this engine's client, so the fact projection reuses the one
        graph connection rather than opening a second (keeps the client encapsulated here).
        """
        from vera.adapters.graph.fact_projection import GraphitiFactProjection

        return GraphitiFactProjection(self._client)

    async def ensure_schema(self) -> None:
        await self._client.build_indices_and_constraints()

    async def ingest_episode(self, episode: EpisodeSpec) -> IngestReceipt:
        episode_uuid = str(deterministic_id(str(episode.source_id)))
        triples = episode.metadata.get("triples")
        if triples:
            return await self._ingest_triples(episode, triples, episode_uuid)
        return await self._ingest_text(episode, episode_uuid)

    async def _ingest_text(self, episode: EpisodeSpec, episode_uuid: str) -> IngestReceipt:
        source = _EPISODE_TYPES.get(episode.knowledge_type, EpisodeType.text)
        # add_episode treats a non-null ``uuid`` as an existing episode to update and
        # fails if it is absent, so a new episode is created with uuid=None and its
        # generated uuid is returned. Re-ingestion is guarded upstream (queue dedup,
        # content-idempotent curation) and a rebuild wipes the group first.
        with span("graph.add_episode", knowledge_type=episode.knowledge_type):
            results = await self._client.add_episode(
                name=str(episode.source_id),
                episode_body=episode.body,
                source_description="vera",
                reference_time=episode.reference_time,
                source=source,
                group_id=_graphiti_group(str(episode.group_id)),
                entity_types=ENTITY_TYPES,
                edge_types=EDGE_TYPES,
                edge_type_map=EDGE_TYPE_MAP,
            )
        nodes = tuple(
            GraphNodeRef(uuid=n.uuid, name=n.name, entity_type=_entity_type(n.labels))
            for n in results.nodes
        )
        return IngestReceipt(
            episode_uuid=results.episode.uuid,
            nodes=nodes,
            edge_uuids=tuple(e.uuid for e in results.edges),
        )

    async def _ingest_triples(
        self, episode: EpisodeSpec, triples: list[dict[str, Any]], episode_uuid: str
    ) -> IngestReceipt:
        gid = _graphiti_group(str(episode.group_id))
        nodes: dict[str, GraphNodeRef] = {}
        edge_uuids: list[str] = []
        with span("graph.add_triplet", triples=len(triples)):
            return await self._add_triplets(episode, triples, episode_uuid, gid, nodes, edge_uuids)

    async def _add_triplets(
        self,
        episode: EpisodeSpec,
        triples: list[dict[str, Any]],
        episode_uuid: str,
        gid: str,
        nodes: dict[str, GraphNodeRef],
        edge_uuids: list[str],
    ) -> IngestReceipt:
        for triple in triples:
            subject = str(triple["subject"])
            predicate = str(triple["predicate"])
            obj = str(triple["object"])
            entity_type = str(triple.get("entity_type", "Entity"))
            source_node = EntityNode(name=subject, group_id=gid, labels=["Entity", entity_type])
            target_node = EntityNode(name=obj, group_id=gid, labels=["Entity", entity_type])
            edge = EntityEdge(
                group_id=gid,
                source_node_uuid=source_node.uuid,
                target_node_uuid=target_node.uuid,
                created_at=utc_now(),
                name=predicate,
                fact=f"{subject} {predicate} {obj}",
                valid_at=episode.reference_time,
            )
            result = await self._client.add_triplet(source_node, edge, target_node)
            for node in result.nodes:
                nodes[node.uuid] = GraphNodeRef(
                    uuid=node.uuid, name=node.name, entity_type=_entity_type(node.labels)
                )
            edge_uuids.extend(e.uuid for e in result.edges)
            # add_triplet does not reconcile contradictions (that is add_episode's job),
            # so when curation marks specific prior objects as superseded, close their
            # edges' valid time here. Current search hides them; an as_of query returns them.
            superseded_objects = triple.get("supersede_objects")
            if superseded_objects:
                await self._invalidate_objects(gid, subject, predicate, list(superseded_objects))
        return IngestReceipt(
            episode_uuid=episode_uuid, nodes=tuple(nodes.values()), edge_uuids=tuple(edge_uuids)
        )

    async def _invalidate_objects(
        self, gid: str, subject: str, predicate: str, objects: list[str]
    ) -> None:
        await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
            """
            MATCH (s:Entity {group_id: $gid, name: $subject})
                  -[e:RELATES_TO {group_id: $gid, name: $predicate}]->(m:Entity)
            WHERE m.name IN $objects AND e.invalid_at IS NULL
            SET e.invalid_at = $now
            """,
            gid=gid,
            subject=subject,
            predicate=predicate,
            objects=objects,
            now=utc_now(),
        )

    async def search(self, query: GraphQuery) -> Sequence[GraphHit]:
        if not query.group_ids:
            return []
        # Graphiti's fulltext filter joins group_ids as `group_id:"a" OR group_id:"b"
        # AND (query)`. Lucene's OR/AND precedence makes that parse depend on group
        # order, so a multi-group search silently drops results unless a group with
        # matching data happens to sort last. Search each group on its own (a valid
        # single-group query) and merge, which is order-independent and correct.
        per_group = await asyncio.gather(
            *(self._search_one(query, group_id) for group_id in query.group_ids)
        )
        by_edge: dict[str, GraphHit] = {}
        for hits in per_group:
            for hit in hits:
                key = hit.edge_uuid or f"anon-{len(by_edge)}"
                existing = by_edge.get(key)
                if existing is None or hit.score > existing.score:
                    by_edge[key] = hit
        merged = sorted(by_edge.values(), key=lambda h: h.score, reverse=True)
        return merged[: query.limit]

    async def _search_one(self, query: GraphQuery, group_id: GroupId) -> list[GraphHit]:
        # Default to "as of now" so a superseded (invalidated) edge is hidden from the
        # current view; an explicit as_of returns the historical state instead.
        as_of = query.as_of or utc_now()
        if self._falkordb:
            return await self._falkordb_search(query, group_id, as_of)
        config = EDGE_HYBRID_SEARCH_RRF.model_copy(
            update={"limit": query.limit, "reranker_min_score": 0}
        )
        with span("graph.search_group"):
            results = await self._client.search_(
                query.text,
                config=config,
                group_ids=[_graphiti_group(str(group_id))],
                search_filter=_as_of_filter(as_of),
            )
        scores = results.edge_reranker_scores or []
        hits: list[GraphHit] = []
        for index, edge in enumerate(results.edges):
            score = float(scores[index]) if index < len(scores) else 0.0
            hits.append(
                GraphHit(
                    fact=edge.fact,
                    score=score,
                    edge_uuid=edge.uuid,
                    valid_at=edge.valid_at,
                    invalid_at=edge.invalid_at,
                )
            )
        return hits

    async def _falkordb_search(
        self, query: GraphQuery, group_id: GroupId, as_of: datetime
    ) -> list[GraphHit]:
        # Graphiti's hybrid search returns nothing on FalkorDB, so VERA runs the two halves
        # itself and fuses them with RRF, the same shape as Neo4j's stage 1: a fulltext half
        # over the edge fulltext index and a vector half over the edge fact_embedding (which
        # add_triplet does populate on FalkorDB). Both apply the bi-temporal filter; ISO-8601
        # timestamps compare correctly as strings (all UTC, +00:00).
        gid = _graphiti_group(str(group_id))
        asof = as_of.isoformat()
        result_lists: list[list[Any]] = []
        with span("graph.search_group.falkordb"):
            # Fulltext half. Terms are OR-joined so a natural-language query matches any term
            # (RediSearch defaults to AND, which a full sentence rarely satisfies).
            terms = "|".join(_FULLTEXT_TOKEN.findall(query.text))
            if terms:
                ft = cast(
                    "Any",
                    await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
                        _FALKOR_EDGE_FULLTEXT, q=terms, gid=gid, asof=asof, limit=query.limit
                    ),
                )
                result_lists.append(_records(ft))
            # Vector half, embedding the query with the same embedder used at ingestion.
            vector = await self._embed_query(query.text)
            if vector is not None:
                vec = cast(
                    "Any",
                    await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
                        _FALKOR_EDGE_VECTOR, v=vector, gid=gid, asof=asof, limit=query.limit
                    ),
                )
                result_lists.append(_records(vec))
        return _rrf_fuse(result_lists, query.limit)

    async def _embed_query(self, text: str) -> list[float] | None:
        embedder = getattr(self._client, "embedder", None)
        if embedder is None:
            return None
        try:
            return await embedder.create(text)
        except Exception:  # the vector half is optional; degrade to fulltext-only on failure
            return None

    async def neighbors(
        self, *, group_ids: Sequence[GroupId], center: str, depth: int, limit: int
    ) -> Sequence[GraphHit]:
        # Variable-length bound cannot be a query parameter, so clamp and inline it.
        # Variable-length bound cannot be a query parameter; select a fixed literal query
        # per clamped depth so the driver still receives a LiteralString.
        query = cast("LiteralString", _NEIGHBORS_QUERIES[max(1, min(int(depth), 3))])
        merged: dict[str, GraphHit] = {}
        with span("graph.neighbors", depth=int(depth)):
            for group_id in group_ids:
                gid = _graphiti_group(str(group_id))
                result = cast(
                    "Any",
                    await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
                        query, gid=gid, center=center, limit=limit
                    ),
                )
                for record in _records(result):
                    uuid = record["uuid"]
                    merged.setdefault(
                        uuid, GraphHit(fact=record["fact"], score=0.0, edge_uuid=uuid)
                    )
        return list(merged.values())[:limit]

    async def health(self) -> bool:
        try:
            await self._client.driver.execute_query("RETURN 1")  # pyright: ignore[reportUnknownMemberType]
        except Exception:  # health reports status instead of raising
            return False
        return True

    async def retract_episode(self, *, group_id: str, edge_uuids: Sequence[str]) -> None:
        if not edge_uuids:
            return
        gid = _graphiti_group(str(group_id))
        # Delete the named edges, then any entity node in the group left with no relations.
        await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
            """
            MATCH ()-[e:RELATES_TO {group_id: $gid}]->()
            WHERE e.uuid IN $edge_uuids
            DELETE e
            """,
            gid=gid,
            edge_uuids=list(edge_uuids),
        )
        await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
            "MATCH (n:Entity {group_id: $gid}) WHERE NOT (n)--() DELETE n", gid=gid
        )

    async def clear_group(self, group_id: str) -> None:
        # Delete every node (and its edges) for the group so a reprocess rebuilds it
        # from Postgres. Episodic and entity nodes both carry group_id.
        gid = _graphiti_group(str(group_id))
        await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
            "MATCH (n {group_id: $gid}) DETACH DELETE n", gid=gid
        )

    async def build_communities(self, *, group_id: str) -> int:
        # Graphiti clusters the group's entities and writes an LLM summary per community. The
        # community nodes are a rebuildable projection: clear_group plus a re-run reconstructs
        # them, so this is safe to run repeatedly.
        nodes, _edges = await self._client.build_communities(
            group_ids=[_graphiti_group(str(group_id))]
        )
        return len(nodes)
