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
    GraphCommunity,
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
_COMMUNITIES: LiteralString = (
    "MATCH (c:Community) "
    "WHERE c.group_id IN $gids AND c.derived = true "
    "AND ($query = '' OR toLower(c.name) CONTAINS $query "
    "OR toLower(c.summary) CONTAINS $query) "
    "RETURN c.uuid AS community_id, c.name AS name, c.summary AS summary, "
    "c.derivation_run_id AS derivation_run_id, "
    "c.source_fact_set_hash AS source_fact_set_hash, "
    "c.projection_checkpoint AS projection_checkpoint, c.derived AS derived "
    "ORDER BY c.created_at DESC, c.uuid LIMIT $limit"
)


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


def _label_propagation_capped(
    projection: dict[str, list[tuple[str, int]]], *, max_iterations: int = 50
) -> list[list[str]]:
    """Label-propagation clustering with an iteration cap. Graphiti's own version loops until
    convergence with no bound, and oscillates forever on symmetric graphs; the cap makes it
    terminate (taking the labeling reached at the cap), so community construction is reliable.
    """
    community_map = {uuid: i for i, uuid in enumerate(projection)}
    for _ in range(max_iterations):
        changed = False
        next_map: dict[str, int] = {}
        for uuid, neighbors in projection.items():
            tally: dict[int, int] = {}
            for neighbor_uuid, weight in neighbors:
                label = community_map.get(neighbor_uuid, community_map[uuid])
                tally[label] = tally.get(label, 0) + weight
            ranked = sorted(((count, label) for label, count in tally.items()), reverse=True)
            top_count, top_label = ranked[0] if ranked else (0, -1)
            chosen = (
                top_label
                if top_label != -1 and top_count > 1
                else max(top_label, community_map[uuid])
            )
            next_map[uuid] = chosen
            if chosen != community_map[uuid]:
                changed = True
        community_map = next_map
        if not changed:
            break
    clusters: dict[int, list[str]] = {}
    for uuid, label in community_map.items():
        clusters.setdefault(label, []).append(uuid)
    return list(clusters.values())


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

    async def build_communities(self, *, group_id: str) -> tuple[GraphCommunity, ...]:
        # Graphiti clusters the group's entities and writes an LLM summary per community. The
        # community nodes are a rebuildable projection: clear_group plus a re-run reconstructs
        # them, so this is safe to run repeatedly.
        gid = _graphiti_group(str(group_id))
        # Entities ingested via add_triplet persist only uuid/name/group_id, but the community
        # clustering reads full EntityNode records (created_at and summary are required).
        # Backfill them via coalesce (unconditional: FalkorDB treats a missing property as
        # distinct from NULL, so a WHERE ... IS NULL guard would skip them).
        await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
            "MATCH (n:Entity {group_id: $gid}) "
            "SET n.created_at = coalesce(n.created_at, $now), n.summary = coalesce(n.summary, '')",
            gid=gid,
            now=utc_now().isoformat(),
        )
        await self._seed_entity_summaries(gid)
        if self._falkordb:
            # Graphiti's generic clustering issues a per-node neighbor query that returns no
            # rows on FalkorDB, so its native build yields zero communities. Cluster here with a
            # single edge query FalkorDB handles, then reuse Graphiti's LLM community builder.
            return await self._build_communities_falkordb(gid)
        nodes, edges = await self._client.build_communities(group_ids=[gid])
        return await self._community_results(nodes, edges)

    async def _seed_entity_summaries(self, gid: str) -> None:
        # Community summaries must be grounded in the projected active facts, not stale entity
        # prose. The caller rebuilds that projection from PostgreSQL before this method runs.
        fact_rows = cast(
            "Any",
            await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
                "MATCH (n:Entity {group_id: $gid}) "
                "OPTIONAL MATCH (n)-[e:RELATES_TO {group_id: $gid}]-() "
                "RETURN n.uuid AS uuid, n.name AS name, collect(DISTINCT e.fact) AS facts",
                gid=gid,
            ),
        )
        for row in _records(fact_rows):
            facts = [str(fact) for fact in cast("list[Any]", row["facts"] or []) if fact]
            name = str(row["name"])
            summary = f"{name}. {'; '.join(sorted(facts))}"[:800] if facts else name
            await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
                "MATCH (n:Entity {uuid: $uuid}) SET n.summary = $summary",
                uuid=str(row["uuid"]),
                summary=summary,
            )

    async def _community_results(
        self, nodes: Sequence[Any], edges: Sequence[Any]
    ) -> tuple[GraphCommunity, ...]:
        member_ids = sorted({str(edge.target_node_uuid) for edge in edges})
        members = await EntityNode.get_by_uuids(self._client.driver, member_ids)
        member_names = {member.uuid: member.name for member in members}
        by_community: dict[str, list[str]] = {}
        for edge in edges:
            name = member_names.get(str(edge.target_node_uuid))
            if name is not None:
                by_community.setdefault(str(edge.source_node_uuid), []).append(name)
        return tuple(
            GraphCommunity(
                community_id=str(node.uuid),
                name=str(node.name),
                summary=str(node.summary),
                member_names=tuple(sorted(set(by_community.get(str(node.uuid), [])))),
            )
            for node in nodes
        )

    async def _build_communities_falkordb(self, gid: str) -> tuple[GraphCommunity, ...]:
        from graphiti_core.nodes import EntityNode
        from graphiti_core.utils.maintenance.community_operations import (
            build_community,
            remove_communities,
        )

        await remove_communities(self._client.driver, group_ids=[gid])
        result = cast(
            "Any",
            await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
                "MATCH (n:Entity {group_id: $gid})-[e:RELATES_TO]-(m:Entity {group_id: $gid}) "
                "RETURN n.uuid AS a, m.uuid AS b, count(e) AS c",
                gid=gid,
            ),
        )
        rows = cast(
            "list[dict[str, Any]]", result[0] if isinstance(result, tuple) and result else []
        )
        projection: dict[str, list[tuple[str, int]]] = {}
        for row in rows:
            projection.setdefault(str(row["a"]), []).append((str(row["b"]), int(row["c"])))
        communities: list[Any] = []
        community_edges: list[Any] = []
        for cluster in _label_propagation_capped(projection):
            nodes = await EntityNode.get_by_uuids(self._client.driver, cluster)
            if not nodes:
                continue
            community, edges = await build_community(self._client.llm_client, nodes)
            await community.generate_name_embedding(self._client.embedder)
            await community.save(self._client.driver)
            for edge in edges:
                await edge.save(self._client.driver)
            communities.append(community)
            community_edges.extend(edges)
        return await self._community_results(communities, community_edges)

    async def annotate_community(
        self,
        *,
        group_id: str,
        community_id: str,
        derivation_run_id: str,
        source_fact_set_hash: str,
        projection_checkpoint: str,
    ) -> None:
        await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
            "MATCH (c:Community {group_id: $gid, uuid: $community_id}) "
            "SET c.derivation_run_id = $run_id, c.source_fact_set_hash = $fact_hash, "
            "c.projection_checkpoint = $checkpoint, c.derived = true",
            gid=_graphiti_group(group_id),
            community_id=community_id,
            run_id=derivation_run_id,
            fact_hash=source_fact_set_hash,
            checkpoint=projection_checkpoint,
        )

    async def search_communities(
        self, *, group_ids: Sequence[GroupId], query: str | None, limit: int
    ) -> Sequence[GraphCommunity]:
        if not group_ids:
            return []
        result = cast(
            "Any",
            await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
                _COMMUNITIES,
                gids=[_graphiti_group(str(group_id)) for group_id in group_ids],
                query=(query or "").lower(),
                limit=limit,
            ),
        )
        return [
            GraphCommunity(
                community_id=str(row["community_id"]),
                name=str(row["name"]),
                summary=str(row["summary"]),
                derivation_run_id=str(row["derivation_run_id"]),
                source_fact_set_hash=str(row["source_fact_set_hash"]),
                projection_checkpoint=str(row["projection_checkpoint"]),
                derived=bool(row["derived"]),
            )
            for row in _records(result)
        ]
