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
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from graphiti_core import Graphiti
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
from vera.observability import span
from vera.shared.ids import deterministic_id
from vera.shared.time import utc_now
from vera.shared.types import GroupId

_EPISODE_TYPES = {
    "message": EpisodeType.message,
    "json": EpisodeType.json,
    "text": EpisodeType.text,
}


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
        with span("graph.add_episode", knowledge_type=episode.knowledge_type):
            results = await self._client.add_episode(
                name=str(episode.source_id),
                episode_body=episode.body,
                source_description="vera",
                reference_time=episode.reference_time,
                source=source,
                group_id=_graphiti_group(str(episode.group_id)),
                uuid=episode_uuid,
                entity_types=ENTITY_TYPES,
                edge_types=EDGE_TYPES,
                edge_type_map=EDGE_TYPE_MAP,
            )
        nodes = tuple(
            GraphNodeRef(uuid=n.uuid, name=n.name, entity_type=_entity_type(n.labels))
            for n in results.nodes
        )
        return IngestReceipt(
            episode_uuid=episode_uuid,
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
        return IngestReceipt(
            episode_uuid=episode_uuid, nodes=tuple(nodes.values()), edge_uuids=tuple(edge_uuids)
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
        config = EDGE_HYBRID_SEARCH_RRF.model_copy(
            update={"limit": query.limit, "reranker_min_score": 0}
        )
        with span("graph.search_group"):
            results = await self._client.search_(
                query.text,
                config=config,
                group_ids=[_graphiti_group(str(group_id))],
                search_filter=_as_of_filter(query.as_of),
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

    async def health(self) -> bool:
        try:
            await self._client.driver.execute_query("RETURN 1")  # pyright: ignore[reportUnknownMemberType]
        except Exception:  # health reports status instead of raising
            return False
        return True

    async def clear_group(self, group_id: str) -> None:
        # Delete every node (and its edges) for the group so a reprocess rebuilds it
        # from Postgres. Episodic and entity nodes both carry group_id.
        gid = _graphiti_group(str(group_id))
        await self._client.driver.execute_query(  # pyright: ignore[reportUnknownMemberType]
            "MATCH (n {group_id: $gid}) DETACH DELETE n", gid=gid
        )
