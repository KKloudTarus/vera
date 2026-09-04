"""Graphiti projection of the authoritative fact store (Phase 3).

Projects approved Facts as temporal ``RELATES_TO`` edges keyed by ``fact_key``, between the
subject and object ``Entity`` nodes, carrying validity, aggregates, and the ids of the
episodes that currently support the fact. This is the only place these graph writes happen;
Graphiti stays non-authoritative and the projection is rebuildable from Postgres (ADR-0003).

The writes use the same lower-level driver path the search adapter already uses
(``client.driver.execute_query``), which is covered by the Graphiti compatibility contract
tests so a Graphiti bump that changes it fails loudly. An edge upsert is a delete-by-key
followed by a create, in two statements, because a leading ``MATCH`` that finds nothing would
otherwise short-circuit a combined statement.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from vera.domain.ports.projection import ProjectedFact
from vera.shared.ids import deterministic_id
from vera.shared.time import utc_now


def _graphiti_group(group_id: str) -> str:
    # Graphiti allows only [A-Za-z0-9_-]; VERA scopes use a colon. Same deterministic mapping
    # the search adapter uses, so projection and search stay consistent.
    return group_id.replace(":", "_")


def _records(result: Any) -> list[Any]:
    # neo4j returns an EagerResult(records, ...); the FalkorDB driver returns a tuple.
    records: Any = getattr(result, "records", None)
    if records is None:
        records = cast("Any", result)[0] if isinstance(result, tuple) and result else []
    return list(records)


_DELETE_EDGE = """
MATCH ()-[e:RELATES_TO {group_id: $gid, fact_key: $fact_key}]->()
DELETE e
"""

_CREATE_EDGE = """
MERGE (s:Entity {group_id: $gid, name: $subject})
  ON CREATE SET s.uuid = $subject_uuid
MERGE (o:Entity {group_id: $gid, name: $object})
  ON CREATE SET o.uuid = $object_uuid
CREATE (s)-[e:RELATES_TO {group_id: $gid, fact_key: $fact_key}]->(o)
SET e.uuid = $fact_key, e.name = $predicate, e.fact = $fact,
    e.created_at = $created_at, e.episodes = $episodes,
    e.valid_at = $valid_at, e.invalid_at = $invalid_at,
    e.authority = $authority, e.confidence = $confidence,
    e.supporting = $supporting
"""

_LIST_KEYS = """
MATCH ()-[e:RELATES_TO {group_id: $gid}]->()
WHERE e.fact_key IS NOT NULL
RETURN e.fact_key AS fact_key
"""

_CLEAR = "MATCH (n {group_id: $gid}) DETACH DELETE n"


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


class GraphitiFactProjection:
    """Implements the ``FactProjection`` port over a Graphiti client's driver."""

    def __init__(self, client: Any) -> None:
        self._client = client

    async def project(self, fact: ProjectedFact) -> None:
        gid = _graphiti_group(fact.group_id)
        await self._client.driver.execute_query(_DELETE_EDGE, gid=gid, fact_key=fact.fact_key)
        await self._client.driver.execute_query(
            _CREATE_EDGE,
            gid=gid,
            fact_key=fact.fact_key,
            subject=fact.subject_name,
            object=fact.object_name,
            subject_uuid=str(deterministic_id(fact.group_id, "entity", fact.subject_name)),
            object_uuid=str(deterministic_id(fact.group_id, "entity", fact.object_name)),
            predicate=fact.predicate.upper(),
            fact=fact.fact_text,
            created_at=utc_now().isoformat(),
            episodes=list(fact.supporting_episode_ids),
            valid_at=_iso(fact.valid_at),
            invalid_at=_iso(fact.invalid_at),
            authority=fact.authority,
            confidence=fact.confidence,
            supporting=list(fact.supporting_episode_ids),
        )

    async def remove(self, *, group_id: str, fact_key: str) -> None:
        await self._client.driver.execute_query(
            _DELETE_EDGE, gid=_graphiti_group(group_id), fact_key=fact_key
        )

    async def projected_fact_keys(self, *, group_id: str) -> set[str]:
        result = await self._client.driver.execute_query(_LIST_KEYS, gid=_graphiti_group(group_id))
        keys: set[str] = set()
        for record in _records(result):
            value = record["fact_key"]
            if value is not None:
                keys.add(str(value))
        return keys

    async def clear(self, *, group_id: str) -> None:
        await self._client.driver.execute_query(_CLEAR, gid=_graphiti_group(group_id))
