"""The active ontology registry: the type maps passed to the graph extractor.

``ONTOLOGY_VERSION`` bumps whenever the entity/edge types or the edge-type map change,
so published episodes can record which ontology produced them and a reprocess can
rebuild the graph under a new one.
"""

from __future__ import annotations

from pydantic import BaseModel

from vera.domain.ontology.types import (
    Caused,
    Component,
    Datastore,
    DecidedBy,
    Decision,
    DependsOn,
    DeployedTo,
    Environment,
    Incident,
    MemberOf,
    Owns,
    Person,
    Repository,
    RunsOn,
    Service,
    Team,
)

ONTOLOGY_NAME = "vera-core"
ONTOLOGY_VERSION = 1

ENTITY_TYPES: dict[str, type[BaseModel]] = {
    "Service": Service,
    "Environment": Environment,
    "Team": Team,
    "Person": Person,
    "Repository": Repository,
    "Datastore": Datastore,
    "Component": Component,
    "Incident": Incident,
    "Decision": Decision,
}

EDGE_TYPES: dict[str, type[BaseModel]] = {
    "RUNS_ON": RunsOn,
    "DEPENDS_ON": DependsOn,
    "OWNS": Owns,
    "DEPLOYED_TO": DeployedTo,
    "MEMBER_OF": MemberOf,
    "CAUSED": Caused,
    "DECIDED_BY": DecidedBy,
}

# Which edge types may connect which entity pairs. ("Entity", "Entity") is the catch-all
# Graphiti uses when a more specific pair is not listed.
EDGE_TYPE_MAP: dict[tuple[str, str], list[str]] = {
    ("Service", "Environment"): ["RUNS_ON", "DEPLOYED_TO"],
    ("Service", "Service"): ["DEPENDS_ON"],
    ("Service", "Component"): ["DEPENDS_ON"],
    ("Service", "Datastore"): ["DEPENDS_ON"],
    ("Team", "Service"): ["OWNS"],
    ("Team", "Repository"): ["OWNS"],
    ("Person", "Team"): ["MEMBER_OF"],
    ("Service", "Incident"): ["CAUSED"],
    ("Team", "Decision"): ["DECIDED_BY"],
    ("Person", "Decision"): ["DECIDED_BY"],
    ("Entity", "Entity"): list(EDGE_TYPES.keys()),
}


# Functional (single-valued) predicates: a subject has at most one current object, so a
# new, contradicting value supersedes the old one. Multi-valued predicates (DEPENDS_ON,
# OWNS, MEMBER_OF) can hold several objects at once, so a different value is not a
# contradiction and both are kept.
SINGLE_VALUED_PREDICATES: frozenset[str] = frozenset({"RUNS_ON", "DEPLOYED_TO", "HAS_STATUS"})


def is_single_valued(predicate: str) -> bool:
    return predicate.upper() in SINGLE_VALUED_PREDICATES


def entity_type_names() -> list[str]:
    return list(ENTITY_TYPES.keys())


def edge_type_names() -> list[str]:
    return list(EDGE_TYPES.keys())
