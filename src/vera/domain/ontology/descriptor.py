"""The active ontology as a single versioned descriptor.

The descriptor carries the identity (version, name), the type maps, and the per-predicate
governance policies together, so the code registry and the persisted ``ontology_versions``
row can be compared field by field. ``detect_drift`` makes a mismatch fail fast at startup
instead of the code and the database silently disagreeing about how facts are governed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID

from vera.domain.ontology.policy import (
    AbsenceSemantics,
    Cardinality,
    ConflictStrategy,
    PredicatePolicy,
    policy_for,
)
from vera.domain.ontology.registry import (
    ONTOLOGY_NAME,
    ONTOLOGY_VERSION,
    SINGLE_VALUED_PREDICATES,
    edge_type_names,
    entity_type_names,
)
from vera.shared.types import JsonDict


@dataclass(frozen=True, slots=True)
class OntologyDescriptor:
    version: int
    name: str
    entity_types: tuple[str, ...]
    edge_types: tuple[str, ...]
    predicate_policies: tuple[PredicatePolicy, ...]
    id: UUID | None = None

    def policies_as_json(self) -> JsonDict:
        return {
            p.predicate: {
                "cardinality": p.cardinality.value,
                "absence_semantics": p.absence_semantics.value,
                "conflict_strategy": p.conflict_strategy.value,
            }
            for p in self.predicate_policies
        }


def governed_predicates() -> tuple[str, ...]:
    """Every predicate the reconciliation policy governs: the edge types plus any
    single-valued predicate (such as HAS_STATUS) that is not itself an edge type.
    """
    names = {n.upper() for n in edge_type_names()} | {p.upper() for p in SINGLE_VALUED_PREDICATES}
    return tuple(sorted(names))


def current_descriptor() -> OntologyDescriptor:
    """The descriptor the running code implements, before it is reconciled with the DB."""
    return OntologyDescriptor(
        version=ONTOLOGY_VERSION,
        name=ONTOLOGY_NAME,
        entity_types=tuple(entity_type_names()),
        edge_types=tuple(edge_type_names()),
        predicate_policies=tuple(policy_for(p) for p in governed_predicates()),
    )


def _policies_from_json(raw: JsonDict) -> tuple[PredicatePolicy, ...]:
    policies: list[PredicatePolicy] = []
    for predicate in sorted(raw):
        raw_spec = raw[predicate]
        if not isinstance(raw_spec, dict):  # pragma: no cover - defensive against bad rows
            continue
        spec = cast("dict[str, Any]", raw_spec)
        policies.append(
            PredicatePolicy(
                predicate=predicate,
                cardinality=Cardinality(str(spec["cardinality"])),
                absence_semantics=AbsenceSemantics(str(spec["absence_semantics"])),
                conflict_strategy=ConflictStrategy(str(spec["conflict_strategy"])),
            )
        )
    return tuple(policies)


def descriptor_from_row(
    *,
    id: UUID,
    version: int,
    name: str,
    entity_types: list[str],
    edge_types: list[str],
    predicate_policies: JsonDict,
) -> OntologyDescriptor:
    return OntologyDescriptor(
        id=id,
        version=version,
        name=name,
        entity_types=tuple(entity_types),
        edge_types=tuple(edge_types),
        predicate_policies=_policies_from_json(predicate_policies),
    )


def detect_drift(code: OntologyDescriptor, persisted: OntologyDescriptor) -> list[str]:
    """Field-by-field differences between the code registry and the persisted row for the
    same version number. An empty list means the two agree.
    """
    diffs: list[str] = []
    if code.name != persisted.name:
        diffs.append(f"name: code={code.name!r} db={persisted.name!r}")
    if set(code.entity_types) != set(persisted.entity_types):
        diffs.append(
            f"entity_types: +{sorted(set(code.entity_types) - set(persisted.entity_types))} "
            f"-{sorted(set(persisted.entity_types) - set(code.entity_types))}"
        )
    if set(code.edge_types) != set(persisted.edge_types):
        diffs.append(
            f"edge_types: +{sorted(set(code.edge_types) - set(persisted.edge_types))} "
            f"-{sorted(set(persisted.edge_types) - set(code.edge_types))}"
        )
    if code.policies_as_json() != persisted.policies_as_json():
        diffs.append("predicate_policies differ between code and database")
    return diffs
