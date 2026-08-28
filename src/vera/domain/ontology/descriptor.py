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
    ObjectKind,
    PredicatePolicy,
    QualifierRule,
    QualifierValueType,
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
                "subject_types": list(p.subject_types),
                "object_types": list(p.object_types),
                "object_kind": p.object_kind.value,
                "qualifier_schema": {
                    rule.name: {
                        "type": rule.value_type.value,
                        "required": rule.required,
                    }
                    for rule in p.qualifier_schema
                },
                "allow_additional_qualifiers": p.allow_additional_qualifiers,
                "minimum_source_authority": p.minimum_source_authority,
                "ttl_seconds": p.ttl_seconds,
                "deprecated": p.deprecated,
                "replacement_predicate": p.replacement_predicate,
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
        raw_schema = spec.get("qualifier_schema", {})
        schema = cast("dict[str, Any]", raw_schema) if isinstance(raw_schema, dict) else {}
        policies.append(
            PredicatePolicy(
                predicate=predicate,
                cardinality=Cardinality(str(spec["cardinality"])),
                absence_semantics=AbsenceSemantics(str(spec["absence_semantics"])),
                conflict_strategy=ConflictStrategy(str(spec["conflict_strategy"])),
                subject_types=tuple(str(v) for v in spec.get("subject_types", [])),
                object_types=tuple(str(v) for v in spec.get("object_types", [])),
                object_kind=ObjectKind(str(spec.get("object_kind", ObjectKind.ANY.value))),
                qualifier_schema=tuple(
                    QualifierRule(
                        name=name,
                        value_type=QualifierValueType(
                            str(cast("dict[str, Any]", value).get("type", "string"))
                        ),
                        required=bool(cast("dict[str, Any]", value).get("required", False)),
                    )
                    for name, value in sorted(schema.items())
                    if isinstance(value, dict)
                ),
                allow_additional_qualifiers=bool(spec.get("allow_additional_qualifiers", True)),
                minimum_source_authority=float(spec.get("minimum_source_authority", 0.0)),
                ttl_seconds=(
                    int(spec["ttl_seconds"]) if spec.get("ttl_seconds") is not None else None
                ),
                deprecated=bool(spec.get("deprecated", False)),
                replacement_predicate=(
                    str(spec["replacement_predicate"])
                    if spec.get("replacement_predicate")
                    else None
                ),
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


def diff_descriptors(previous: OntologyDescriptor, current: OntologyDescriptor) -> JsonDict:
    """A structured, stable ontology-version diff suitable for APIs and audit reports."""
    previous_policies = previous.policies_as_json()
    current_policies = current.policies_as_json()
    previous_names = set(previous_policies)
    current_names = set(current_policies)
    changed = {
        name: {"from": previous_policies[name], "to": current_policies[name]}
        for name in sorted(previous_names & current_names)
        if previous_policies[name] != current_policies[name]
    }
    return {
        "from_version": previous.version,
        "to_version": current.version,
        "name_changed": previous.name != current.name,
        "entity_types_added": sorted(set(current.entity_types) - set(previous.entity_types)),
        "entity_types_removed": sorted(set(previous.entity_types) - set(current.entity_types)),
        "edge_types_added": sorted(set(current.edge_types) - set(previous.edge_types)),
        "edge_types_removed": sorted(set(previous.edge_types) - set(current.edge_types)),
        "predicates_added": sorted(current_names - previous_names),
        "predicates_removed": sorted(previous_names - current_names),
        "predicate_policies_changed": changed,
    }
