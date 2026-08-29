"""Versioned predicate governance used by extraction and reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from vera.domain.ontology.registry import EDGE_TYPE_MAP, is_edge_predicate, is_single_valued
from vera.shared.types import JsonDict


class Cardinality(StrEnum):
    SINGLE_PER_QUALIFIER_SET = "one_per_qualifier_set"
    MULTI = "multi"


class AbsenceSemantics(StrEnum):
    """What to do with a fact when its final active supporting assertion disappears."""

    RETRACT = "retract"
    EXPIRE = "expire"
    REVIEW = "review"
    KEEP = "keep"


class ConflictStrategy(StrEnum):
    HIGHER_AUTHORITY_THEN_REVIEW = "higher_authority_then_review"
    NEWEST_WINS = "newest_wins"
    ALWAYS_REVIEW = "always_review"


class ObjectKind(StrEnum):
    ANY = "any"
    ENTITY = "entity"
    SCALAR = "scalar"


class QualifierValueType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"


@dataclass(frozen=True, slots=True)
class QualifierRule:
    name: str
    value_type: QualifierValueType
    required: bool = False


@dataclass(frozen=True, slots=True)
class PredicatePolicy:
    predicate: str
    cardinality: Cardinality
    absence_semantics: AbsenceSemantics
    conflict_strategy: ConflictStrategy
    subject_types: tuple[str, ...] = ()
    object_types: tuple[str, ...] = ()
    object_kind: ObjectKind = ObjectKind.ANY
    qualifier_schema: tuple[QualifierRule, ...] = ()
    allow_additional_qualifiers: bool = True
    minimum_source_authority: float = 0.0
    ttl_seconds: int | None = None
    deprecated: bool = False
    replacement_predicate: str | None = None


def _entity_constraints(predicate: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    pairs = [
        (subject, obj)
        for (subject, obj), predicates in EDGE_TYPE_MAP.items()
        if predicate in predicates and (subject, obj) != ("Entity", "Entity")
    ]
    return (
        tuple(sorted({subject for subject, _ in pairs})),
        tuple(sorted({obj for _, obj in pairs})),
    )


def policy_for(predicate: str) -> PredicatePolicy:
    """The reconciliation policy for a predicate. Single-valued predicates replace within a
    qualifier slot and retract when their last support is gone; multi-valued predicates let
    values coexist and retract each fact independently when its own support disappears.
    """
    normalized = predicate.upper()
    subjects, objects = _entity_constraints(normalized)
    qualifiers: tuple[QualifierRule, ...] = ()
    ttl_seconds: int | None = None
    if normalized == "RUNS_ON":
        qualifiers = (QualifierRule("environment", QualifierValueType.STRING),)
    elif normalized == "HAS_STATUS":
        subjects = ("Environment", "Incident", "Service")
        qualifiers = (QualifierRule("environment", QualifierValueType.STRING, required=True),)
        ttl_seconds = 86_400
    return PredicatePolicy(
        predicate=normalized,
        cardinality=(
            Cardinality.SINGLE_PER_QUALIFIER_SET
            if is_single_valued(normalized)
            else Cardinality.MULTI
        ),
        absence_semantics=AbsenceSemantics.RETRACT,
        conflict_strategy=ConflictStrategy.HIGHER_AUTHORITY_THEN_REVIEW,
        subject_types=subjects,
        object_types=objects,
        object_kind=(
            ObjectKind.ENTITY
            if is_edge_predicate(normalized)
            else ObjectKind.SCALAR
            if normalized == "HAS_STATUS"
            else ObjectKind.ANY
        ),
        qualifier_schema=qualifiers,
        minimum_source_authority=(
            0.85
            if normalized == "DECIDED_BY"
            else 0.7
            if is_edge_predicate(normalized) or normalized == "HAS_STATUS"
            else 0.0
        ),
        ttl_seconds=ttl_seconds,
    )


def governance_violations(
    policy: PredicatePolicy,
    *,
    subject_type: str | None,
    object_type: str | None,
    qualifiers: JsonDict,
    source_authority: float,
) -> tuple[str, ...]:
    """Return deterministic reasons an assertion needs review under ``policy``.

    ``Entity``/missing types remain compatible with older persisted episodes that predate typed
    extraction. Explicit concrete types are always checked against the governed constraints.
    """
    violations: list[str] = []
    if policy.deprecated:
        replacement = (
            f"; use {policy.replacement_predicate}" if policy.replacement_predicate else ""
        )
        violations.append(f"predicate {policy.predicate} is deprecated{replacement}")
    if (
        subject_type
        and subject_type != "Entity"
        and policy.subject_types
        and subject_type not in policy.subject_types
    ):
        violations.append(f"subject type {subject_type} is not allowed for {policy.predicate}")
    if policy.object_kind is ObjectKind.SCALAR and object_type not in (None, "Scalar"):
        violations.append(f"{policy.predicate} requires a scalar object")
    elif (
        policy.object_kind is ObjectKind.ENTITY
        and object_type
        and object_type != "Entity"
        and policy.object_types
        and object_type not in policy.object_types
    ):
        violations.append(f"object type {object_type} is not allowed for {policy.predicate}")

    schema = {rule.name: rule for rule in policy.qualifier_schema}
    for rule in policy.qualifier_schema:
        if rule.required and rule.name not in qualifiers:
            violations.append(f"required qualifier {rule.name} is missing")
            continue
        if rule.name in qualifiers and not _qualifier_matches(
            qualifiers[rule.name], rule.value_type
        ):
            violations.append(f"qualifier {rule.name} must be {rule.value_type.value}")
    if not policy.allow_additional_qualifiers:
        for name in sorted(set(qualifiers) - set(schema)):
            violations.append(f"qualifier {name} is not allowed")
    if source_authority < policy.minimum_source_authority:
        violations.append(
            f"source authority {source_authority:g} is below {policy.minimum_source_authority:g}"
        )
    return tuple(violations)


def _qualifier_matches(value: object, expected: QualifierValueType) -> bool:
    if expected is QualifierValueType.STRING:
        return isinstance(value, str)
    if expected is QualifierValueType.BOOLEAN:
        return isinstance(value, bool)
    if expected is QualifierValueType.INTEGER:
        return isinstance(value, int) and not isinstance(value, bool)
    return isinstance(value, (int, float)) and not isinstance(value, bool)
