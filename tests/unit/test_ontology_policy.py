"""Predicate policy that drives Phase 2 reconciliation."""

from __future__ import annotations

from vera.domain.ontology.policy import (
    AbsenceSemantics,
    Cardinality,
    ObjectKind,
    governance_violations,
    policy_for,
)


def test_single_valued_predicate_is_one_per_qualifier_set() -> None:
    p = policy_for("RUNS_ON")
    assert p.cardinality is Cardinality.SINGLE_PER_QUALIFIER_SET
    assert p.absence_semantics is AbsenceSemantics.RETRACT


def test_multi_valued_predicate_lets_values_coexist() -> None:
    assert policy_for("DEPENDS_ON").cardinality is Cardinality.MULTI


def test_policy_normalizes_predicate_case() -> None:
    assert policy_for("runs_on").predicate == "RUNS_ON"


def test_policy_enforces_explicit_entity_types_and_source_authority() -> None:
    policy = policy_for("RUNS_ON")
    assert policy.subject_types == ("Service",)
    assert policy.object_types == ("Environment",)
    assert policy.object_kind is ObjectKind.ENTITY
    assert governance_violations(
        policy,
        subject_type="Team",
        object_type="Repository",
        qualifiers={},
        source_authority=0.4,
    ) == (
        "subject type Team is not allowed for RUNS_ON",
        "object type Repository is not allowed for RUNS_ON",
        "source authority 0.4 is below 0.7",
    )


def test_status_policy_requires_typed_qualifier_and_has_ttl() -> None:
    policy = policy_for("HAS_STATUS")
    assert policy.ttl_seconds == 86_400
    assert governance_violations(
        policy,
        subject_type="Service",
        object_type=None,
        qualifiers={},
        source_authority=1.0,
    ) == ("required qualifier environment is missing",)
    assert governance_violations(
        policy,
        subject_type="Service",
        object_type=None,
        qualifiers={"environment": 7},
        source_authority=1.0,
    ) == ("qualifier environment must be string",)
