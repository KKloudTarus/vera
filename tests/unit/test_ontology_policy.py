"""Predicate policy that drives Phase 2 reconciliation."""

from __future__ import annotations

from vera.domain.ontology.policy import AbsenceSemantics, Cardinality, policy_for


def test_single_valued_predicate_is_one_per_qualifier_set() -> None:
    p = policy_for("RUNS_ON")
    assert p.cardinality is Cardinality.SINGLE_PER_QUALIFIER_SET
    assert p.absence_semantics is AbsenceSemantics.RETRACT


def test_multi_valued_predicate_lets_values_coexist() -> None:
    assert policy_for("DEPENDS_ON").cardinality is Cardinality.MULTI


def test_policy_normalizes_predicate_case() -> None:
    assert policy_for("runs_on").predicate == "RUNS_ON"
