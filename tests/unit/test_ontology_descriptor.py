"""The code ontology descriptor and its drift detection against a persisted row."""

from __future__ import annotations

from vera.domain.ontology import (
    OntologyDescriptor,
    current_descriptor,
    descriptor_from_row,
    detect_drift,
    governed_predicates,
)
from vera.domain.ontology.policy import AbsenceSemantics, Cardinality, ConflictStrategy


def test_current_descriptor_governs_edge_types_and_single_valued_predicates() -> None:
    d = current_descriptor()
    preds = {p.predicate for p in d.predicate_policies}
    assert preds == set(governed_predicates())
    # HAS_STATUS is single-valued but not an edge type; it must still be governed.
    assert "HAS_STATUS" in preds
    by_name = {p.predicate: p for p in d.predicate_policies}
    assert by_name["RUNS_ON"].cardinality is Cardinality.SINGLE_PER_QUALIFIER_SET
    assert by_name["DEPENDS_ON"].cardinality is Cardinality.MULTI
    assert by_name["RUNS_ON"].absence_semantics is AbsenceSemantics.RETRACT
    assert by_name["RUNS_ON"].conflict_strategy is ConflictStrategy.HIGHER_AUTHORITY_THEN_REVIEW


def test_descriptor_round_trips_through_json() -> None:
    code = current_descriptor()
    rebuilt = descriptor_from_row(
        id=code.id or __import__("uuid").UUID(int=1),
        version=code.version,
        name=code.name,
        entity_types=list(code.entity_types),
        edge_types=list(code.edge_types),
        predicate_policies=code.policies_as_json(),
    )
    assert detect_drift(code, rebuilt) == []


def test_detect_drift_flags_each_divergence() -> None:
    code = current_descriptor()

    renamed = OntologyDescriptor(
        version=code.version,
        name="other",
        entity_types=code.entity_types,
        edge_types=code.edge_types,
        predicate_policies=code.predicate_policies,
    )
    assert any("name" in d for d in detect_drift(code, renamed))

    fewer_entities = OntologyDescriptor(
        version=code.version,
        name=code.name,
        entity_types=code.entity_types[:-1],
        edge_types=code.edge_types,
        predicate_policies=code.predicate_policies,
    )
    assert any("entity_types" in d for d in detect_drift(code, fewer_entities))

    changed_policy = OntologyDescriptor(
        version=code.version,
        name=code.name,
        entity_types=code.entity_types,
        edge_types=code.edge_types,
        predicate_policies=code.predicate_policies[:-1],
    )
    assert any("predicate_policies" in d for d in detect_drift(code, changed_policy))
