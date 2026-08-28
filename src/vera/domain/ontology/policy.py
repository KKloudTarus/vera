"""Predicate policy that drives reconciliation (Phase 2).

A lightweight, typed layer over the existing predicate classification: it says whether a
predicate is single-valued per qualifier set, what happens to a fact when its last support
disappears, and how contradictions are resolved. Phase 6 turns this into a fully versioned,
source-aware governance registry; for now it derives sensible defaults from the current
``SINGLE_VALUED_PREDICATES`` set so reconciliation has explicit, testable rules to follow.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from vera.domain.ontology.registry import is_single_valued


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


@dataclass(frozen=True, slots=True)
class PredicatePolicy:
    predicate: str
    cardinality: Cardinality
    absence_semantics: AbsenceSemantics
    conflict_strategy: ConflictStrategy


def policy_for(predicate: str) -> PredicatePolicy:
    """The reconciliation policy for a predicate. Single-valued predicates replace within a
    qualifier slot and retract when their last support is gone; multi-valued predicates let
    values coexist and retract each fact independently when its own support disappears.
    """
    if is_single_valued(predicate):
        return PredicatePolicy(
            predicate=predicate.upper(),
            cardinality=Cardinality.SINGLE_PER_QUALIFIER_SET,
            absence_semantics=AbsenceSemantics.RETRACT,
            conflict_strategy=ConflictStrategy.HIGHER_AUTHORITY_THEN_REVIEW,
        )
    return PredicatePolicy(
        predicate=predicate.upper(),
        cardinality=Cardinality.MULTI,
        absence_semantics=AbsenceSemantics.RETRACT,
        conflict_strategy=ConflictStrategy.HIGHER_AUTHORITY_THEN_REVIEW,
    )
