"""Deterministic identity derivation for the Knowledge Fabric (docs/adr/0002)."""

from __future__ import annotations

from uuid import UUID

from vera.domain.knowledge.fabric import (
    canonical_qualifiers,
    chunk_key,
    fact_key,
    normalize_object,
    slot_key,
)

_SUBJECT = UUID("11111111-1111-1111-1111-111111111111")
_OBJECT = UUID("22222222-2222-2222-2222-222222222222")


def test_fact_key_is_deterministic() -> None:
    a = fact_key(
        scope="g", subject_entity_id=_SUBJECT, predicate="RUNS_ON", object_scalar="prod-eks"
    )
    b = fact_key(
        scope="g", subject_entity_id=_SUBJECT, predicate="RUNS_ON", object_scalar="prod-eks"
    )
    assert a == b


def test_fact_key_is_case_and_whitespace_insensitive_for_scalars() -> None:
    a = fact_key(
        scope="g", subject_entity_id=_SUBJECT, predicate="RUNS_ON", object_scalar="Prod-EKS"
    )
    b = fact_key(
        scope="g", subject_entity_id=_SUBJECT, predicate="runs_on", object_scalar="  prod-eks "
    )
    assert a == b


def test_fact_key_qualifier_order_does_not_matter() -> None:
    a = fact_key(
        scope="g",
        subject_entity_id=_SUBJECT,
        predicate="RUNS_ON",
        object_scalar="eks",
        qualifiers={"environment": "prod", "region": "eu"},
    )
    b = fact_key(
        scope="g",
        subject_entity_id=_SUBJECT,
        predicate="RUNS_ON",
        object_scalar="eks",
        qualifiers={"region": "eu", "environment": "prod"},
    )
    assert a == b


def test_different_objects_yield_different_fact_keys() -> None:
    a = fact_key(scope="g", subject_entity_id=_SUBJECT, predicate="RUNS_ON", object_scalar="eks")
    b = fact_key(scope="g", subject_entity_id=_SUBJECT, predicate="RUNS_ON", object_scalar="ecs")
    assert a != b


def test_slot_key_ignores_object_but_not_qualifiers() -> None:
    # Same slot regardless of object value: single-valued replacement is detected here.
    eks = fact_key(
        scope="g",
        subject_entity_id=_SUBJECT,
        predicate="RUNS_ON",
        object_scalar="eks",
        qualifiers={"environment": "prod"},
    )
    ecs = fact_key(
        scope="g",
        subject_entity_id=_SUBJECT,
        predicate="RUNS_ON",
        object_scalar="ecs",
        qualifiers={"environment": "prod"},
    )
    assert eks != ecs
    slot_eks = slot_key(
        scope="g",
        subject_entity_id=_SUBJECT,
        predicate="RUNS_ON",
        qualifiers={"environment": "prod"},
    )
    slot_ecs = slot_key(
        scope="g",
        subject_entity_id=_SUBJECT,
        predicate="RUNS_ON",
        qualifiers={"environment": "prod"},
    )
    assert slot_eks == slot_ecs  # same slot -> ecs replaces eks under this qualifier set


def test_qualifiers_prevent_false_contradiction() -> None:
    # prod and dev are different slots, so RUNS_ON EKS [prod] and RUNS_ON ECS [dev] never clash.
    prod = slot_key(
        scope="g",
        subject_entity_id=_SUBJECT,
        predicate="RUNS_ON",
        qualifiers={"environment": "prod"},
    )
    dev = slot_key(
        scope="g",
        subject_entity_id=_SUBJECT,
        predicate="RUNS_ON",
        qualifiers={"environment": "dev"},
    )
    assert prod != dev


def test_scope_isolates_keys() -> None:
    a = fact_key(
        scope="tenant-a", subject_entity_id=_SUBJECT, predicate="RUNS_ON", object_scalar="eks"
    )
    b = fact_key(
        scope="tenant-b", subject_entity_id=_SUBJECT, predicate="RUNS_ON", object_scalar="eks"
    )
    assert a != b


def test_normalize_object_distinguishes_entity_from_scalar() -> None:
    entity = normalize_object(object_entity_id=_OBJECT)
    scalar = normalize_object(object_scalar=str(_OBJECT))
    assert entity.startswith("entity:")
    assert scalar.startswith("scalar:")
    assert entity != scalar


def test_entity_object_fact_key_uses_entity_id() -> None:
    a = fact_key(
        scope="g", subject_entity_id=_SUBJECT, predicate="DEPENDS_ON", object_entity_id=_OBJECT
    )
    b = fact_key(
        scope="g", subject_entity_id=_SUBJECT, predicate="DEPENDS_ON", object_entity_id=_OBJECT
    )
    assert a == b
    other = fact_key(
        scope="g", subject_entity_id=_SUBJECT, predicate="DEPENDS_ON", object_entity_id=_SUBJECT
    )
    assert a != other


def test_chunk_key_is_deterministic() -> None:
    version = UUID("33333333-3333-3333-3333-333333333333")
    a = chunk_key(artifact_version_id=version, ordinal=3, content_hash="abc")
    b = chunk_key(artifact_version_id=version, ordinal=3, content_hash="abc")
    assert a == b
    assert chunk_key(artifact_version_id=version, ordinal=4, content_hash="abc") != a


def test_canonical_qualifiers_empty_is_stable() -> None:
    assert canonical_qualifiers(None) == canonical_qualifiers({}) == ""
