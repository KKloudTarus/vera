"""Authoritative Knowledge Fabric domain model (Phase 1).

Framework-free value objects and the pure identity derivations for the fact model. A Fact is
a normalized proposition; an Assertion is a source-specific statement supporting or refuting
it; Evidence is the exact support; a Chunk is a citable piece of an artifact version; a
KnowledgeEvent is one entry in the append-only semantic ledger. See docs/adr/0001, 0002,
0004, 0005. The SQLAlchemy tables that persist these live in the adapters layer and are
mapped at the repository boundary, so these classes stay pure.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from vera.shared.types import JsonDict, empty_json

# --------------------------------------------------------------------- enums ---


class Polarity(StrEnum):
    SUPPORTS = "supports"
    REFUTES = "refutes"


class ObjectType(StrEnum):
    ENTITY = "entity"
    SCALAR = "scalar"


class FactLifecycle(StrEnum):
    PROPOSED = "proposed"
    ACTIVE = "active"
    DISPUTED = "disputed"
    SUPERSEDED = "superseded"
    RETRACTED = "retracted"
    EXPIRED = "expired"


class AssertionState(StrEnum):
    ACTIVE = "active"
    NEEDS_REVIEW = "needs_review"
    WITHDRAWN = "withdrawn"


class RelationType(StrEnum):
    SUPERSEDES = "SUPERSEDES"
    CONTRADICTS = "CONTRADICTS"
    REFINES = "REFINES"
    DUPLICATES = "DUPLICATES"
    DERIVED_FROM = "DERIVED_FROM"
    RELATED_TO = "RELATED_TO"


class KnowledgeEventType(StrEnum):
    ARTIFACT_DISCOVERED = "ARTIFACT_DISCOVERED"
    ARTIFACT_CHANGED = "ARTIFACT_CHANGED"
    ARTIFACT_REMOVED = "ARTIFACT_REMOVED"
    ASSERTION_ADDED = "ASSERTION_ADDED"
    ASSERTION_REAFFIRMED = "ASSERTION_REAFFIRMED"
    ASSERTION_WITHDRAWN = "ASSERTION_WITHDRAWN"
    EVIDENCE_ADDED = "EVIDENCE_ADDED"
    EVIDENCE_REMOVED = "EVIDENCE_REMOVED"
    FACT_ACTIVATED = "FACT_ACTIVATED"
    FACT_DISPUTED = "FACT_DISPUTED"
    FACT_SUPERSEDED = "FACT_SUPERSEDED"
    FACT_RETRACTED = "FACT_RETRACTED"
    FACT_EXPIRED = "FACT_EXPIRED"
    FACT_RESTORED = "FACT_RESTORED"
    ENTITY_MERGED = "ENTITY_MERGED"
    ENTITY_SPLIT = "ENTITY_SPLIT"
    ONTOLOGY_CHANGED = "ONTOLOGY_CHANGED"
    SNAPSHOT_CREATED = "SNAPSHOT_CREATED"
    CONTEXT_PACK_CREATED = "CONTEXT_PACK_CREATED"


# ------------------------------------------------------- identity derivation ---

_SEP = "\x1f"  # unit separator, so joined parts can never collide


def _norm_scalar(value: object) -> str:
    return str(value).strip().lower()


def canonical_qualifiers(qualifiers: JsonDict | None) -> str:
    """A stable string form of qualifiers: keys sorted and lowercased, values normalized,
    so qualifier order and whitespace never change a derived key.
    """
    if not qualifiers:
        return ""
    normalized = {str(k).strip().lower(): _norm_scalar(v) for k, v in qualifiers.items()}
    ordered = dict(sorted(normalized.items()))
    return json.dumps(ordered, separators=(",", ":"), ensure_ascii=False)


def normalize_object(
    *, object_entity_id: UUID | None = None, object_scalar: str | None = None
) -> str:
    """The object component of a fact key: the entity id for an entity object, or the
    normalized scalar for a scalar object.
    """
    if object_entity_id is not None:
        return f"entity:{object_entity_id}"
    return f"scalar:{_norm_scalar(object_scalar or '')}"


def _sha256(*parts: str) -> str:
    return hashlib.sha256(_SEP.join(parts).encode("utf-8")).hexdigest()


def fact_key(
    *,
    scope: str,
    subject_entity_id: UUID | str,
    predicate: str,
    object_entity_id: UUID | None = None,
    object_scalar: str | None = None,
    qualifiers: JsonDict | None = None,
) -> str:
    """Content-derived identity of a proposition (see ADR-0002). Identical propositions from
    different sources share a fact_key and so deduplicate into one Fact.
    """
    return _sha256(
        scope,
        str(subject_entity_id),
        predicate.upper(),
        normalize_object(object_entity_id=object_entity_id, object_scalar=object_scalar),
        canonical_qualifiers(qualifiers),
    )


def slot_key(
    *,
    scope: str,
    subject_entity_id: UUID | str,
    predicate: str,
    qualifiers: JsonDict | None = None,
) -> str:
    """Identity of a predicate slot (subject + predicate + qualifiers), ignoring the object.
    A single-valued predicate has at most one active fact per slot; a new value replaces it.
    """
    return _sha256(
        scope,
        str(subject_entity_id),
        predicate.upper(),
        canonical_qualifiers(qualifiers),
    )


def chunk_key(*, artifact_version_id: UUID | str, ordinal: int, content_hash: str) -> str:
    """Deterministic chunk identity, so re-chunking an unchanged artifact version is a no-op."""
    return _sha256(str(artifact_version_id), str(ordinal), content_hash)


# ---------------------------------------------------------------- entities  ---


@dataclass(frozen=True, slots=True)
class Chunk:
    id: UUID
    artifact_version_id: UUID
    group_id: str
    chunk_key: str
    ordinal: int
    text: str
    content_hash: str
    token_count: int
    heading_path: str | None = None
    start_offset: int | None = None
    end_offset: int | None = None
    page_number: int | None = None
    symbol_name: str | None = None
    start_line: int | None = None
    end_line: int | None = None
    parent_chunk_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class ChunkEmbedding:
    id: UUID
    group_id: str
    chunk_id: UUID
    provider: str
    model: str
    model_version: str
    dimension: int
    embedding: list[float]
    content_hash: str
    created_at: datetime
    active: bool = True


@dataclass(frozen=True, slots=True)
class ExtractionRun:
    id: UUID
    group_id: str
    artifact_version_id: UUID
    model: str
    provider: str
    prompt_version: str
    pipeline_version: JsonDict
    started_at: datetime


@dataclass(frozen=True, slots=True)
class Fact:
    id: UUID
    group_id: str
    fact_key: str
    slot_key: str
    subject_entity_id: UUID
    predicate: str
    object_type: ObjectType
    normalized_object: str
    object_entity_id: UUID | None = None
    object_scalar: str | None = None
    qualifiers: JsonDict = field(default_factory=empty_json)
    lifecycle_state: FactLifecycle = FactLifecycle.PROPOSED
    authority: float = 0.0
    confidence: float = 0.0
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    expires_at: datetime | None = None
    system_from: datetime | None = None
    system_to: datetime | None = None
    ontology_version_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class Assertion:
    id: UUID
    group_id: str
    fact_id: UUID
    polarity: Polarity
    knowledge_source_id: UUID | None = None
    artifact_id: UUID | None = None
    artifact_version_id: UUID | None = None
    extractor_confidence: float = 0.0
    source_authority: float = 0.0
    verification_state: str = "unverified"
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    observed_at: datetime | None = None
    recorded_at: datetime | None = None
    extraction_run_id: UUID | None = None
    run_key: str | None = None
    state: AssertionState = AssertionState.ACTIVE


@dataclass(frozen=True, slots=True)
class Evidence:
    id: UUID
    group_id: str
    assertion_id: UUID
    content_hash: str
    chunk_id: UUID | None = None
    artifact_version_id: UUID | None = None
    structured_record: JsonDict | None = None
    excerpt: str | None = None
    citation_uri: str | None = None
    quote_start: int | None = None
    quote_end: int | None = None
    quote_hash: str | None = None
    citation_override: str | None = None
    extraction_run_id: UUID | None = None
    source_coordinates: JsonDict = field(default_factory=empty_json)
    confidentiality: str = "internal"


@dataclass(frozen=True, slots=True)
class FactRelation:
    id: UUID
    group_id: str
    from_fact_id: UUID
    to_fact_id: UUID
    relation_type: RelationType


@dataclass(frozen=True, slots=True)
class KnowledgeEvent:
    id: UUID
    group_id: str
    event_type: KnowledgeEventType
    occurred_at: datetime
    actor: str | None = None
    source_id: str | None = None
    fact_id: UUID | None = None
    assertion_id: UUID | None = None
    artifact_id: UUID | None = None
    entity_id: UUID | None = None
    previous_state: JsonDict | None = None
    next_state: JsonDict | None = None
    reason: str | None = None
    policy_version: str | None = None
    trace_id: str | None = None
