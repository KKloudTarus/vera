"""SQLAlchemy repositories for the Knowledge Fabric model (Phase 1).

Each maps ORM rows to the pure domain objects at its boundary, so application and domain code
never sees SQLAlchemy. Idempotency is enforced in the database (unique keys / partial unique
index) so retries and rebuilds converge; see docs/adr/0002.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.fabric import (
    AssertionRow,
    ChunkRow,
    EvidenceRow,
    FactRelationRow,
    FactRow,
    KnowledgeEventRow,
)
from vera.domain.knowledge.fabric import (
    Assertion,
    AssertionState,
    Chunk,
    Evidence,
    Fact,
    FactLifecycle,
    FactRelation,
    KnowledgeEvent,
    KnowledgeEventType,
    ObjectType,
    Polarity,
    RelationType,
)
from vera.shared.time import utc_now

# --------------------------------------------------------------- row mapping ---


def _to_chunk(row: ChunkRow) -> Chunk:
    return Chunk(
        id=row.id,
        artifact_version_id=row.artifact_version_id,
        group_id=row.group_id,
        chunk_key=row.chunk_key,
        ordinal=row.ordinal,
        text=row.text,
        content_hash=row.content_hash,
        token_count=row.token_count,
        heading_path=row.heading_path,
        start_offset=row.start_offset,
        end_offset=row.end_offset,
        page_number=row.page_number,
        symbol_name=row.symbol_name,
        start_line=row.start_line,
        end_line=row.end_line,
        parent_chunk_id=row.parent_chunk_id,
    )


def _to_fact(row: FactRow) -> Fact:
    return Fact(
        id=row.id,
        group_id=row.group_id,
        fact_key=row.fact_key,
        slot_key=row.slot_key,
        subject_entity_id=row.subject_entity_id,
        predicate=row.predicate,
        object_type=ObjectType(row.object_type),
        normalized_object=row.normalized_object,
        object_entity_id=row.object_entity_id,
        object_scalar=row.object_scalar,
        qualifiers=dict(row.qualifiers),
        lifecycle_state=FactLifecycle(row.lifecycle_state),
        authority=row.authority,
        confidence=row.confidence,
        valid_from=row.valid_from,
        valid_to=row.valid_to,
        system_from=row.system_from,
        system_to=row.system_to,
        ontology_version_id=row.ontology_version_id,
    )


def _to_assertion(row: AssertionRow) -> Assertion:
    return Assertion(
        id=row.id,
        group_id=row.group_id,
        fact_id=row.fact_id,
        polarity=Polarity(row.polarity),
        knowledge_source_id=row.knowledge_source_id,
        artifact_id=row.artifact_id,
        artifact_version_id=row.artifact_version_id,
        extractor_confidence=row.extractor_confidence,
        source_authority=row.source_authority,
        verification_state=row.verification_state,
        valid_from=row.valid_from,
        valid_to=row.valid_to,
        observed_at=row.observed_at,
        recorded_at=row.recorded_at,
        extraction_run_id=row.extraction_run_id,
        state=AssertionState(row.state),
    )


def _to_evidence(row: EvidenceRow) -> Evidence:
    return Evidence(
        id=row.id,
        group_id=row.group_id,
        assertion_id=row.assertion_id,
        content_hash=row.content_hash,
        chunk_id=row.chunk_id,
        artifact_version_id=row.artifact_version_id,
        structured_record=dict(row.structured_record) if row.structured_record else None,
        excerpt=row.excerpt,
        citation_uri=row.citation_uri,
        source_coordinates=dict(row.source_coordinates),
        confidentiality=row.confidentiality,
    )


def _to_relation(row: FactRelationRow) -> FactRelation:
    return FactRelation(
        id=row.id,
        group_id=row.group_id,
        from_fact_id=row.from_fact_id,
        to_fact_id=row.to_fact_id,
        relation_type=RelationType(row.relation_type),
    )


def _to_event(row: KnowledgeEventRow) -> KnowledgeEvent:
    return KnowledgeEvent(
        id=row.id,
        group_id=row.group_id,
        event_type=KnowledgeEventType(row.event_type),
        occurred_at=row.occurred_at,
        actor=row.actor,
        source_id=row.source_id,
        fact_id=row.fact_id,
        assertion_id=row.assertion_id,
        artifact_id=row.artifact_id,
        entity_id=row.entity_id,
        previous_state=dict(row.previous_state) if row.previous_state else None,
        next_state=dict(row.next_state) if row.next_state else None,
        reason=row.reason,
        policy_version=row.policy_version,
        trace_id=row.trace_id,
    )


# --------------------------------------------------------------- repositories ---


class SqlAlchemyChunkRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, chunk: Chunk) -> Chunk:
        stmt = (
            pg_insert(ChunkRow)
            .values(
                id=chunk.id,
                artifact_version_id=chunk.artifact_version_id,
                group_id=chunk.group_id,
                chunk_key=chunk.chunk_key,
                ordinal=chunk.ordinal,
                text=chunk.text,
                content_hash=chunk.content_hash,
                token_count=chunk.token_count,
                heading_path=chunk.heading_path,
                start_offset=chunk.start_offset,
                end_offset=chunk.end_offset,
                page_number=chunk.page_number,
                symbol_name=chunk.symbol_name,
                start_line=chunk.start_line,
                end_line=chunk.end_line,
                parent_chunk_id=chunk.parent_chunk_id,
            )
            .on_conflict_do_nothing(constraint="uq_chunk_key")
        )
        await self._session.execute(stmt)
        row = await self._session.scalar(
            select(ChunkRow).where(
                ChunkRow.group_id == chunk.group_id, ChunkRow.chunk_key == chunk.chunk_key
            )
        )
        assert row is not None  # noqa: S101  present after insert-or-existing
        return _to_chunk(row)

    async def by_artifact_version(self, *, group_id: str, artifact_version_id: str) -> list[Chunk]:
        rows = await self._session.scalars(
            select(ChunkRow)
            .where(
                ChunkRow.group_id == group_id,
                ChunkRow.artifact_version_id == UUID(artifact_version_id),
            )
            .order_by(ChunkRow.ordinal)
        )
        return [_to_chunk(r) for r in rows]

    async def get(self, *, group_id: str, chunk_id: str) -> Chunk | None:
        row = await self._session.scalar(
            select(ChunkRow).where(ChunkRow.group_id == group_id, ChunkRow.id == UUID(chunk_id))
        )
        return _to_chunk(row) if row is not None else None


class SqlAlchemyFactRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, fact: Fact) -> Fact:
        existing = await self.active_by_fact_key(group_id=fact.group_id, fact_key=fact.fact_key)
        if existing is not None:
            return existing
        row = FactRow(
            id=fact.id,
            group_id=fact.group_id,
            fact_key=fact.fact_key,
            slot_key=fact.slot_key,
            subject_entity_id=fact.subject_entity_id,
            predicate=fact.predicate,
            object_type=fact.object_type.value,
            normalized_object=fact.normalized_object,
            object_entity_id=fact.object_entity_id,
            object_scalar=fact.object_scalar,
            qualifiers=dict(fact.qualifiers),
            lifecycle_state=fact.lifecycle_state.value,
            authority=fact.authority,
            confidence=fact.confidence,
            valid_from=fact.valid_from,
            valid_to=fact.valid_to,
            system_to=fact.system_to,
            ontology_version_id=fact.ontology_version_id,
        )
        self._session.add(row)
        await self._session.flush()
        return _to_fact(row)

    async def active_by_fact_key(self, *, group_id: str, fact_key: str) -> Fact | None:
        row = await self._session.scalar(
            select(FactRow).where(
                FactRow.group_id == group_id,
                FactRow.fact_key == fact_key,
                FactRow.lifecycle_state == FactLifecycle.ACTIVE.value,
            )
        )
        return _to_fact(row) if row is not None else None

    async def by_fact_key(self, *, group_id: str, fact_key: str) -> Fact | None:
        row = await self._session.scalar(
            select(FactRow)
            .where(FactRow.group_id == group_id, FactRow.fact_key == fact_key)
            .order_by(FactRow.system_from.desc())
            .limit(1)
        )
        return _to_fact(row) if row is not None else None

    async def active_by_slot_key(self, *, group_id: str, slot_key: str) -> list[Fact]:
        rows = await self._session.scalars(
            select(FactRow).where(
                FactRow.group_id == group_id,
                FactRow.slot_key == slot_key,
                FactRow.lifecycle_state == FactLifecycle.ACTIVE.value,
            )
        )
        return [_to_fact(r) for r in rows]

    async def get(self, *, group_id: str, fact_id: str) -> Fact | None:
        row = await self._session.scalar(
            select(FactRow).where(FactRow.group_id == group_id, FactRow.id == UUID(fact_id))
        )
        return _to_fact(row) if row is not None else None

    async def set_lifecycle(self, *, group_id: str, fact_id: str, state: FactLifecycle) -> None:
        await self._session.execute(
            update(FactRow)
            .where(FactRow.group_id == group_id, FactRow.id == UUID(fact_id))
            .values(lifecycle_state=state.value, updated_at=utc_now())
        )

    async def set_aggregates(
        self, *, group_id: str, fact_id: str, authority: float, confidence: float
    ) -> None:
        await self._session.execute(
            update(FactRow)
            .where(FactRow.group_id == group_id, FactRow.id == UUID(fact_id))
            .values(authority=authority, confidence=confidence, updated_at=utc_now())
        )


class SqlAlchemyAssertionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, assertion: Assertion) -> Assertion:
        now = utc_now()
        stmt = (
            pg_insert(AssertionRow)
            .values(
                id=assertion.id,
                group_id=assertion.group_id,
                fact_id=assertion.fact_id,
                polarity=assertion.polarity.value,
                knowledge_source_id=assertion.knowledge_source_id,
                artifact_id=assertion.artifact_id,
                artifact_version_id=assertion.artifact_version_id,
                extractor_confidence=assertion.extractor_confidence,
                source_authority=assertion.source_authority,
                verification_state=assertion.verification_state,
                valid_from=assertion.valid_from,
                valid_to=assertion.valid_to,
                observed_at=assertion.observed_at,
                recorded_at=assertion.recorded_at or now,
                extraction_run_id=assertion.extraction_run_id,
                state=AssertionState.ACTIVE.value,
            )
            .on_conflict_do_update(
                constraint="uq_assertion_source",
                set_={
                    "recorded_at": now,
                    "state": AssertionState.ACTIVE.value,
                    "withdrawn_at": None,
                    "extractor_confidence": assertion.extractor_confidence,
                    "source_authority": assertion.source_authority,
                    "verification_state": assertion.verification_state,
                },
            )
            .returning(AssertionRow.id)
        )
        new_id = await self._session.scalar(stmt)
        row = await self._session.scalar(select(AssertionRow).where(AssertionRow.id == new_id))
        assert row is not None  # noqa: S101  present after insert-or-update
        return _to_assertion(row)

    async def active_for_fact(self, *, group_id: str, fact_id: str) -> list[Assertion]:
        rows = await self._session.scalars(
            select(AssertionRow).where(
                AssertionRow.group_id == group_id,
                AssertionRow.fact_id == UUID(fact_id),
                AssertionRow.state == AssertionState.ACTIVE.value,
            )
        )
        return [_to_assertion(r) for r in rows]

    async def active_for_artifact(self, *, group_id: str, artifact_id: str) -> list[Assertion]:
        rows = await self._session.scalars(
            select(AssertionRow).where(
                AssertionRow.group_id == group_id,
                AssertionRow.artifact_id == UUID(artifact_id),
                AssertionRow.state == AssertionState.ACTIVE.value,
            )
        )
        return [_to_assertion(r) for r in rows]

    async def withdraw(self, *, group_id: str, assertion_id: str) -> None:
        await self._session.execute(
            update(AssertionRow)
            .where(AssertionRow.group_id == group_id, AssertionRow.id == UUID(assertion_id))
            .values(state=AssertionState.WITHDRAWN.value, withdrawn_at=utc_now())
        )


class SqlAlchemyEvidenceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, evidence: Evidence) -> Evidence:
        stmt = (
            pg_insert(EvidenceRow)
            .values(
                id=evidence.id,
                group_id=evidence.group_id,
                assertion_id=evidence.assertion_id,
                chunk_id=evidence.chunk_id,
                artifact_version_id=evidence.artifact_version_id,
                structured_record=evidence.structured_record,
                excerpt=evidence.excerpt,
                citation_uri=evidence.citation_uri,
                content_hash=evidence.content_hash,
                source_coordinates=dict(evidence.source_coordinates),
                confidentiality=evidence.confidentiality,
            )
            .on_conflict_do_nothing(constraint="uq_evidence_hash")
        )
        await self._session.execute(stmt)
        row = await self._session.scalar(
            select(EvidenceRow).where(
                EvidenceRow.assertion_id == evidence.assertion_id,
                EvidenceRow.content_hash == evidence.content_hash,
            )
        )
        assert row is not None  # noqa: S101  present after insert-or-existing
        return _to_evidence(row)

    async def for_assertion(self, *, group_id: str, assertion_id: str) -> list[Evidence]:
        rows = await self._session.scalars(
            select(EvidenceRow).where(
                EvidenceRow.group_id == group_id,
                EvidenceRow.assertion_id == UUID(assertion_id),
            )
        )
        return [_to_evidence(r) for r in rows]


class SqlAlchemyFactRelationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, relation: FactRelation) -> FactRelation:
        stmt = (
            pg_insert(FactRelationRow)
            .values(
                id=relation.id,
                group_id=relation.group_id,
                from_fact_id=relation.from_fact_id,
                to_fact_id=relation.to_fact_id,
                relation_type=relation.relation_type.value,
            )
            .on_conflict_do_nothing(constraint="uq_fact_relation")
        )
        await self._session.execute(stmt)
        row = await self._session.scalar(
            select(FactRelationRow).where(
                FactRelationRow.from_fact_id == relation.from_fact_id,
                FactRelationRow.to_fact_id == relation.to_fact_id,
                FactRelationRow.relation_type == relation.relation_type.value,
            )
        )
        assert row is not None  # noqa: S101  present after insert-or-existing
        return _to_relation(row)

    async def from_fact(self, *, group_id: str, fact_id: str) -> list[FactRelation]:
        rows = await self._session.scalars(
            select(FactRelationRow).where(
                FactRelationRow.group_id == group_id,
                FactRelationRow.from_fact_id == UUID(fact_id),
            )
        )
        return [_to_relation(r) for r in rows]


class SqlAlchemyKnowledgeEventLog:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(self, event: KnowledgeEvent) -> KnowledgeEvent:
        row = KnowledgeEventRow(
            id=event.id,
            occurred_at=event.occurred_at,
            group_id=event.group_id,
            event_type=event.event_type.value,
            actor=event.actor,
            source_id=event.source_id,
            fact_id=event.fact_id,
            assertion_id=event.assertion_id,
            artifact_id=event.artifact_id,
            entity_id=event.entity_id,
            previous_state=event.previous_state,
            next_state=event.next_state,
            reason=event.reason,
            policy_version=event.policy_version,
            trace_id=event.trace_id,
        )
        self._session.add(row)
        await self._session.flush()
        return _to_event(row)

    async def recent(self, *, group_id: str, limit: int = 100) -> list[KnowledgeEvent]:
        rows = await self._session.scalars(
            select(KnowledgeEventRow)
            .where(KnowledgeEventRow.group_id == group_id)
            .order_by(KnowledgeEventRow.occurred_at.desc())
            .limit(limit)
        )
        return [_to_event(r) for r in rows]
