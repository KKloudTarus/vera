"""SQLAlchemy repositories for the Knowledge Fabric model (Phase 1).

Each maps ORM rows to the pure domain objects at its boundary, so application and domain code
never sees SQLAlchemy. Idempotency is enforced in the database (unique keys / partial unique
index) so retries and rebuilds converge; see docs/adr/0002.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.fabric import (
    AssertionRow,
    ChunkRow,
    EvidenceRow,
    ExtractionRunRow,
    FactRelationRow,
    FactRow,
    KnowledgeEventRow,
)
from vera.domain.knowledge.fabric import (
    Assertion,
    AssertionState,
    Chunk,
    Evidence,
    ExtractionRun,
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
        expires_at=row.expires_at,
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
        run_key=row.run_key,
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
        quote_start=row.quote_start,
        quote_end=row.quote_end,
        quote_hash=row.quote_hash,
        citation_override=row.citation_override,
        extraction_run_id=row.extraction_run_id,
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


def _to_extraction_run(row: ExtractionRunRow) -> ExtractionRun:
    return ExtractionRun(
        id=row.id,
        group_id=row.group_id,
        artifact_version_id=row.artifact_version_id,
        model=row.model,
        provider=row.provider,
        prompt_version=row.prompt_version,
        pipeline_version=dict(row.pipeline_version),
        started_at=row.started_at,
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


class SqlAlchemyExtractionRunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, run: ExtractionRun) -> ExtractionRun:
        row = ExtractionRunRow(
            id=run.id,
            group_id=run.group_id,
            artifact_version_id=run.artifact_version_id,
            model=run.model,
            provider=run.provider,
            prompt_version=run.prompt_version,
            pipeline_version=dict(run.pipeline_version),
            started_at=run.started_at,
        )
        self._session.add(row)
        await self._session.flush()
        return _to_extraction_run(row)

    async def get(self, *, group_id: str, run_id: str) -> ExtractionRun | None:
        row = await self._session.scalar(
            select(ExtractionRunRow).where(
                ExtractionRunRow.group_id == group_id,
                ExtractionRunRow.id == UUID(run_id),
            )
        )
        return _to_extraction_run(row) if row is not None else None


class SqlAlchemyFactRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def lock_fact_key(self, *, group_id: str, fact_key: str) -> None:
        await self._session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"{group_id}:{fact_key}"},
        )

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
            expires_at=fact.expires_at,
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

    async def by_fact_key_for_update(self, *, group_id: str, fact_key: str) -> Fact | None:
        row = await self._session.scalar(
            select(FactRow)
            .where(FactRow.group_id == group_id, FactRow.fact_key == fact_key)
            .order_by(FactRow.system_from.desc())
            .limit(1)
            .with_for_update()
        )
        return _to_fact(row) if row is not None else None

    async def live_by_slot_key(self, *, group_id: str, slot_key: str) -> list[Fact]:
        rows = await self._session.scalars(
            select(FactRow).where(
                FactRow.group_id == group_id,
                FactRow.slot_key == slot_key,
                FactRow.lifecycle_state.in_(
                    (
                        FactLifecycle.PROPOSED.value,
                        FactLifecycle.ACTIVE.value,
                        FactLifecycle.DISPUTED.value,
                    )
                ),
            )
        )
        return [_to_fact(row) for row in rows]

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

    async def set_lifecycle(
        self,
        *,
        group_id: str,
        fact_id: str,
        state: FactLifecycle,
        valid_to: datetime | None = None,
    ) -> None:
        now = utc_now()
        values: dict[str, object] = {"lifecycle_state": state.value, "updated_at": now}
        if state in {
            FactLifecycle.SUPERSEDED,
            FactLifecycle.RETRACTED,
            FactLifecycle.EXPIRED,
        }:
            values["valid_to"] = func.coalesce(FactRow.valid_to, valid_to or now)
        await self._session.execute(
            update(FactRow)
            .where(FactRow.group_id == group_id, FactRow.id == UUID(fact_id))
            .values(**values)
        )

    async def set_aggregates(
        self, *, group_id: str, fact_id: str, authority: float, confidence: float
    ) -> None:
        await self._session.execute(
            update(FactRow)
            .where(FactRow.group_id == group_id, FactRow.id == UUID(fact_id))
            .values(authority=authority, confidence=confidence, updated_at=utc_now())
        )

    async def set_expiry(self, *, group_id: str, fact_id: str, expires_at: datetime | None) -> None:
        await self._session.execute(
            update(FactRow)
            .where(FactRow.group_id == group_id, FactRow.id == UUID(fact_id))
            .values(expires_at=expires_at, updated_at=utc_now())
        )


class SqlAlchemyFactExpiryRepository:
    """Privileged worker adapter for cross-scope freshness expiration."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def expire_due(self, *, at: datetime, limit: int = 1000) -> list[Fact]:
        rows = list(
            await self._session.scalars(
                select(FactRow)
                .where(
                    FactRow.lifecycle_state == FactLifecycle.ACTIVE.value,
                    FactRow.expires_at.is_not(None),
                    FactRow.expires_at <= at,
                )
                .order_by(FactRow.expires_at, FactRow.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        for row in rows:
            row.lifecycle_state = FactLifecycle.EXPIRED.value
            row.valid_to = row.valid_to or row.expires_at
            row.updated_at = at
        await self._session.flush()
        return [_to_fact(row) for row in rows]


class SqlAlchemyAssertionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, assertion: Assertion) -> Assertion:
        now = utc_now()
        insert = pg_insert(AssertionRow).values(
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
            run_key=assertion.run_key,
            state=assertion.state.value,
        )
        updates = {
            "extractor_confidence": assertion.extractor_confidence,
            "source_authority": assertion.source_authority,
            "verification_state": assertion.verification_state,
            "extraction_run_id": assertion.extraction_run_id,
            "run_key": assertion.run_key,
        }
        if assertion.artifact_version_id is None and assertion.run_key is not None:
            stmt = insert.on_conflict_do_update(
                index_elements=[AssertionRow.fact_id, AssertionRow.run_key, AssertionRow.polarity],
                index_where=AssertionRow.run_key.is_not(None),
                set_=updates,
            )
        else:
            stmt = insert.on_conflict_do_update(
                constraint="uq_assertion_source",
                set_=updates,
            )
        stmt = stmt.returning(AssertionRow.id)
        new_id = await self._session.scalar(stmt)
        row = await self._session.scalar(select(AssertionRow).where(AssertionRow.id == new_id))
        assert row is not None  # noqa: S101  present after insert-or-update
        return _to_assertion(row)

    async def create_proposal_if_absent(self, assertion: Assertion) -> tuple[Assertion, bool]:
        if assertion.artifact_version_id is not None or assertion.run_key is None:
            raise ValueError("a proposal assertion requires a run key and no artifact version")
        stmt = (
            pg_insert(AssertionRow)
            .values(
                id=assertion.id,
                group_id=assertion.group_id,
                fact_id=assertion.fact_id,
                polarity=assertion.polarity.value,
                extractor_confidence=assertion.extractor_confidence,
                source_authority=assertion.source_authority,
                verification_state=assertion.verification_state,
                observed_at=assertion.observed_at,
                recorded_at=assertion.recorded_at or utc_now(),
                run_key=assertion.run_key,
                state=assertion.state.value,
            )
            .on_conflict_do_nothing(
                index_elements=[AssertionRow.fact_id, AssertionRow.run_key, AssertionRow.polarity],
                index_where=AssertionRow.run_key.is_not(None),
            )
            .returning(AssertionRow.id)
        )
        created_id = await self._session.scalar(stmt)
        stored_id = created_id
        if stored_id is None:
            stored_id = await self._session.scalar(
                select(AssertionRow.id).where(
                    AssertionRow.fact_id == assertion.fact_id,
                    AssertionRow.run_key == assertion.run_key,
                    AssertionRow.polarity == assertion.polarity.value,
                )
            )
        assert stored_id is not None  # noqa: S101  inserted or selected after conflict
        row = await self._session.get(AssertionRow, stored_id)
        assert row is not None  # noqa: S101  selected from the same transaction
        return _to_assertion(row), created_id is not None

    async def lock_run_key(self, *, group_id: str, run_key: str) -> None:
        await self._session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"{group_id}:{run_key}"},
        )

    async def for_fact_and_run_key(
        self, *, group_id: str, fact_id: str, run_key: str
    ) -> Assertion | None:
        row = await self._session.scalar(
            select(AssertionRow).where(
                AssertionRow.group_id == group_id,
                AssertionRow.fact_id == UUID(fact_id),
                AssertionRow.run_key == run_key,
                AssertionRow.polarity == Polarity.SUPPORTS.value,
            )
        )
        return _to_assertion(row) if row is not None else None

    async def count_for_run_key(self, *, group_id: str, run_key: str) -> int:
        count = await self._session.scalar(
            select(func.count(AssertionRow.id)).where(
                AssertionRow.group_id == group_id,
                AssertionRow.run_key == run_key,
            )
        )
        return int(count or 0)

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

    async def withdraw_for_fact(self, *, group_id: str, fact_id: str) -> list[Assertion]:
        rows = list(
            await self._session.scalars(
                select(AssertionRow)
                .where(
                    AssertionRow.group_id == group_id,
                    AssertionRow.fact_id == UUID(fact_id),
                    AssertionRow.state == AssertionState.ACTIVE.value,
                )
                .with_for_update()
            )
        )
        if rows:
            await self._session.execute(
                update(AssertionRow)
                .where(AssertionRow.id.in_([row.id for row in rows]))
                .values(state=AssertionState.WITHDRAWN.value, withdrawn_at=utc_now())
            )
        return [_to_assertion(row) for row in rows]


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
                quote_start=evidence.quote_start,
                quote_end=evidence.quote_end,
                quote_hash=evidence.quote_hash,
                citation_override=evidence.citation_override,
                extraction_run_id=evidence.extraction_run_id,
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
