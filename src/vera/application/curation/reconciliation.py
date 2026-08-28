"""Deterministic artifact reconciliation (Phase 2).

Given the propositions extracted from a new artifact version, reconcile them against the
existing Fact/Assertion/Evidence store the way docs/knowledge-fabric section 5 requires:

- a repeated proposition reaffirms an Assertion and adds Evidence, it never creates a second
  Fact (fact_key dedup);
- a single-valued predicate replaces the prior value in its qualifier slot only when the new
  source outranks it, otherwise the conflict is recorded as disputed rather than silently
  overwriting higher authority;
- multi-valued predicates keep coexisting values;
- refuting propositions are recorded with refutes polarity, never as supporting edges;
- when the new version drops a proposition, that artifact's prior Assertion is withdrawn, and
  a Fact that loses its final active support transitions per the predicate's ontology policy.

Every transition appends a KnowledgeEvent. Entity resolution happens before this service:
propositions arrive with resolved subject/object entity ids. Reconciliation is scoped to one
artifact so removing this artifact's support never touches another artifact's assertions.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from vera.domain.curation.trust import TrustAction, action_for_tier
from vera.domain.knowledge.fabric import (
    Assertion,
    Evidence,
    Fact,
    FactLifecycle,
    FactRelation,
    KnowledgeEvent,
    KnowledgeEventType,
    ObjectType,
    Polarity,
    RelationType,
    fact_key,
    normalize_object,
    slot_key,
)
from vera.domain.ontology.policy import AbsenceSemantics, Cardinality, policy_for
from vera.domain.ports.fabric import (
    AssertionRepository,
    EvidenceRepository,
    FactRelationRepository,
    FactRepository,
    KnowledgeEventLog,
)
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import JsonDict, empty_json

_POLICY_VERSION = "ontology-v1"


@dataclass(frozen=True, slots=True)
class ResolvedProposition:
    """An extracted proposition with entities already resolved to canonical ids."""

    subject_entity_id: UUID
    predicate: str
    polarity: Polarity = Polarity.SUPPORTS
    object_entity_id: UUID | None = None
    object_scalar: str | None = None
    qualifiers: JsonDict = field(default_factory=empty_json)
    extractor_confidence: float = 0.0
    chunk_id: UUID | None = None
    excerpt: str | None = None
    citation_uri: str | None = None
    evidence_content_hash: str | None = None


@dataclass(frozen=True, slots=True)
class ArtifactReconciliation:
    group_id: str
    artifact_version_id: UUID
    source_authority: float
    trust_tier: int
    propositions: list[ResolvedProposition]
    knowledge_source_id: UUID | None = None
    artifact_id: UUID | None = None
    ontology_version_id: UUID | None = None
    actor: str | None = None
    trace_id: str | None = None


@dataclass(slots=True)
class ReconciliationReport:
    facts_activated: int = 0
    facts_disputed: int = 0
    facts_superseded: int = 0
    facts_retracted: int = 0
    assertions_added: int = 0
    assertions_reaffirmed: int = 0
    assertions_withdrawn: int = 0
    evidence_added: int = 0


class ReconciliationService:
    def __init__(
        self,
        *,
        facts: FactRepository,
        assertions: AssertionRepository,
        evidence: EvidenceRepository,
        relations: FactRelationRepository,
        events: KnowledgeEventLog,
    ) -> None:
        self._facts = facts
        self._assertions = assertions
        self._evidence = evidence
        self._relations = relations
        self._events = events

    async def reconcile(self, req: ArtifactReconciliation) -> ReconciliationReport:
        report = ReconciliationReport()
        now = utc_now()
        for prop in req.propositions:
            if prop.polarity is Polarity.REFUTES:
                await self._handle_refute(req, prop, now, report)
            else:
                await self._handle_support(req, prop, now, report)
        await self._withdraw_dropped(req, now, report)
        return report

    # ---------------------------------------------------------------- support ---

    async def _handle_support(
        self,
        req: ArtifactReconciliation,
        prop: ResolvedProposition,
        now: datetime,
        report: ReconciliationReport,
    ) -> None:
        fk = _fact_key(req.group_id, prop)
        existing = await self._facts.active_by_fact_key(group_id=req.group_id, fact_key=fk)
        if existing is not None:
            fact = existing  # reaffirm an already-active proposition; never a second Fact
        else:
            fact = await self._create_fact(req, prop, fk, now, report)
        await self._attach_assertion(req, fact, prop, Polarity.SUPPORTS, now, report)
        await self._recompute_aggregates(req, fact.id, now)

    async def _create_fact(
        self,
        req: ArtifactReconciliation,
        prop: ResolvedProposition,
        fk: str,
        now: datetime,
        report: ReconciliationReport,
    ) -> Fact:
        policy = policy_for(prop.predicate)
        auto = action_for_tier(req.trust_tier) is TrustAction.AUTO_PUBLISH
        sk = _slot_key(req.group_id, prop)
        lifecycle = FactLifecycle.ACTIVE if auto else FactLifecycle.PROPOSED

        superseded_ids: list[UUID] = []
        if policy.cardinality is Cardinality.SINGLE_PER_QUALIFIER_SET and auto:
            rivals = [
                f
                for f in await self._facts.active_by_slot_key(group_id=req.group_id, slot_key=sk)
                if f.normalized_object != _normalized(prop)
            ]
            if rivals:
                lifecycle, superseded_ids = await self._resolve_single_valued(
                    req, rivals, now, report
                )

        fact = self._new_fact(req, prop, fk, sk, lifecycle)
        if lifecycle is FactLifecycle.ACTIVE:
            fact = await self._facts.upsert(fact)
            await self._emit(req, KnowledgeEventType.FACT_ACTIVATED, now, fact_id=fact.id)
            report.facts_activated += 1
            for rival_id in superseded_ids:
                # The old revision stays queryable; the SUPERSEDES relation records the history.
                await self._relations.add(
                    FactRelation(
                        id=uuid7(),
                        group_id=req.group_id,
                        from_fact_id=fact.id,
                        to_fact_id=rival_id,
                        relation_type=RelationType.SUPERSEDES,
                    )
                )
        else:
            existing_any = await self._facts.by_fact_key(group_id=req.group_id, fact_key=fk)
            if existing_any is not None:
                return existing_any  # idempotent: the disputed/proposed fact already exists
            fact = await self._facts.upsert(fact)
            if lifecycle is FactLifecycle.DISPUTED:
                await self._emit(req, KnowledgeEventType.FACT_DISPUTED, now, fact_id=fact.id)
                report.facts_disputed += 1
        return fact

    async def _resolve_single_valued(
        self,
        req: ArtifactReconciliation,
        rivals: list[Fact],
        now: datetime,
        report: ReconciliationReport,
    ) -> tuple[FactLifecycle, list[UUID]]:
        """Decide the new fact's lifecycle against the current values in its slot, and demote
        rivals when superseded or equally contested. Returns the new fact's lifecycle and the
        ids of any rivals it supersedes (for the SUPERSEDES relations the caller records).
        """
        top = max(f.authority for f in rivals)
        if req.source_authority > top:
            for rival in rivals:
                await self._facts.set_lifecycle(
                    group_id=req.group_id, fact_id=str(rival.id), state=FactLifecycle.SUPERSEDED
                )
                await self._emit(
                    req,
                    KnowledgeEventType.FACT_SUPERSEDED,
                    now,
                    fact_id=rival.id,
                    reason="replaced by a higher-authority value",
                )
                report.facts_superseded += 1
            return FactLifecycle.ACTIVE, [r.id for r in rivals]
        if req.source_authority == top:
            for rival in rivals:
                await self._facts.set_lifecycle(
                    group_id=req.group_id, fact_id=str(rival.id), state=FactLifecycle.DISPUTED
                )
                await self._emit(req, KnowledgeEventType.FACT_DISPUTED, now, fact_id=rival.id)
                report.facts_disputed += 1
        # lower or equal authority: the new value does not overwrite; record it as disputed.
        return FactLifecycle.DISPUTED, []

    def _new_fact(
        self,
        req: ArtifactReconciliation,
        prop: ResolvedProposition,
        fk: str,
        sk: str,
        lifecycle: FactLifecycle,
    ) -> Fact:
        return Fact(
            id=uuid7(),
            group_id=req.group_id,
            fact_key=fk,
            slot_key=sk,
            subject_entity_id=prop.subject_entity_id,
            predicate=prop.predicate.upper(),
            object_type=ObjectType.ENTITY if prop.object_entity_id else ObjectType.SCALAR,
            normalized_object=_normalized(prop),
            object_entity_id=prop.object_entity_id,
            object_scalar=prop.object_scalar,
            qualifiers=dict(prop.qualifiers),
            lifecycle_state=lifecycle,
            authority=req.source_authority,
            confidence=prop.extractor_confidence,
            ontology_version_id=req.ontology_version_id,
        )

    # ----------------------------------------------------------------- refute ---

    async def _handle_refute(
        self,
        req: ArtifactReconciliation,
        prop: ResolvedProposition,
        now: datetime,
        report: ReconciliationReport,
    ) -> None:
        fk = _fact_key(req.group_id, prop)
        target = await self._facts.active_by_fact_key(group_id=req.group_id, fact_key=fk)
        if target is None:
            return  # nothing to refute; a refutation does not fabricate the fact it denies
        await self._attach_assertion(req, target, prop, Polarity.REFUTES, now, report)
        if req.source_authority >= target.authority:
            await self._facts.set_lifecycle(
                group_id=req.group_id, fact_id=str(target.id), state=FactLifecycle.DISPUTED
            )
            await self._emit(
                req,
                KnowledgeEventType.FACT_DISPUTED,
                now,
                fact_id=target.id,
                reason="refuted by a source of at least equal authority",
            )
            report.facts_disputed += 1

    # ------------------------------------------------------- assertions/events ---

    async def _attach_assertion(
        self,
        req: ArtifactReconciliation,
        fact: Fact,
        prop: ResolvedProposition,
        polarity: Polarity,
        now: datetime,
        report: ReconciliationReport,
    ) -> None:
        prior = [
            a
            for a in await self._assertions.active_for_fact(
                group_id=req.group_id, fact_id=str(fact.id)
            )
            if a.artifact_id == req.artifact_id and a.polarity is polarity
        ]
        assertion = await self._assertions.upsert(
            Assertion(
                id=uuid7(),
                group_id=req.group_id,
                fact_id=fact.id,
                polarity=polarity,
                knowledge_source_id=req.knowledge_source_id,
                artifact_id=req.artifact_id,
                artifact_version_id=req.artifact_version_id,
                extractor_confidence=prop.extractor_confidence,
                source_authority=req.source_authority,
                observed_at=now,
                recorded_at=now,
            )
        )
        if prior:
            await self._emit(
                req,
                KnowledgeEventType.ASSERTION_REAFFIRMED,
                now,
                fact_id=fact.id,
                assertion_id=assertion.id,
            )
            report.assertions_reaffirmed += 1
        else:
            await self._emit(
                req,
                KnowledgeEventType.ASSERTION_ADDED,
                now,
                fact_id=fact.id,
                assertion_id=assertion.id,
            )
            report.assertions_added += 1
        await self._add_evidence(req, assertion.id, prop, now, report)

    async def _add_evidence(
        self,
        req: ArtifactReconciliation,
        assertion_id: UUID,
        prop: ResolvedProposition,
        now: datetime,
        report: ReconciliationReport,
    ) -> None:
        content = prop.excerpt or (str(prop.chunk_id) if prop.chunk_id else None)
        if content is None:
            return
        content_hash = prop.evidence_content_hash or hashlib.sha256(content.encode()).hexdigest()
        before = await self._evidence.for_assertion(
            group_id=req.group_id, assertion_id=str(assertion_id)
        )
        added = await self._evidence.add(
            Evidence(
                id=uuid7(),
                group_id=req.group_id,
                assertion_id=assertion_id,
                content_hash=content_hash,
                chunk_id=prop.chunk_id,
                artifact_version_id=req.artifact_version_id,
                excerpt=prop.excerpt,
                citation_uri=prop.citation_uri,
            )
        )
        if all(e.id != added.id for e in before):
            await self._emit(req, KnowledgeEventType.EVIDENCE_ADDED, now, assertion_id=assertion_id)
            report.evidence_added += 1

    async def _recompute_aggregates(
        self, req: ArtifactReconciliation, fact_id: UUID, now: datetime
    ) -> None:
        supports = [
            a
            for a in await self._assertions.active_for_fact(
                group_id=req.group_id, fact_id=str(fact_id)
            )
            if a.polarity is Polarity.SUPPORTS
        ]
        if not supports:
            return
        await self._facts.set_aggregates(
            group_id=req.group_id,
            fact_id=str(fact_id),
            authority=max(a.source_authority for a in supports),
            confidence=max(a.extractor_confidence for a in supports),
        )

    # --------------------------------------------------------------- withdraw ---

    async def _withdraw_dropped(
        self, req: ArtifactReconciliation, now: datetime, report: ReconciliationReport
    ) -> None:
        """Withdraw this artifact's assertions from earlier versions (a new version supersedes
        an old one), then transition any fact left with no active support per its policy.
        """
        if req.artifact_id is None:
            return
        stale = [
            a
            for a in await self._assertions.active_for_artifact(
                group_id=req.group_id, artifact_id=str(req.artifact_id)
            )
            if a.artifact_version_id != req.artifact_version_id
        ]
        touched: set[UUID] = set()
        for assertion in stale:
            await self._assertions.withdraw(group_id=req.group_id, assertion_id=str(assertion.id))
            await self._emit(
                req,
                KnowledgeEventType.ASSERTION_WITHDRAWN,
                now,
                fact_id=assertion.fact_id,
                assertion_id=assertion.id,
            )
            report.assertions_withdrawn += 1
            touched.add(assertion.fact_id)
        for fact_id in touched:
            await self._transition_if_unsupported(req, fact_id, now, report)

    async def _transition_if_unsupported(
        self,
        req: ArtifactReconciliation,
        fact_id: UUID,
        now: datetime,
        report: ReconciliationReport,
    ) -> None:
        active = await self._assertions.active_for_fact(group_id=req.group_id, fact_id=str(fact_id))
        if any(a.polarity is Polarity.SUPPORTS for a in active):
            await self._recompute_aggregates(req, fact_id, now)
            return  # still supported elsewhere; leave it active
        fact = await self._facts.get(group_id=req.group_id, fact_id=str(fact_id))
        if fact is None or fact.lifecycle_state in (
            FactLifecycle.RETRACTED,
            FactLifecycle.SUPERSEDED,
            FactLifecycle.EXPIRED,
        ):
            return
        semantics = policy_for(fact.predicate).absence_semantics
        if semantics is AbsenceSemantics.KEEP:
            return
        state = {
            AbsenceSemantics.RETRACT: FactLifecycle.RETRACTED,
            AbsenceSemantics.EXPIRE: FactLifecycle.EXPIRED,
            AbsenceSemantics.REVIEW: FactLifecycle.DISPUTED,
        }[semantics]
        await self._facts.set_lifecycle(group_id=req.group_id, fact_id=str(fact_id), state=state)
        event = (
            KnowledgeEventType.FACT_DISPUTED
            if state is FactLifecycle.DISPUTED
            else KnowledgeEventType.FACT_RETRACTED
        )
        await self._emit(
            req, event, now, fact_id=fact_id, reason="final supporting assertion withdrawn"
        )
        if state is FactLifecycle.DISPUTED:
            report.facts_disputed += 1
        else:
            report.facts_retracted += 1

    # ------------------------------------------------------------------ utils ---

    async def _emit(
        self,
        req: ArtifactReconciliation,
        event_type: KnowledgeEventType,
        now: datetime,
        *,
        fact_id: UUID | None = None,
        assertion_id: UUID | None = None,
        reason: str | None = None,
    ) -> None:
        await self._events.append(
            KnowledgeEvent(
                id=uuid7(),
                group_id=req.group_id,
                event_type=event_type,
                occurred_at=now,
                actor=req.actor,
                fact_id=fact_id,
                assertion_id=assertion_id,
                artifact_id=req.artifact_id,
                reason=reason,
                policy_version=_POLICY_VERSION,
                trace_id=req.trace_id,
            )
        )


def _normalized(prop: ResolvedProposition) -> str:
    return normalize_object(
        object_entity_id=prop.object_entity_id, object_scalar=prop.object_scalar
    )


def _fact_key(group_id: str, prop: ResolvedProposition) -> str:
    return fact_key(
        scope=group_id,
        subject_entity_id=prop.subject_entity_id,
        predicate=prop.predicate,
        object_entity_id=prop.object_entity_id,
        object_scalar=prop.object_scalar,
        qualifiers=prop.qualifiers,
    )


def _slot_key(group_id: str, prop: ResolvedProposition) -> str:
    return slot_key(
        scope=group_id,
        subject_entity_id=prop.subject_entity_id,
        predicate=prop.predicate,
        qualifiers=prop.qualifiers,
    )
