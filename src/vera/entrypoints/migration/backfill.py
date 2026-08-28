"""Backfill the legacy published-episode model into Fact / Assertion / Evidence (Phase 8).

Each structured published episode becomes a Fact (deduplicated by fact_key) with a supporting
Assertion carrying the original source_id and verification, and Evidence for the triple. An
edge-predicate triple's object is resolved to a canonical entity, so the object side of the
graph edge is reconstructed rather than stored as a scalar string. The old ids and graph
mappings are preserved (published_episodes and the graph maps are left in place). The
conversion is idempotent: re-running converges (fact_key dedups the Fact and the assertion
source key dedups the Assertion), so a partial run can be resumed. A free-text episode with no
structured triple is re-extracted through the claim extractor when one is configured, taking
its provenance from the original episode (nothing is invented); without an extractor it is
counted for review rather than fabricated (ADR-0006). See docs/runbooks.

Runs inside a tenant-scoped transaction (the caller sets the RLS group), so it only ever
touches one group's rows.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.repositories.canonical import SqlAlchemyCanonicalEntityRepository
from vera.adapters.persistence.repositories.fabric import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyEvidenceRepository,
    SqlAlchemyFactRepository,
    SqlAlchemyKnowledgeEventLog,
)
from vera.domain.knowledge.fabric import (
    Assertion,
    Evidence,
    Fact,
    FactLifecycle,
    KnowledgeEvent,
    KnowledgeEventType,
    ObjectType,
    Polarity,
    fact_key,
    normalize_object,
    slot_key,
)
from vera.domain.ontology import is_edge_predicate
from vera.domain.ports.curation import ClaimExtractor
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import JsonDict

_EPISODES = text(
    "SELECT id::text AS id, source_id, artifact_version_id, verification, authority, confidence, "
    "payload, invalid_at "
    "FROM published_episodes WHERE group_id = :g AND retracted_at IS NULL"
)
_VERIFY = text(
    "SELECT "
    "(SELECT count(*) FROM published_episodes WHERE group_id = :g AND retracted_at IS NULL) "
    "  AS episodes, "
    "(SELECT count(*) FROM facts WHERE group_id = :g) AS facts, "
    "(SELECT count(*) FROM assertions WHERE group_id = :g) AS assertions"
)


@dataclass(slots=True)
class BackfillReport:
    episodes_processed: int = 0
    facts_created: int = 0
    assertions_created: int = 0
    evidence_created: int = 0
    needs_review: int = 0  # free-text episodes with no structured triple


class FabricBackfillService:
    def __init__(self, session: AsyncSession, extractor: ClaimExtractor | None = None) -> None:
        self._session = session
        self._extractor = extractor
        self._canonical = SqlAlchemyCanonicalEntityRepository(session)
        self._facts = SqlAlchemyFactRepository(session)
        self._assertions = SqlAlchemyAssertionRepository(session)
        self._evidence = SqlAlchemyEvidenceRepository(session)
        self._events = SqlAlchemyKnowledgeEventLog(session)

    async def backfill_group(self, *, group_id: str) -> BackfillReport:
        report = BackfillReport()
        now = utc_now()
        rows = (await self._session.execute(_EPISODES, {"g": group_id})).mappings().all()
        for row in rows:
            report.episodes_processed += 1
            payload: JsonDict = (
                cast("JsonDict", row["payload"]) if isinstance(row["payload"], dict) else {}
            )
            triples = payload.get("triples")
            if not triples:
                triples = await self._reextract(payload)
            if not triples:
                report.needs_review += 1
                continue
            for triple in triples:
                await self._backfill_triple(group_id, row, triple, now, report)
        return report

    async def _reextract(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Turn a free-text episode into triples with the claim extractor, when one is
        configured. The provenance still comes from the original episode row (authority,
        confidence, verification, source), so nothing is invented: the extractor only recovers
        the propositions the text already asserts. Without an extractor, returns nothing and the
        episode is counted for review.
        """
        body = payload.get("body")
        if self._extractor is None or not body:
            return []
        claims = await self._extractor.extract(body=str(body), knowledge_type="text", metadata={})
        return [
            {"subject": c.subject, "predicate": c.predicate, "object": c.object}
            for c in claims
            if c.subject and c.predicate and c.object
        ]

    async def _backfill_triple(
        self,
        group_id: str,
        row: Any,
        triple: dict[str, Any],
        now: datetime,
        report: BackfillReport,
    ) -> None:
        subject = str(triple.get("subject", "")).strip()
        predicate = str(triple.get("predicate", "")).strip()
        obj = str(triple.get("object", "")).strip()
        if not (subject and predicate and obj):
            report.needs_review += 1
            return

        entity = await self._canonical.resolve(group_id=group_id, name=subject) or (
            await self._canonical.create(
                group_id=group_id, entity_type="Entity", canonical_name=subject, aliases=[]
            )
        )
        # An edge predicate relates two entities: resolve the object to a canonical entity so the
        # object side of the graph edge is reconstructed, not lost as a scalar string. Scalar
        # attributes (HAS_STATUS and the like) stay scalar.
        object_entity_id: UUID | None = None
        object_scalar: str | None = obj
        if is_edge_predicate(predicate):
            object_entity = await self._canonical.resolve(group_id=group_id, name=obj) or (
                await self._canonical.create(
                    group_id=group_id, entity_type="Entity", canonical_name=obj, aliases=[]
                )
            )
            object_entity_id = object_entity.id
            object_scalar = None
        fk = fact_key(
            scope=group_id,
            subject_entity_id=entity.id,
            predicate=predicate,
            object_entity_id=object_entity_id,
            object_scalar=object_scalar,
        )
        lifecycle = (
            FactLifecycle.SUPERSEDED if row["invalid_at"] is not None else FactLifecycle.ACTIVE
        )

        existing = await self._facts.by_fact_key(group_id=group_id, fact_key=fk)
        if existing is None:
            fact = await self._facts.upsert(
                Fact(
                    id=uuid7(),
                    group_id=group_id,
                    fact_key=fk,
                    slot_key=slot_key(
                        scope=group_id, subject_entity_id=entity.id, predicate=predicate
                    ),
                    subject_entity_id=entity.id,
                    predicate=predicate.upper(),
                    object_type=(
                        ObjectType.ENTITY if object_entity_id is not None else ObjectType.SCALAR
                    ),
                    normalized_object=normalize_object(
                        object_entity_id=object_entity_id, object_scalar=object_scalar
                    ),
                    object_entity_id=object_entity_id,
                    object_scalar=object_scalar,
                    lifecycle_state=lifecycle,
                    authority=float(row["authority"]),
                    confidence=float(row["confidence"]),
                )
            )
            report.facts_created += 1
            if lifecycle is FactLifecycle.ACTIVE:
                await self._events.append(
                    KnowledgeEvent(
                        id=uuid7(),
                        group_id=group_id,
                        event_type=KnowledgeEventType.FACT_ACTIVATED,
                        occurred_at=now,
                        actor="backfill",
                        fact_id=fact.id,
                        reason="migrated from published_episode",
                    )
                )
        else:
            fact = existing

        # Idempotency for episodes without an artifact version: the assertion unique key treats
        # a NULL version as distinct, so guard on the per-episode extraction run id instead.
        run_id = f"backfill:{row['source_id']}"
        already = await self._assertions.active_for_fact(group_id=group_id, fact_id=str(fact.id))
        if any(a.extraction_run_id == run_id for a in already):
            return

        version_id = UUID(str(row["artifact_version_id"])) if row["artifact_version_id"] else None
        assertion = await self._assertions.upsert(
            Assertion(
                id=uuid7(),
                group_id=group_id,
                fact_id=fact.id,
                polarity=Polarity.SUPPORTS,
                artifact_version_id=version_id,
                source_authority=float(row["authority"]),
                extractor_confidence=float(row["confidence"]),
                verification_state=row["verification"],
                observed_at=now,
                recorded_at=now,
                extraction_run_id=run_id,
            )
        )
        report.assertions_created += 1

        excerpt = f"{subject} {predicate} {obj}"
        before = await self._evidence.for_assertion(
            group_id=group_id, assertion_id=str(assertion.id)
        )
        added = await self._evidence.add(
            Evidence(
                id=uuid7(),
                group_id=group_id,
                assertion_id=assertion.id,
                content_hash=hashlib.sha256(f"{row['source_id']}:{excerpt}".encode()).hexdigest(),
                artifact_version_id=version_id,
                excerpt=excerpt,
                confidentiality="internal",
            )
        )
        if all(e.id != added.id for e in before):
            report.evidence_created += 1

    async def verify_group(self, *, group_id: str) -> dict[str, int]:
        row = (await self._session.execute(_VERIFY, {"g": group_id})).mappings().one()
        return {"episodes": row["episodes"], "facts": row["facts"], "assertions": row["assertions"]}
