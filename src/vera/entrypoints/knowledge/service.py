"""KnowledgeService: the generic knowledge operations behind the REST and MCP contracts.

Every method resolves the caller's readable scopes from the authenticated principal, so a
consumer never chooses a scope (invariant 4). Reads span the resolved scopes; get_context and
snapshots act on one resolved project; a proposal lands in the caller's personal scope as a
PROPOSED fact with a pending assertion, never a published shared fact (invariant 5).
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from vera.adapters.persistence.repositories import (
    SqlAlchemyCanonicalEntityRepository,
    SqlAlchemyCommunityLineageRepository,
)
from vera.adapters.persistence.repositories.fabric import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyEvidenceRepository,
    SqlAlchemyFactRepository,
    SqlAlchemyKnowledgeEventLog,
)
from vera.adapters.persistence.repositories.knowledge_read import SqlAlchemyKnowledgeReadModel
from vera.adapters.persistence.repositories.ontology import SqlAlchemyOntologyRepository
from vera.adapters.persistence.repositories.passage_index import (
    SqlAlchemyCodeIndex,
    SqlAlchemyFactCandidateSource,
    SqlAlchemyPassageIndex,
)
from vera.adapters.persistence.repositories.pgvector_index import (
    PgVectorCodeIndex,
    PgVectorPassageIndex,
)
from vera.adapters.persistence.repositories.snapshot import (
    SqlAlchemyContextPackRepository,
    SqlAlchemySnapshotRepository,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.retrieval import ContextAssembler, HybridPassageIndex
from vera.application.snapshot import ContextPackService, SnapshotService
from vera.bootstrap import Container
from vera.domain.identity.models import Role, role_at_least
from vera.domain.identity.scopes import ScopeKind, scope_kind
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
from vera.domain.ontology import current_descriptor, diff_descriptors
from vera.domain.ports.identity import ResolvedScope, ScopeResolver
from vera.domain.ports.retrieval_index import CodeIndex, PassageIndex, RetrievalFilters
from vera.domain.ports.snapshot import ContextPack
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import GroupId, JsonDict

_PROPOSAL_AUTHORITY = 0.4  # tier 4 (unverified) authority; proposals never outrank real facts


class ScopeError(Exception):
    """The principal has no resolvable scope, or requested a scope it may not access."""


def _context_pack_payload(pack: ContextPack) -> JsonDict:
    return {
        "pack_id": pack.id,
        "scope_id": pack.group_id,
        "snapshot_id": pack.snapshot_id,
        "query": pack.query,
        "created_at": pack.created_at.isoformat(),
        "expires_at": pack.expires_at.isoformat(),
        "request_hash": pack.request_hash,
        "result_references": pack.result_references,
        "assembler_version": pack.assembler_version,
        "token_estimate": pack.token_estimate,
        "result_count": pack.result_count,
        "omitted": pack.omitted,
        "conflicts": pack.conflicts,
        "freshness_warnings": pack.freshness_warnings,
        "results": pack.results,
    }


class KnowledgeService:
    def __init__(self, container: Container, scope_resolver: ScopeResolver) -> None:
        self._c = container
        self._scopes = scope_resolver
        # Read repos run on the cross-scope read path (vera_trusted when enforced); writes go
        # through a UnitOfWork on the base factory (vera_app + RLS via use_tenant).
        sm = container.reads
        self._read = SqlAlchemyKnowledgeReadModel(sm)
        self._snapshots = SqlAlchemySnapshotRepository(sm)
        self._packs = SqlAlchemyContextPackRepository(sm)
        self._community_lineage = SqlAlchemyCommunityLineageRepository(sm)
        passages: PassageIndex
        code: CodeIndex
        if container.settings.memory.vector_search_enabled and container.embedder is not None:
            passages = HybridPassageIndex(
                SqlAlchemyPassageIndex(sm),
                PgVectorPassageIndex(
                    sm,
                    container.embedder,
                    provider=container.settings.memory.embedder,
                    model=container.settings.memory.embedding_model,
                    model_version=container.settings.memory.embedding_model_version,
                    dimension=container.settings.memory.embedding_dim,
                ),
            )
            code = HybridPassageIndex(
                SqlAlchemyCodeIndex(sm),
                PgVectorCodeIndex(
                    sm,
                    container.embedder,
                    provider=container.settings.memory.embedder,
                    model=container.settings.memory.embedding_model,
                    model_version=container.settings.memory.embedding_model_version,
                    dimension=container.settings.memory.embedding_dim,
                ),
            )
        else:
            passages = SqlAlchemyPassageIndex(sm)
            code = SqlAlchemyCodeIndex(sm)
        self._assembler = ContextAssembler(
            facts=SqlAlchemyFactCandidateSource(sm), passages=passages, code=code
        )

    async def _resolve(self, principal_id: UUID) -> ResolvedScope:
        scope = await self._scopes.resolve(principal_id)
        if scope is None:
            raise ScopeError(f"principal {principal_id} has no scope")
        return scope

    async def _target_group(self, scope: ResolvedScope, project: str | None) -> str:
        if project is not None:
            if project in scope.group_ids:
                return project
            resolved = await self._read.resolve_project(
                group_ids=list(scope.group_ids), project=project
            )
            if resolved is not None:
                return resolved
            raise ScopeError("project is outside the caller's resolved scopes")
        shared = [g for g in scope.group_ids if scope_kind(g) is not ScopeKind.PERSONAL]
        if len(shared) == 1:
            return shared[0]
        if not shared and scope.personal_group_id:
            return scope.personal_group_id
        raise ScopeError("ambiguous scope: specify a project")

    # ---------------------------------------------------------------- context ---

    async def get_context(
        self,
        principal_id: UUID,
        *,
        query: str,
        project: str | None = None,
        snapshot_id: str | None = None,
        limit: int = 10,
        token_budget: int = 2000,
        as_of: datetime | None = None,
        hints: JsonDict | None = None,
        repository: str | None = None,
        branch: str | None = None,
        code_path: str | None = None,
        document_type: str | None = None,
        source_type: str | None = None,
        include_predicates: tuple[str, ...] = (),
        exclude_predicates: tuple[str, ...] = (),
        min_authority: float | None = None,
        max_trust_tier: int | None = None,
        citation_mode: Literal["full", "compact"] = "full",
        conflict_handling: Literal["include", "exclude", "only"] = "include",
    ) -> JsonDict:
        scope = await self._resolve(principal_id)
        group = await self._target_group(scope, project)
        service = ContextPackService(
            assembler=self._assembler, snapshots=self._snapshots, packs=self._packs
        )
        filters = RetrievalFilters(
            repository=repository,
            branch=branch,
            code_path=code_path,
            document_type=document_type,
            source_type=source_type,
            include_predicates=tuple(predicate.upper() for predicate in include_predicates),
            exclude_predicates=tuple(predicate.upper() for predicate in exclude_predicates),
            min_authority=min_authority,
            max_trust_tier=max_trust_tier,
            conflict_handling=conflict_handling,
        )
        pack = await service.create(
            group_id=group,
            query=query,
            snapshot_id=snapshot_id,
            hints=hints,
            limit=limit,
            token_budget=token_budget,
            as_of=as_of,
            filters=filters,
            citation_mode=citation_mode,
            actor=str(principal_id),
        )
        return _context_pack_payload(pack)

    async def get_context_pack(self, principal_id: UUID, *, pack_id: str) -> JsonDict | None:
        scope = await self._resolve(principal_id)
        service = ContextPackService(
            assembler=self._assembler, snapshots=self._snapshots, packs=self._packs
        )
        for group_id in scope.group_ids:
            pack = await service.get(group_id=group_id, pack_id=pack_id)
            if pack is not None:
                return _context_pack_payload(pack)
        return None

    async def search(
        self,
        principal_id: UUID,
        *,
        query: str,
        project: str | None = None,
        limit: int = 10,
        as_of: datetime | None = None,
    ) -> JsonDict:
        scope = await self._resolve(principal_id)
        group = await self._target_group(scope, project)
        assembled = await self._assembler.assemble(
            query=query, group_id=group, limit=limit, as_of=as_of
        )
        return {
            "query": query,
            "conflicts": assembled.conflicts,
            "freshness_warnings": assembled.freshness_warnings,
            "omitted": assembled.omitted,
            "results": [
                {
                    "kind": r.kind,
                    "ref": r.ref,
                    "text": r.text,
                    "score": r.score,
                    "conflict": r.conflict,
                    "reason": r.reason,
                    "citation": {
                        "kind": r.citation.kind,
                        "ref": r.citation.ref,
                        "excerpt": r.citation.excerpt,
                    },
                }
                for r in assembled.results
            ],
        }

    async def communities(
        self,
        principal_id: UUID,
        *,
        project: str | None = None,
        query: str | None = None,
        limit: int = 20,
    ) -> list[JsonDict]:
        scope = await self._resolve(principal_id)
        group = await self._target_group(scope, project)
        communities = await self._c.memory.search_communities(
            group_ids=(GroupId(group),), query=query, limit=limit
        )
        return [
            {
                "kind": "community_summary",
                "community_id": community.community_id,
                "name": community.name,
                "summary": community.summary,
                "derived": True,
                "authoritative": False,
                "evidence": None,
                "derivation_run_id": community.derivation_run_id,
                "source_fact_set_hash": community.source_fact_set_hash,
                "projection_checkpoint": community.projection_checkpoint,
            }
            for community in communities
        ]

    async def community_lineage(
        self,
        principal_id: UUID,
        *,
        community_id: str,
        derivation_run_id: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> JsonDict | None:
        scope = await self._resolve(principal_id)
        page = await self._community_lineage.page(
            group_ids=tuple(scope.group_ids),
            community_id=UUID(community_id),
            derivation_run_id=UUID(derivation_run_id) if derivation_run_id else None,
            cursor=UUID(cursor) if cursor else None,
            limit=limit,
        )
        if not page.items:
            return None
        return {
            "community_id": community_id,
            "derivation_run_id": str(page.items[0].derivation_run_id),
            "derived": True,
            "items": [
                {
                    "fact_id": str(item.fact_id),
                    "fact_key": item.fact_key,
                    "subject": item.subject_name,
                    "predicate": item.predicate,
                    "object": item.object_name,
                    "created_at": item.created_at.isoformat(),
                }
                for item in page.items
            ],
            "next_cursor": page.next_cursor,
        }

    # ------------------------------------------------------------------- reads ---

    async def get_fact(self, principal_id: UUID, *, fact_key: str) -> dict[str, Any] | None:
        scope = await self._resolve(principal_id)
        return await self._read.get_fact(group_ids=list(scope.group_ids), fact_key=fact_key)

    async def get_entity(
        self, principal_id: UUID, *, entity_id: str, limit: int = 100
    ) -> dict[str, Any] | None:
        scope = await self._resolve(principal_id)
        return await self._read.get_entity(
            group_ids=list(scope.group_ids), entity_id=entity_id, limit=limit
        )

    async def get_source(self, principal_id: UUID, *, source_id: str) -> dict[str, Any] | None:
        scope = await self._resolve(principal_id)
        return await self._read.get_source(group_ids=list(scope.group_ids), source_id=source_id)

    async def explain_fact(self, principal_id: UUID, *, fact_key: str) -> dict[str, Any] | None:
        scope = await self._resolve(principal_id)
        return await self._read.explain_fact(group_ids=list(scope.group_ids), fact_key=fact_key)

    async def get_evidence(
        self, principal_id: UUID, *, fact_key: str
    ) -> list[dict[str, Any]] | None:
        scope = await self._resolve(principal_id)
        return await self._read.get_evidence(group_ids=list(scope.group_ids), fact_key=fact_key)

    async def record_feedback(
        self,
        principal_id: UUID,
        *,
        result_ref: str,
        signal: str,
        query: str = "",
        signals: JsonDict | None = None,
    ) -> JsonDict:
        """Record a caller's up/down feedback on a knowledge result (a fact_key or context-pack
        id). Feedback is a personal signal, so it is written under the caller's personal scope;
        it never mutates shared truth.
        """
        if signal not in {"up", "down"}:
            raise ScopeError("signal must be 'up' or 'down'")
        scope = await self._resolve(principal_id)
        group = scope.personal_group_id
        async with SqlAlchemyUnitOfWork(self._c.sessionmaker) as uow:
            await uow.use_tenant(group)
            await uow.feedback.record(
                group_id=group,
                principal_id=principal_id,
                query=query,
                result_ref=result_ref,
                signal=signal,
                signals=signals,
            )
            await uow.commit()
        return {"status": "recorded", "result_ref": result_ref, "signal": signal}

    async def get_changes(self, principal_id: UUID, *, limit: int = 50) -> list[dict[str, Any]]:
        scope = await self._resolve(principal_id)
        return await self._read.recent_changes(group_ids=list(scope.group_ids), limit=limit)

    async def get_conflicts(self, principal_id: UUID, *, limit: int = 50) -> list[dict[str, Any]]:
        scope = await self._resolve(principal_id)
        return await self._read.conflicts(group_ids=list(scope.group_ids), limit=limit)

    # --------------------------------------------------------------- snapshots ---

    async def create_snapshot(
        self, principal_id: UUID, *, project: str | None = None, as_of: datetime | None = None
    ) -> JsonDict:
        scope = await self._resolve(principal_id)
        group = await self._target_group(scope, project)
        embedding_version: JsonDict = {}
        retrieval_index_version = "fts-v1"
        if self._c.settings.memory.vector_search_enabled and self._c.embedder is not None:
            memory = self._c.settings.memory
            embedding_version = {
                "provider": memory.embedder,
                "model": memory.embedding_model,
                "model_version": memory.embedding_model_version,
                "dimension": memory.embedding_dim,
            }
            retrieval_index_version = "hybrid-rrf-v1"
        snap = await SnapshotService(snapshots=self._snapshots).create(
            group_id=group,
            as_of=as_of,
            embedding_version=embedding_version,
            retrieval_index_version=retrieval_index_version,
            actor=str(principal_id),
        )
        return {
            "snapshot_id": snap.id,
            "fact_count": snap.fact_count,
            "created_at": snap.created_at.isoformat(),
            "as_of_valid_time": snap.as_of_valid_time.isoformat(),
            "frozen_at_system_time": snap.frozen_at_system_time.isoformat(),
            "embedding_version": snap.embedding_version,
            "retrieval_index_version": snap.retrieval_index_version,
            "graph_projection_checkpoint": snap.graph_projection_checkpoint,
            "policy_version": snap.policy_version,
        }

    async def get_snapshot(self, principal_id: UUID, *, snapshot_id: str) -> JsonDict | None:
        scope = await self._resolve(principal_id)
        for group in scope.group_ids:
            snap = await self._snapshots.get(group_id=group, snapshot_id=snapshot_id)
            if snap is not None:
                return {
                    "snapshot_id": snap.id,
                    "group_id": snap.group_id,
                    "fact_count": snap.fact_count,
                    "created_at": snap.created_at.isoformat(),
                    "as_of_valid_time": snap.as_of_valid_time.isoformat(),
                    "frozen_at_system_time": snap.frozen_at_system_time.isoformat(),
                    "embedding_version": snap.embedding_version,
                    "retrieval_index_version": snap.retrieval_index_version,
                    "graph_projection_checkpoint": snap.graph_projection_checkpoint,
                    "policy_version": snap.policy_version,
                    "source_boundaries": snap.source_boundaries,
                }
        return None

    # ----------------------------------------------------------------- propose ---

    async def propose(
        self,
        principal_id: UUID,
        *,
        subject: str,
        predicate: str,
        object: str,
        qualifiers: JsonDict | None = None,
        evidence_text: str | None = None,
    ) -> JsonDict:
        scope = await self._resolve(principal_id)
        group = scope.personal_group_id  # proposals are always personal, never shared truth
        async with SqlAlchemyUnitOfWork(self._c.sessionmaker) as uow:
            await uow.use_tenant(group)
            session = uow.session
            canonical = SqlAlchemyCanonicalEntityRepository(session)
            subject_entity = await canonical.resolve(group_id=group, name=subject) or (
                await canonical.create(
                    group_id=group, entity_type="Entity", canonical_name=subject, aliases=[]
                )
            )
            fk = fact_key(
                scope=group,
                subject_entity_id=subject_entity.id,
                predicate=predicate,
                object_scalar=object,
                qualifiers=qualifiers,
            )
            facts = SqlAlchemyFactRepository(session)
            existing = await facts.by_fact_key(group_id=group, fact_key=fk)
            if existing is not None:
                fact = existing
            else:
                fact = await facts.upsert(
                    Fact(
                        id=uuid7(),
                        group_id=group,
                        fact_key=fk,
                        slot_key=slot_key(
                            scope=group,
                            subject_entity_id=subject_entity.id,
                            predicate=predicate,
                            qualifiers=qualifiers,
                        ),
                        subject_entity_id=subject_entity.id,
                        predicate=predicate.upper(),
                        object_type=ObjectType.SCALAR,
                        normalized_object=normalize_object(object_scalar=object),
                        object_scalar=object,
                        qualifiers=dict(qualifiers or {}),
                        lifecycle_state=FactLifecycle.PROPOSED,
                        authority=_PROPOSAL_AUTHORITY,
                        confidence=0.5,
                    )
                )
            assertion = await SqlAlchemyAssertionRepository(session).upsert(
                Assertion(
                    id=uuid7(),
                    group_id=group,
                    fact_id=fact.id,
                    polarity=Polarity.SUPPORTS,
                    source_authority=_PROPOSAL_AUTHORITY,
                    extractor_confidence=0.5,
                    verification_state="pending",
                    observed_at=utc_now(),
                    recorded_at=utc_now(),
                    run_key=f"proposal:{principal_id}",
                )
            )
            if evidence_text:
                await SqlAlchemyEvidenceRepository(session).add(
                    Evidence(
                        id=uuid7(),
                        group_id=group,
                        assertion_id=assertion.id,
                        content_hash=hashlib.sha256(evidence_text.encode()).hexdigest(),
                        excerpt=evidence_text,
                        confidentiality="internal",
                    )
                )
            await SqlAlchemyKnowledgeEventLog(session).append(
                KnowledgeEvent(
                    id=uuid7(),
                    group_id=group,
                    event_type=KnowledgeEventType.ASSERTION_ADDED,
                    occurred_at=utc_now(),
                    actor=str(principal_id),
                    fact_id=fact.id,
                    assertion_id=assertion.id,
                    reason="agent proposal (personal scope)",
                )
            )
            await uow.commit()
        return {"status": "proposed", "fact_key": fk, "lifecycle": "proposed", "group_id": group}

    # -------------------------------------------------------------- governance ---

    async def _can_admin(self, principal_id: UUID, group_id: str) -> bool:
        role = await self._scopes.role_for(principal_id, group_id)
        return role is not None and role_at_least(role, Role.ADMIN)

    async def review_queue(self, principal_id: UUID, *, limit: int = 50) -> list[dict[str, Any]]:
        scope = await self._resolve(principal_id)
        return await self._read.review_queue(group_ids=list(scope.group_ids), limit=limit)

    async def fact_timeline(self, principal_id: UUID, *, fact_key: str) -> list[dict[str, Any]]:
        scope = await self._resolve(principal_id)
        return await self._read.fact_timeline(group_ids=list(scope.group_ids), fact_key=fact_key)

    async def promote_fact(self, principal_id: UUID, *, fact_key: str) -> JsonDict:
        return await self._transition_fact(
            principal_id,
            fact_key=fact_key,
            to=FactLifecycle.ACTIVE,
            event=KnowledgeEventType.FACT_ACTIVATED,
            reason="promoted by reviewer",
        )

    async def reject_fact(self, principal_id: UUID, *, fact_key: str) -> JsonDict:
        return await self._transition_fact(
            principal_id,
            fact_key=fact_key,
            to=FactLifecycle.RETRACTED,
            event=KnowledgeEventType.FACT_RETRACTED,
            reason="rejected by reviewer",
        )

    async def _transition_fact(
        self,
        principal_id: UUID,
        *,
        fact_key: str,
        to: FactLifecycle,
        event: KnowledgeEventType,
        reason: str,
    ) -> JsonDict:
        scope = await self._resolve(principal_id)
        fact = await self._read.get_fact(group_ids=list(scope.group_ids), fact_key=fact_key)
        if fact is None:
            raise ScopeError("fact not found in the caller's scopes")
        group = fact["group_id"]
        if not await self._can_admin(principal_id, group):
            raise ScopeError("this action requires an admin role on the fact's scope")
        async with SqlAlchemyUnitOfWork(self._c.sessionmaker) as uow:
            await uow.use_tenant(group)
            session = uow.session
            await SqlAlchemyFactRepository(session).set_lifecycle(
                group_id=group, fact_id=fact["fact_id"], state=to
            )
            await SqlAlchemyKnowledgeEventLog(session).append(
                KnowledgeEvent(
                    id=uuid7(),
                    group_id=group,
                    event_type=event,
                    occurred_at=utc_now(),
                    actor=str(principal_id),
                    fact_id=UUID(fact["fact_id"]),
                    reason=reason,
                )
            )
            await uow.commit()
        return {"fact_key": fact_key, "lifecycle": to.value, "group_id": group}

    async def ontology(self) -> JsonDict:
        """The active ontology from the persisted registry: its identity and the governance
        policies it shipped with. Falls back to the code registry only if nothing is persisted.
        """
        async with self._c.reads() as session:
            active = await SqlAlchemyOntologyRepository(session).get_active()
        if active is None:
            active = current_descriptor()
        return {
            "ontology_version_id": str(active.id) if active.id is not None else None,
            "ontology_version": active.version,
            "name": active.name,
            "entity_types": list(active.entity_types),
            "predicates": [
                {
                    "predicate": p.predicate,
                    "cardinality": p.cardinality.value,
                    "absence_semantics": p.absence_semantics.value,
                    "conflict_strategy": p.conflict_strategy.value,
                    "subject_types": list(p.subject_types),
                    "object_types": list(p.object_types),
                    "object_kind": p.object_kind.value,
                    "qualifier_schema": {
                        rule.name: {
                            "type": rule.value_type.value,
                            "required": rule.required,
                        }
                        for rule in p.qualifier_schema
                    },
                    "allow_additional_qualifiers": p.allow_additional_qualifiers,
                    "minimum_source_authority": p.minimum_source_authority,
                    "ttl_seconds": p.ttl_seconds,
                    "deprecated": p.deprecated,
                    "replacement_predicate": p.replacement_predicate,
                }
                for p in active.predicate_policies
            ],
        }

    async def ontology_diff(self, *, from_version: int, to_version: int | None = None) -> JsonDict:
        async with self._c.reads() as session:
            repository = SqlAlchemyOntologyRepository(session)
            previous = await repository.get_version(from_version)
            current = (
                await repository.get_version(to_version)
                if to_version is not None
                else await repository.get_active()
            )
        if previous is None:
            raise ValueError(f"ontology version {from_version} was not found")
        if current is None:
            requested = to_version if to_version is not None else "active"
            raise ValueError(f"ontology version {requested} was not found")
        return diff_descriptors(previous, current)
