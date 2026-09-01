"""KnowledgeService: the generic knowledge operations behind the REST and MCP contracts.

Every method resolves the caller's readable scopes from the authenticated principal, so a
consumer never chooses a scope (invariant 4). Reads span the resolved scopes; get_context and
snapshots act on one resolved project; a proposal lands in the caller's personal scope as a
PROPOSED fact with a pending assertion, never a published shared fact (invariant 5).
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from vera import __version__
from vera.adapters.persistence.repositories import (
    SqlAlchemyCanonicalEntityRepository,
    SqlAlchemyCommunityLineageRepository,
    SqlAlchemyProposalAttemptRepository,
)
from vera.adapters.persistence.repositories.fabric import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyEvidenceRepository,
    SqlAlchemyFactRepository,
    SqlAlchemyKnowledgeEventLog,
)
from vera.adapters.persistence.repositories.knowledge_read import SqlAlchemyKnowledgeReadModel
from vera.adapters.persistence.repositories.ontology import SqlAlchemyOntologyRepository
from vera.adapters.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from vera.adapters.persistence.repositories.passage_index import (
    SqlAlchemyCodeIndex,
    SqlAlchemyContentAvailability,
    SqlAlchemyFactCandidateSource,
    SqlAlchemyPassageIndex,
)
from vera.adapters.persistence.repositories.pgvector_index import (
    PgVectorCodeIndex,
    PgVectorHybridFactCandidateSource,
    PgVectorPassageIndex,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.retrieval import (
    ContextAssembler,
    HybridFactCandidateSource,
    HybridPassageIndex,
)
from vera.application.snapshot import (
    ContextPackExpiredError,
    ContextPackService,
    SnapshotService,
    serialize_candidate,
)
from vera.bootstrap import Container
from vera.config.settings import McpToolProfile, active_embedding
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
from vera.domain.ontology.registry import is_single_valued
from vera.domain.ports.identity import ResolvedScope, ScopeResolver
from vera.domain.ports.retrieval_index import (
    CodeIndex,
    FactCandidateSource,
    PassageIndex,
    RetrievalFilters,
)
from vera.domain.ports.snapshot import ContextPack
from vera.domain.repository_identity import canonical_repository_ref
from vera.observability.cost import UsageContext, reset_usage_context, set_usage_context
from vera.observability.metrics import record_search
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import GroupId, JsonDict

_PROPOSAL_AUTHORITY = 0.4  # tier 4 (unverified) authority; proposals never outrank real facts


def active_embedding_version(container: Container) -> JsonDict:
    if not container.settings.memory.vector_search_enabled or container.embedder is None:
        return {}
    model, dimension = active_embedding(container.settings)
    memory = container.settings.memory
    return {
        "provider": memory.embedder,
        "model": model,
        "model_version": memory.embedding_model_version,
        "dimension": dimension,
    }


def active_retrieval_index_version(container: Container) -> str:
    if not container.settings.memory.vector_search_enabled or container.embedder is None:
        return "fts-v1"
    if container.reranker is None:
        return "hybrid-rrf-v1"
    rerank = container.settings.rerank
    model = (
        container.settings.voyage.rerank_model
        if rerank.cross_encoder_provider == "voyage"
        else container.settings.memory.small_llm_model
    )
    fingerprint = hashlib.sha256(
        (
            f"semantic-fact-ann-v1:{rerank.cross_encoder_provider}:{model}:"
            f"{rerank.cross_encoder_min_score}:{rerank.cross_encoder_top_n}"
        ).encode()
    ).hexdigest()[:12]
    return f"hybrid-rrf-v1+semantic-fact-ann-v1:{fingerprint}"


class ScopeError(Exception):
    """The principal has no resolvable scope, or requested a scope it may not access."""


class InputError(Exception):
    """A caller-controlled field failed service-level validation."""

    def __init__(self, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"invalid {field}: {reason}")


def _normalized_ref(value: str | None, *, name: str, lowercase: bool = False) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if len(normalized) > 256 or any(ord(character) < 32 for character in normalized):
        raise InputError(name, "must be a printable string of at most 256 characters")
    return normalized.lower() if lowercase else normalized


def _proposal_context(
    *,
    runtime: str | None,
    session_ref: str | None,
    task_ref: str | None,
    repository_ref: str | None,
) -> JsonDict:
    context: JsonDict = {}
    normalized_runtime = _normalized_ref(runtime, name="runtime", lowercase=True)
    normalized_session = _normalized_ref(session_ref, name="session_ref")
    normalized_task = _normalized_ref(task_ref, name="task_ref")
    repository_input = _normalized_ref(repository_ref, name="repository_ref")
    normalized_repository = canonical_repository_ref(repository_input)
    if repository_input is not None and normalized_repository is None:
        raise InputError("repository_ref", "must be a remote or server-side repository identity")
    if normalized_runtime is not None:
        context["runtime"] = normalized_runtime
    if normalized_session is not None:
        context["session_ref"] = normalized_session
    if normalized_task is not None:
        context["task_ref"] = normalized_task
    if normalized_repository is not None:
        context["repository_ref"] = normalized_repository
    return context


def _proposal_run_key(principal_id: UUID, context: JsonDict) -> str:
    if not context:
        return f"proposal:{principal_id}"
    encoded = json.dumps(context, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode()).hexdigest()[:32]
    return f"proposal:{principal_id}:{digest}"


def _context_pack_payload(pack: ContextPack) -> JsonDict:
    return {
        "pack_id": pack.id,
        "persisted": pack.id is not None,
        "scope_id": pack.group_id,
        "snapshot_id": pack.snapshot_id,
        "query": pack.query,
        "created_at": pack.created_at.isoformat(),
        "expires_at": pack.expires_at.isoformat(),
        "request_hash": pack.request_hash,
        "result_references": pack.result_references,
        "assembler_version": pack.assembler_version,
        "request": pack.request,
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
        sm = container.reads
        self._read = SqlAlchemyKnowledgeReadModel(sm)
        self._uow_factory = lambda: SqlAlchemyUnitOfWork(container.sessionmaker)
        self._community_lineage = SqlAlchemyCommunityLineageRepository(sm)
        facts: FactCandidateSource = SqlAlchemyFactCandidateSource(sm)
        passages: PassageIndex
        code: CodeIndex
        if container.settings.memory.vector_search_enabled and container.embedder is not None:
            model, dimension = active_embedding(container.settings)
            passages = HybridPassageIndex(
                SqlAlchemyPassageIndex(sm),
                PgVectorPassageIndex(
                    sm,
                    container.embedder,
                    provider=container.settings.memory.embedder,
                    model=model,
                    model_version=container.settings.memory.embedding_model_version,
                    dimension=dimension,
                ),
            )
            if container.reranker is not None:
                hydrator = SqlAlchemyFactCandidateSource(sm)
                facts = HybridFactCandidateSource(
                    batch_source=PgVectorHybridFactCandidateSource(
                        sm,
                        container.embedder,
                        container.reranker,
                        provider=container.settings.memory.embedder,
                        model=model,
                        model_version=container.settings.memory.embedding_model_version,
                        dimension=dimension,
                        min_score=container.settings.rerank.cross_encoder_min_score,
                        top_n=container.settings.rerank.cross_encoder_top_n,
                        include_provenance=False,
                        semantic_semaphore=container.fact_candidate_semaphore,
                        exact_semaphore=container.exact_fact_candidate_semaphore,
                    ),
                    hydrator=hydrator,
                )
            code = HybridPassageIndex(
                SqlAlchemyCodeIndex(sm),
                PgVectorCodeIndex(
                    sm,
                    container.embedder,
                    provider=container.settings.memory.embedder,
                    model=model,
                    model_version=container.settings.memory.embedding_model_version,
                    dimension=dimension,
                ),
            )
        else:
            passages = SqlAlchemyPassageIndex(sm)
            code = SqlAlchemyCodeIndex(sm)
        self._assembler = ContextAssembler(
            facts=facts,
            passages=passages,
            code=code,
            content_availability=SqlAlchemyContentAvailability(sm),
        )
        self._snapshot_service = SnapshotService(uow_factory=self._uow_factory)
        self._context_pack_service = ContextPackService(
            assembler=self._assembler,
            uow_factory=self._uow_factory,
            max_persisted_per_group=(container.settings.knowledge.context_pack_quota_per_scope),
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

    async def bootstrap(
        self,
        principal_id: UUID,
        *,
        auth_profile: Literal["local-dev", "remote-authenticated"],
        repository: str | None = None,
        branch: str | None = None,
        capability_classes: tuple[str, ...] = (
            "read",
            "personal-proposal",
            "feedback",
            "snapshot",
        ),
        tool_profile: McpToolProfile = "coding",
    ) -> JsonDict:
        """Describe the caller's safe operating context without returning knowledge content."""
        scope = await self._resolve(principal_id)
        async with SqlAlchemyUnitOfWork(self._c.sessionmaker) as uow:
            principal = await uow.identity.get_principal(principal_id)
        if principal is None:  # scope resolution and identity lookup must agree
            raise ScopeError(f"principal {principal_id} was not found")

        rows = await self._read.list_projects(group_ids=list(scope.group_ids))
        projects: list[JsonDict] = []
        repository_matches: list[JsonDict] = []
        canonical_repository = canonical_repository_ref(repository)
        for row in rows:
            project: JsonDict = {
                "project_id": row["project_id"],
                "slug": row["slug"],
                "name": row["name"],
                "scope_id": row["group_id"],
                "workspace_id": row["workspace_id"],
                "workspace_slug": row["workspace_slug"],
                "workspace_name": row["workspace_name"],
            }
            projects.append(project)
            known_repositories = {
                normalized
                for value in row["repositories"]
                if (normalized := canonical_repository_ref(str(value))) is not None
            }
            if canonical_repository is not None and canonical_repository in known_repositories:
                repository_matches.append(project)

        if not projects:
            resolution = "personal_only"
            candidates: list[JsonDict] = []
        elif repository is not None and canonical_repository is None:
            resolution = "unsupported_repository"
            candidates = projects
        elif canonical_repository is not None and len(repository_matches) == 1:
            resolution = "selected"
            candidates = repository_matches
        elif canonical_repository is not None and len(repository_matches) > 1:
            resolution = "selection_required"
            candidates = repository_matches
        elif canonical_repository is not None:
            resolution = "unmapped"
            candidates = projects
        elif len(projects) == 1:
            resolution = "selected"
            candidates = projects
        else:
            resolution = "selection_required"
            candidates = projects

        selected = candidates[0] if resolution == "selected" else None
        ontology = current_descriptor()
        granted_classes = set(capability_classes)
        return {
            "server_version": __version__,
            "principal": {
                "id": str(principal.id),
                "kind": principal.kind.value,
                "display_name": principal.display_name,
            },
            "auth_profile": auth_profile,
            "capability_classes": list(capability_classes),
            "tool_profile": {
                "active": tool_profile,
                "advanced_capabilities": tool_profile in {"advanced", "compatibility"},
                "legacy_aliases": tool_profile == "compatibility",
            },
            "shared_context_available": bool(projects),
            "projects": projects,
            "project_resolution": {
                "status": resolution,
                "repository": canonical_repository,
                "branch": branch.strip() if branch and branch.strip() else None,
                "selected": selected,
                "candidates": candidates,
            },
            "selection_policy": {
                "request_scope": "one explicit project per request",
                "monorepo": "select a project when one repository maps to multiple projects",
                "multi_root": "resolve each repository root independently",
            },
            "write_policy": {
                "personal_proposals": "personal-proposal" in granted_classes,
                "personal_feedback": "feedback" in granted_classes,
                "snapshots": "snapshot" in granted_classes,
                "save_mode_default": "suggest",
                "save_modes": [
                    "off",
                    "suggest",
                    *(["auto-propose"] if "personal-proposal" in granted_classes else []),
                ],
                "proposal_approval_enforced_by": "runtime",
                "auto_propose_requires_user_opt_in": True,
                "shared_writes": "reviewed-only",
                "auto_publish": False,
            },
            "contract_versions": {
                "bootstrap": 1,
                "knowledge_api": "v2",
                "ontology": ontology.version,
            },
        }

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
        usage_ref: str | None = None,
        persist: bool = False,
    ) -> JsonDict:
        scope = await self._resolve(principal_id)
        group = await self._target_group(scope, project)
        embedding_version = active_embedding_version(self._c)
        retrieval_index_version = active_retrieval_index_version(self._c)
        repository_ref = canonical_repository_ref(repository)
        if repository is not None and repository.strip() and repository_ref is None:
            raise InputError("repository", "must be a remote or server-side repository identity")
        filters = RetrievalFilters(
            repository=repository_ref,
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
        usage_token = set_usage_context(
            UsageContext(request_kind="search", group_id=group, ref=usage_ref)
        )
        try:
            pack = await self._context_pack_service.create(
                group_id=group,
                query=query,
                snapshot_id=snapshot_id,
                hints=hints,
                limit=limit,
                token_budget=token_budget,
                as_of=as_of,
                filters=filters,
                citation_mode=citation_mode,
                active_embedding_version=embedding_version,
                active_retrieval_index_version=retrieval_index_version,
                actor=str(principal_id),
                persist=persist,
            )
        finally:
            reset_usage_context(usage_token)
        return _context_pack_payload(pack)

    async def get_context_pack(self, principal_id: UUID, *, pack_id: str) -> JsonDict | None:
        try:
            pack_id = str(UUID(pack_id))
        except ValueError:
            return None
        scope = await self._resolve(principal_id)
        for group_id in scope.group_ids:
            pack = await self._context_pack_service.get(group_id=group_id, pack_id=pack_id)
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
        known_as_of: datetime | None = None,
    ) -> JsonDict:
        scope = await self._resolve(principal_id)
        group = await self._target_group(scope, project)
        usage_token = set_usage_context(UsageContext(request_kind="search", group_id=group))
        started = time.perf_counter()
        result_count = 0
        try:
            assembled = await self._assembler.assemble(
                query=query,
                group_id=group,
                limit=limit,
                as_of=as_of,
                known_as_of=known_as_of,
            )
            result_count = len(assembled.results)
        finally:
            reset_usage_context(usage_token)
            record_search(duration_s=time.perf_counter() - started, hits=result_count)
        return {
            "query": query,
            "conflicts": assembled.conflicts,
            "freshness_warnings": assembled.freshness_warnings,
            "omitted": assembled.omitted,
            "results": [
                serialize_candidate(result, citation_mode="full") for result in assembled.results
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
        context_pack_id: str,
        result_ref: str,
        signal: str,
    ) -> JsonDict:
        """Record feedback using only attribution captured in a persisted context pack."""
        if signal not in {"up", "down"}:
            raise InputError("signal", "must be 'up' or 'down'")
        try:
            pack_uuid = UUID(context_pack_id)
        except ValueError as exc:
            raise InputError("context_pack_id", "must identify a persisted context pack") from exc
        pack = await self.get_context_pack(principal_id, pack_id=str(pack_uuid))
        if pack is None or not pack["persisted"]:
            raise ContextPackExpiredError("context pack is unavailable to the caller")
        query = str(pack["query"])
        rank: int | None = None
        signals: JsonDict | None = None
        if result_ref != str(pack_uuid):
            shown = next(
                (
                    (index, result)
                    for index, result in enumerate(pack["results"], start=1)
                    if str(result["ref"]) == result_ref
                ),
                None,
            )
            if shown is None:
                raise InputError("result_ref", "was not shown in the attributed context pack")
            rank, result = shown
            signals = dict(result.get("signals") or {})
        scope = await self._resolve(principal_id)
        group = scope.personal_group_id
        async with SqlAlchemyUnitOfWork(self._c.sessionmaker) as uow:
            await uow.use_tenant(group)
            await uow.feedback.lock_attribution(
                principal_id=principal_id,
                context_pack_id=pack_uuid,
                result_ref=result_ref,
            )
            existing_signal = await uow.feedback.attributed_signal(
                principal_id=principal_id,
                context_pack_id=pack_uuid,
                result_ref=result_ref,
            )
            if existing_signal is not None:
                return {
                    "status": "deduplicated",
                    "context_pack_id": str(pack_uuid),
                    "result_ref": result_ref,
                    "signal": existing_signal,
                    "requested_signal": signal,
                    "query": query,
                    "rank": rank,
                    "signals": signals,
                }
            await uow.feedback.record(
                group_id=group,
                principal_id=principal_id,
                query=query,
                result_ref=result_ref,
                context_pack_id=pack_uuid,
                signal=signal,
                signals=signals,
                rank=rank,
            )
            await uow.commit()
        return {
            "status": "recorded",
            "context_pack_id": str(pack_uuid),
            "result_ref": result_ref,
            "signal": signal,
            "query": query,
            "rank": rank,
            "signals": signals,
        }

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
        embedding_version = active_embedding_version(self._c)
        retrieval_index_version = active_retrieval_index_version(self._c)
        snap = await self._snapshot_service.create(
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
            "assembler_version": snap.assembler_version,
            "graph_projection_checkpoint": snap.graph_projection_checkpoint,
            "policy_version": snap.policy_version,
            "ontology_version_id": snap.ontology_version_id,
            "retrieval_frozen": snap.retrieval_frozen,
        }

    async def get_snapshot(self, principal_id: UUID, *, snapshot_id: str) -> JsonDict | None:
        try:
            snapshot_id = str(UUID(snapshot_id))
        except ValueError:
            return None
        scope = await self._resolve(principal_id)
        for group in scope.group_ids:
            snap = await self._snapshot_service.get(group_id=group, snapshot_id=snapshot_id)
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
                    "assembler_version": snap.assembler_version,
                    "graph_projection_checkpoint": snap.graph_projection_checkpoint,
                    "policy_version": snap.policy_version,
                    "ontology_version_id": snap.ontology_version_id,
                    "retrieval_frozen": snap.retrieval_frozen,
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
        runtime: str | None = None,
        session_ref: str | None = None,
        task_ref: str | None = None,
        repository_ref: str | None = None,
    ) -> JsonDict:
        base_context = _proposal_context(
            runtime=runtime,
            session_ref=session_ref,
            task_ref=task_ref,
            repository_ref=None,
        )
        try:
            proposal_context = _proposal_context(
                runtime=runtime,
                session_ref=session_ref,
                task_ref=task_ref,
                repository_ref=repository_ref,
            )
        except InputError as exc:
            scope = await self._resolve(principal_id)
            await self._record_proposal_rejection(
                principal_id=principal_id,
                group_id=scope.personal_group_id,
                run_key=_proposal_run_key(principal_id, base_context),
                context=base_context,
                reason=str(exc),
            )
            raise
        run_key = _proposal_run_key(principal_id, proposal_context)
        scope = await self._resolve(principal_id)
        group = scope.personal_group_id  # proposals are always personal, never shared truth
        if not subject or len(subject) > 512:
            await self._record_proposal_rejection(
                principal_id=principal_id,
                group_id=group,
                run_key=run_key,
                context=proposal_context,
                reason="subject length must be 1..512",
            )
            raise InputError("subject", "length must be 1..512")
        if not predicate or len(predicate) > 2048:
            await self._record_proposal_rejection(
                principal_id=principal_id,
                group_id=group,
                run_key=run_key,
                context=proposal_context,
                reason="predicate length must be 1..2048",
            )
            raise InputError("predicate", "length must be 1..2048")
        if not object or len(object) > 2048:
            await self._record_proposal_rejection(
                principal_id=principal_id,
                group_id=group,
                run_key=run_key,
                context=proposal_context,
                reason="object length must be 1..2048",
            )
            raise InputError("object", "length must be 1..2048")
        predicate = predicate.strip().upper()
        allowed_predicates = {
            policy.predicate for policy in current_descriptor().predicate_policies
        }
        if predicate not in allowed_predicates:
            await self._record_proposal_rejection(
                principal_id=principal_id,
                group_id=group,
                run_key=run_key,
                context=proposal_context,
                reason="predicate is not in the active ontology",
            )
            raise InputError("predicate", "is not in the active ontology")
        if (
            evidence_text is not None
            and len(evidence_text) > self._c.settings.knowledge.proposal_evidence_max_chars
        ):
            await self._record_proposal_rejection(
                principal_id=principal_id,
                group_id=group,
                run_key=run_key,
                context=proposal_context,
                reason="evidence_text exceeds the configured proposal evidence size limit",
            )
            raise InputError("evidence_text", "exceeds the configured proposal evidence size limit")
        async with SqlAlchemyUnitOfWork(self._c.sessionmaker) as uow:
            await uow.use_tenant(group)
            session = uow.session
            attempts = SqlAlchemyProposalAttemptRepository(session)
            assertions = SqlAlchemyAssertionRepository(session)
            has_bounded_run = "task_ref" in proposal_context or "session_ref" in proposal_context
            quota_since = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
            if not has_bounded_run:
                await assertions.lock_run_key(
                    group_id=group,
                    run_key=f"proposal-daily:{principal_id}:{quota_since.date().isoformat()}",
                )
            await assertions.lock_run_key(group_id=group, run_key=run_key)
            proposal_count = (
                await assertions.count_for_run_key(group_id=group, run_key=run_key)
                if has_bounded_run
                else await attempts.count_created_since(
                    group_id=group,
                    principal_id=principal_id,
                    since=quota_since,
                )
            )
            canonical = SqlAlchemyCanonicalEntityRepository(session)
            await canonical.lock_name(group_id=group, name=subject)
            subject_entity = await canonical.resolve(group_id=group, name=subject)
            if (
                subject_entity is None
                and proposal_count >= self._c.settings.knowledge.proposals_per_task
            ):
                await attempts.record(
                    group_id=group,
                    principal_id=principal_id,
                    run_key=run_key,
                    fact_key=None,
                    proposal_ref=None,
                    outcome="skipped",
                    operation="skipped",
                    context=proposal_context,
                    detail={
                        "reason": "task proposal limit reached",
                        "subject": subject,
                        "predicate": predicate,
                        "object": object,
                    },
                )
                await uow.commit()
                return {
                    "status": "skipped",
                    "operation": "skipped",
                    "fact_key": None,
                    "lifecycle": "not_created",
                    "group_id": group,
                    "reason": "the configured proposal limit for this task has been reached",
                    "proposal_context": proposal_context,
                }
            if subject_entity is None:
                subject_entity = await canonical.create(
                    group_id=group, entity_type="Entity", canonical_name=subject, aliases=[]
                )
            fk = fact_key(
                scope=group,
                subject_entity_id=subject_entity.id,
                predicate=predicate,
                object_scalar=object,
                qualifiers=qualifiers,
            )
            fact_slot_key = slot_key(
                scope=group,
                subject_entity_id=subject_entity.id,
                predicate=predicate,
                qualifiers=qualifiers,
            )
            facts = SqlAlchemyFactRepository(session)
            if is_single_valued(predicate):
                await facts.lock_fact_key(group_id=group, fact_key=f"slot:{fact_slot_key}")
            await facts.lock_fact_key(group_id=group, fact_key=fk)
            if is_single_valued(predicate):
                conflicts = [
                    conflict
                    for conflict in await facts.live_by_slot_key(
                        group_id=group, slot_key=fact_slot_key
                    )
                    if conflict.fact_key != fk
                ]
                if conflicts:
                    conflict_keys = [conflict.fact_key for conflict in conflicts]
                    await attempts.record(
                        group_id=group,
                        principal_id=principal_id,
                        run_key=run_key,
                        fact_key=fk,
                        proposal_ref=None,
                        outcome="conflicted",
                        operation="skipped",
                        context=proposal_context,
                        detail={"conflicts": conflict_keys},
                    )
                    await uow.commit()
                    return {
                        "status": "conflicted",
                        "operation": "skipped",
                        "fact_key": fk,
                        "lifecycle": "not_created",
                        "group_id": group,
                        "conflicts": conflict_keys,
                        "proposal_context": proposal_context,
                    }
            existing = await facts.by_fact_key(group_id=group, fact_key=fk)
            if existing is not None and existing.lifecycle_state is not FactLifecycle.PROPOSED:
                await attempts.record(
                    group_id=group,
                    principal_id=principal_id,
                    run_key=run_key,
                    fact_key=fk,
                    proposal_ref=None,
                    outcome="conflicted",
                    operation="skipped",
                    context=proposal_context,
                    detail={"lifecycle": existing.lifecycle_state.value},
                )
                await uow.commit()
                return {
                    "status": "conflicted",
                    "operation": "skipped",
                    "fact_key": fk,
                    "lifecycle": existing.lifecycle_state.value,
                    "group_id": group,
                    "proposal_context": proposal_context,
                }
            existing_assertion = (
                await assertions.for_fact_and_run_key(
                    group_id=group, fact_id=str(existing.id), run_key=run_key
                )
                if existing is not None
                else None
            )
            if (
                existing_assertion is None
                and proposal_count >= self._c.settings.knowledge.proposals_per_task
            ):
                await attempts.record(
                    group_id=group,
                    principal_id=principal_id,
                    run_key=run_key,
                    fact_key=fk,
                    proposal_ref=None,
                    outcome="skipped",
                    operation="skipped",
                    context=proposal_context,
                    detail={"reason": "task proposal limit reached"},
                )
                await uow.commit()
                return {
                    "status": "skipped",
                    "operation": "skipped",
                    "fact_key": fk,
                    "lifecycle": "not_created",
                    "group_id": group,
                    "reason": "the configured proposal limit for this task has been reached",
                    "proposal_context": proposal_context,
                }
            if existing is None:
                fact = await facts.upsert(
                    Fact(
                        id=uuid7(),
                        group_id=group,
                        fact_key=fk,
                        slot_key=fact_slot_key,
                        subject_entity_id=subject_entity.id,
                        predicate=predicate,
                        object_type=ObjectType.SCALAR,
                        normalized_object=normalize_object(object_scalar=object),
                        object_scalar=object,
                        qualifiers=dict(qualifiers or {}),
                        lifecycle_state=FactLifecycle.PROPOSED,
                        authority=_PROPOSAL_AUTHORITY,
                        confidence=0.5,
                    )
                )
            else:
                fact = existing
            assertion, created = await assertions.create_proposal_if_absent(
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
                    run_key=run_key,
                )
            )
            if created and evidence_text:
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
            if created:
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
            await attempts.record(
                group_id=group,
                principal_id=principal_id,
                run_key=run_key,
                fact_key=fk,
                proposal_ref=assertion.id,
                outcome="created" if created else "deduplicated",
                operation="created" if created else "deduplicated",
                context=proposal_context,
            )
            await uow.commit()
        return {
            "status": "proposed",
            "operation": "created" if created else "deduplicated",
            "proposal_ref": str(assertion.id),
            "fact_key": fk,
            "lifecycle": "proposed",
            "group_id": group,
            "proposal_context": proposal_context,
        }

    async def _record_proposal_rejection(
        self,
        *,
        principal_id: UUID,
        group_id: str,
        run_key: str,
        context: JsonDict,
        reason: str,
    ) -> None:
        async with SqlAlchemyUnitOfWork(self._c.sessionmaker) as uow:
            await uow.use_tenant(group_id)
            await SqlAlchemyProposalAttemptRepository(uow.session).record(
                group_id=group_id,
                principal_id=principal_id,
                run_key=run_key,
                fact_key=None,
                proposal_ref=None,
                outcome="rejected",
                operation="rejected",
                context=context,
                detail={"reason": reason},
            )
            await uow.commit()

    async def retract_proposal(self, principal_id: UUID, *, fact_key: str) -> JsonDict:
        scope = await self._resolve(principal_id)
        group = scope.personal_group_id
        async with SqlAlchemyUnitOfWork(self._c.sessionmaker) as uow:
            await uow.use_tenant(group)
            session = uow.session
            facts = SqlAlchemyFactRepository(session)
            await facts.lock_fact_key(group_id=group, fact_key=fact_key)
            fact = await facts.by_fact_key_for_update(group_id=group, fact_key=fact_key)
            if fact is None:
                raise ScopeError("proposal not found in the caller's personal scope")
            if fact.lifecycle_state is FactLifecycle.RETRACTED:
                return {
                    "status": "retracted",
                    "operation": "already_retracted",
                    "fact_key": fact_key,
                    "group_id": group,
                }
            if fact.lifecycle_state is not FactLifecycle.PROPOSED:
                raise ScopeError("only a pending personal proposal can be retracted by its caller")

            assertions = await SqlAlchemyAssertionRepository(session).withdraw_for_fact(
                group_id=group, fact_id=str(fact.id)
            )
            events = SqlAlchemyKnowledgeEventLog(session)
            for assertion in assertions:
                await events.append(
                    KnowledgeEvent(
                        id=uuid7(),
                        group_id=group,
                        event_type=KnowledgeEventType.ASSERTION_WITHDRAWN,
                        occurred_at=utc_now(),
                        actor=str(principal_id),
                        fact_id=fact.id,
                        assertion_id=assertion.id,
                        previous_state={"state": "active"},
                        next_state={"state": "withdrawn"},
                        reason="proposal retracted by caller",
                    )
                )
            await facts.set_lifecycle(
                group_id=group,
                fact_id=str(fact.id),
                state=FactLifecycle.RETRACTED,
            )
            await events.append(
                KnowledgeEvent(
                    id=uuid7(),
                    group_id=group,
                    event_type=KnowledgeEventType.FACT_RETRACTED,
                    occurred_at=utc_now(),
                    actor=str(principal_id),
                    fact_id=fact.id,
                    previous_state={"lifecycle": FactLifecycle.PROPOSED.value},
                    next_state={"lifecycle": FactLifecycle.RETRACTED.value},
                    reason="proposal retracted by caller",
                )
            )
            await uow.commit()
        return {
            "status": "retracted",
            "operation": "retracted",
            "fact_key": fact_key,
            "group_id": group,
        }

    async def proposal_report(
        self,
        principal_id: UUID,
        *,
        runtime: str | None = None,
        session_ref: str | None = None,
        task_ref: str | None = None,
        repository_ref: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> JsonDict:
        proposal_context = _proposal_context(
            runtime=runtime,
            session_ref=session_ref,
            task_ref=task_ref,
            repository_ref=repository_ref,
        )
        if not proposal_context:
            raise InputError(
                "proposal_context", "must include a runtime, session, task, or repository reference"
            )
        if not 1 <= limit <= 100:
            raise InputError("limit", "must be between 1 and 100")
        try:
            cursor_id = UUID(cursor) if cursor else None
        except ValueError as exc:
            raise InputError("cursor", "must identify a proposal attempt") from exc
        scope = await self._resolve(principal_id)
        report = await self._read.proposal_report(
            group_id=scope.personal_group_id,
            context=proposal_context,
            cursor=cursor_id,
            limit=limit,
        )
        proposals: list[JsonDict] = []
        for row in report["rows"]:
            outcome = str(row["outcome"])
            lifecycle = row["lifecycle_state"]
            current_state = (
                (
                    "accepted"
                    if lifecycle == FactLifecycle.ACTIVE.value
                    else "rejected"
                    if lifecycle == FactLifecycle.RETRACTED.value
                    else "pending"
                )
                if lifecycle is not None
                else None
            )
            proposals.append(
                {
                    "attempt_ref": row["attempt_ref"],
                    "proposal_ref": row["proposal_ref"],
                    "fact_key": row["fact_key"],
                    "predicate": row["predicate"],
                    "object": row["object"],
                    "outcome": outcome,
                    "operation": row["operation"],
                    "current_state": current_state,
                    "proposal_context": dict(row["context"]),
                    "detail": dict(row["detail"]),
                    "recorded_at": row["created_at"].isoformat(),
                }
            )
        return {
            "proposal_context": proposal_context,
            "counts": dict(report["counts"]),
            "states": dict(report["states"]),
            "proposals": proposals,
            "next_cursor": report["next_cursor"],
        }

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
            facts = SqlAlchemyFactRepository(session)
            await facts.lock_fact_key(group_id=group, fact_key=fact_key)
            current = await facts.by_fact_key_for_update(group_id=group, fact_key=fact_key)
            if current is None or current.lifecycle_state is not FactLifecycle.PROPOSED:
                raise ScopeError("fact is not awaiting review")
            await facts.set_lifecycle(group_id=group, fact_id=str(current.id), state=to)
            await SqlAlchemyKnowledgeEventLog(session).append(
                KnowledgeEvent(
                    id=uuid7(),
                    group_id=group,
                    event_type=event,
                    occurred_at=utc_now(),
                    actor=str(principal_id),
                    fact_id=current.id,
                    reason=reason,
                )
            )
            if (
                to is FactLifecycle.ACTIVE
                and self._c.settings.memory.vector_search_enabled
                and self._c.embedder is not None
            ):
                await SqlAlchemyOutboxRepository(session).add(
                    group_id=group,
                    source_id=f"fact-activation:{current.id}",
                    dedup_uuid=uuid7(),
                    payload={"job_kind": "embed_facts", "group_id": group},
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
