"""The service behind the MCP tools.

Every method resolves the caller's allowed group_ids from its principal, so a client
can never choose a scope. Reads run against those scopes; a proposal lands in the
caller's personal scope as an unverified, tier-4 claim (never auto-published).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.application.queries.search_memory import SearchMemory, SearchMemoryHandler
from vera.bootstrap import Container, build_rerank_weights
from vera.domain.ports.identity import ResolvedScope, ScopeResolver
from vera.shared.errors import is_ok
from vera.shared.ids import uuid7
from vera.shared.types import GroupId


class ScopeError(Exception):
    """The principal has no resolvable scope."""


class VeraMcpService:
    def __init__(self, container: Container, scope_resolver: ScopeResolver) -> None:
        self._container = container
        self._scopes = scope_resolver
        self._search = SearchMemoryHandler(
            container.memory,
            container.retrieval_read,
            weights=build_rerank_weights(container.settings),
        )

    async def _resolve(self, principal_id: UUID) -> ResolvedScope:
        scope = await self._scopes.resolve(principal_id)
        if scope is None:
            raise ScopeError(f"principal {principal_id} has no scope")
        return scope

    async def search(
        self, principal_id: UUID, *, query: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        scope = await self._resolve(principal_id)
        hits = await self._search.handle(
            SearchMemory(
                text=query, group_ids=tuple(GroupId(g) for g in scope.group_ids), limit=limit
            )
        )
        return [
            {
                "fact": h.fact,
                "score": h.score,
                "source_id": h.source_id,
                "verification": h.verification,
                "authority": h.authority,
                "valid_at": h.valid_at.isoformat() if h.valid_at else None,
                # Echoed back on feedback so the vote can be tied to the signals shown.
                "signals": dict(h.signals),
            }
            for h in hits
        ]

    async def get_context(
        self, principal_id: UUID, *, query: str, limit: int = 5
    ) -> list[dict[str, Any]]:
        return await self.search(principal_id, query=query, limit=limit)

    async def recent_changes(self, principal_id: UUID, *, limit: int = 20) -> list[dict[str, Any]]:
        scope = await self._resolve(principal_id)
        changes = await self._container.retrieval_read.recent_changes(
            group_ids=list(scope.group_ids), limit=limit
        )
        return [
            {
                "source_id": c.source_id,
                "knowledge_type": c.knowledge_type,
                "verification": c.verification,
                "reference_time": c.reference_time.isoformat(),
            }
            for c in changes
        ]

    async def get_source(self, principal_id: UUID, *, source_id: str) -> dict[str, Any] | None:
        scope = await self._resolve(principal_id)
        prov = await self._container.retrieval_read.get_source(
            group_ids=list(scope.group_ids), source_id=source_id
        )
        if prov is None:
            return None
        return {
            "source_id": prov.source_id,
            "knowledge_type": prov.knowledge_type,
            "verification": prov.verification,
            "authority": prov.authority,
            "reference_time": prov.reference_time.isoformat(),
            "payload": prov.payload,
        }

    async def explain(self, principal_id: UUID, *, query: str) -> list[dict[str, Any]]:
        # Explaining a fact is a search that returns provenance for the top matches.
        return await self.search(principal_id, query=query, limit=3)

    async def propose(
        self,
        principal_id: UUID,
        *,
        subject: str,
        predicate: str,
        obj: str,
    ) -> dict[str, Any]:
        scope = await self._resolve(principal_id)
        if scope.primary_workspace_id is None:
            return {"status": "rejected", "reason": "no workspace to attach the proposal to"}
        async with SqlAlchemyUnitOfWork(self._container.sessionmaker) as uow:
            await uow.use_tenant(scope.personal_group_id)
            source_id = await uow.sources.get_or_create_agent(
                workspace_id=scope.primary_workspace_id
            )
            service = CurationService(
                uow, self._container.extractor, self._container.object_store, self._container.judge
            )
            result = await service.ingest_artifact(
                IngestArtifact(
                    source_id=source_id,
                    group_id=scope.personal_group_id,
                    external_id=f"agent:{uuid7().hex}",
                    body="",
                    knowledge_type="fact_triple",
                    metadata={
                        "triples": [{"subject": subject, "predicate": predicate, "object": obj}]
                    },
                )
            )
            await uow.commit()
        claim_ids = result.value.claim_ids if is_ok(result) else ()
        return {"status": "proposed", "claim_ids": list(claim_ids)}

    async def feedback(
        self,
        principal_id: UUID,
        *,
        result_ref: str,
        signal: str,
        query: str = "",
        signals: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        if signal not in {"up", "down"}:
            return {"status": "rejected", "reason": "signal must be 'up' or 'down'"}
        scope = await self._resolve(principal_id)
        async with SqlAlchemyUnitOfWork(self._container.sessionmaker) as uow:
            await uow.use_tenant(scope.personal_group_id)
            await uow.feedback.record(
                group_id=scope.personal_group_id,
                principal_id=principal_id,
                query=query,
                result_ref=result_ref,
                signal=signal,
                # The signal vector search returned for this result, for later calibration.
                signals=signals,
            )
            await uow.commit()
        return {"status": "recorded"}
