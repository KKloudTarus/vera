"""Versioned generic knowledge contracts (`/v2/knowledge`).

The consumer-neutral surface: context assembly, search, fact explanation, change feed,
conflicts, snapshots, and proposals, all over the authoritative fact model. The existing
`/memory` endpoints stay for backward compatibility. A client never chooses a scope; the
server resolves it from the authenticated principal (invariant 4), and a proposal is only ever
a personal-scope proposal, never a published shared fact (invariant 5).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from vera.application.knowledge import ScopeError
from vera.entrypoints.api.deps import KnowledgeServiceDep, PrincipalDep

router = APIRouter(prefix="/v2/knowledge", tags=["knowledge"])


def _forbidden(exc: ScopeError) -> HTTPException:
    return HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


class ContextRequest(BaseModel):
    query: str = Field(min_length=1)
    project: str | None = None  # a hint, validated against the caller's resolved scopes
    snapshot_id: str | None = None
    limit: int = Field(default=10, ge=1, le=50)
    token_budget: int = Field(default=2000, ge=100, le=32000)
    as_of: datetime | None = None
    hints: dict[str, Any] = Field(default_factory=dict)


class SearchRequest(BaseModel):
    query: str = Field(min_length=1)
    project: str | None = None
    limit: int = Field(default=10, ge=1, le=50)
    as_of: datetime | None = None


class ProposeRequest(BaseModel):
    subject: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    object: str = Field(min_length=1)
    qualifiers: dict[str, Any] = Field(default_factory=dict)
    evidence_text: str | None = None


class SnapshotRequest(BaseModel):
    project: str | None = None
    as_of: datetime | None = None


@router.post("/context", summary="Assemble a bounded, cited context pack (primary tool)")
async def get_context(
    req: ContextRequest, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    try:
        return await service.get_context(
            principal.id,
            query=req.query,
            project=req.project,
            snapshot_id=req.snapshot_id,
            limit=req.limit,
            token_budget=req.token_budget,
            as_of=req.as_of,
            hints=req.hints,
        )
    except ScopeError as exc:
        raise _forbidden(exc) from exc


@router.post("/search", summary="Combined, cited search (no persisted pack)")
async def search(
    req: SearchRequest, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    try:
        return await service.search(
            principal.id, query=req.query, project=req.project, limit=req.limit, as_of=req.as_of
        )
    except ScopeError as exc:
        raise _forbidden(exc) from exc


@router.get("/facts/{fact_key}", summary="A fact and its relations")
async def get_fact(
    fact_key: str, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    fact = await service.get_fact(principal.id, fact_key=fact_key)
    if fact is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="fact not found")
    return fact


@router.get("/facts/{fact_key}/explain", summary="A fact with its assertions and evidence")
async def explain_fact(
    fact_key: str, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    fact = await service.explain_fact(principal.id, fact_key=fact_key)
    if fact is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="fact not found")
    return fact


@router.get("/changes", summary="The semantic change feed")
async def get_changes(
    principal: PrincipalDep,
    service: KnowledgeServiceDep,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict[str, Any]]:
    return await service.get_changes(principal.id, limit=limit)


@router.get("/conflicts", summary="Disputed facts")
async def get_conflicts(
    principal: PrincipalDep,
    service: KnowledgeServiceDep,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict[str, Any]]:
    return await service.get_conflicts(principal.id, limit=limit)


@router.post("/snapshots", summary="Create an immutable snapshot")
async def create_snapshot(
    req: SnapshotRequest, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    try:
        return await service.create_snapshot(principal.id, project=req.project, as_of=req.as_of)
    except ScopeError as exc:
        raise _forbidden(exc) from exc


@router.get("/snapshots/{snapshot_id}", summary="Get a snapshot's metadata")
async def get_snapshot(
    snapshot_id: str, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    snap = await service.get_snapshot(principal.id, snapshot_id=snapshot_id)
    if snap is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="snapshot not found")
    return snap


@router.post("/propose", summary="Propose knowledge (personal scope, never published)")
async def propose(
    req: ProposeRequest, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    try:
        return await service.propose(
            principal.id,
            subject=req.subject,
            predicate=req.predicate,
            object=req.object,
            qualifiers=req.qualifiers,
            evidence_text=req.evidence_text,
        )
    except ScopeError as exc:
        raise _forbidden(exc) from exc


# --- Governance and administration (backend for a future Knowledge Workbench, section 14) ---


@router.get("/facts/{fact_key}/timeline", summary="A fact's semantic history")
async def fact_timeline(
    fact_key: str, principal: PrincipalDep, service: KnowledgeServiceDep
) -> list[dict[str, Any]]:
    return await service.fact_timeline(principal.id, fact_key=fact_key)


@router.get("/ontology", summary="Predicate governance policies and the ontology version")
async def ontology(principal: PrincipalDep, service: KnowledgeServiceDep) -> dict[str, Any]:
    return service.ontology()


@router.get("/review", summary="The review queue: proposed facts awaiting a decision")
async def review_queue(
    principal: PrincipalDep,
    service: KnowledgeServiceDep,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict[str, Any]]:
    return await service.review_queue(principal.id, limit=limit)


@router.post("/review/{fact_key}/promote", summary="Promote a proposed fact to active (admin)")
async def promote_fact(
    fact_key: str, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    try:
        return await service.promote_fact(principal.id, fact_key=fact_key)
    except ScopeError as exc:
        raise _forbidden(exc) from exc


@router.post("/review/{fact_key}/reject", summary="Reject a proposed fact (admin)")
async def reject_fact(
    fact_key: str, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    try:
        return await service.reject_fact(principal.id, fact_key=fact_key)
    except ScopeError as exc:
        raise _forbidden(exc) from exc
