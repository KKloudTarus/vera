"""Versioned generic knowledge contracts (`/v2/knowledge`).

The consumer-neutral surface: context assembly, search, fact explanation, change feed,
conflicts, snapshots, and proposals, all over the authoritative fact model. The existing
`/memory` endpoints stay for backward compatibility. A client never chooses a scope; the
server resolves it from the authenticated principal (invariant 4), and a proposal is only ever
a personal-scope proposal, never a published shared fact (invariant 5).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import AwareDatetime, BaseModel, Field

from vera.application.snapshot import SnapshotNotFoundError, SnapshotNotReproducibleError
from vera.entrypoints.api.deps import KnowledgeServiceDep, PrincipalDep
from vera.entrypoints.knowledge import ScopeError

router = APIRouter(prefix="/v2/knowledge", tags=["knowledge"])


def _forbidden(exc: ScopeError) -> HTTPException:
    return HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


class ContextRequest(BaseModel):
    query: str = Field(min_length=1)
    project: str | None = None  # a hint, validated against the caller's resolved scopes
    snapshot_id: UUID | None = None
    limit: int = Field(default=10, ge=1, le=50)
    token_budget: int = Field(default=2000, ge=100, le=32000)
    as_of: AwareDatetime | None = None
    hints: dict[str, Any] = Field(default_factory=dict)


class SearchRequest(BaseModel):
    query: str = Field(min_length=1)
    project: str | None = None
    limit: int = Field(default=10, ge=1, le=50)
    as_of: AwareDatetime | None = None
    known_as_of: AwareDatetime | None = None


class ProposeRequest(BaseModel):
    subject: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    object: str = Field(min_length=1)
    qualifiers: dict[str, Any] = Field(default_factory=dict)
    evidence_text: str | None = None


class SnapshotRequest(BaseModel):
    project: str | None = None
    as_of: AwareDatetime | None = None


@router.post("/context", summary="Assemble a bounded, cited context pack (primary tool)")
async def get_context(
    req: ContextRequest, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    try:
        return await service.get_context(
            principal.id,
            query=req.query,
            project=req.project,
            snapshot_id=str(req.snapshot_id) if req.snapshot_id else None,
            limit=req.limit,
            token_budget=req.token_budget,
            as_of=req.as_of,
            hints=req.hints,
        )
    except ScopeError as exc:
        raise _forbidden(exc) from exc
    except SnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except SnapshotNotReproducibleError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


@router.post("/search", summary="Combined, cited search (no persisted pack)")
async def search(
    req: SearchRequest, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    try:
        return await service.search(
            principal.id,
            query=req.query,
            project=req.project,
            limit=req.limit,
            as_of=req.as_of,
            known_as_of=req.known_as_of,
        )
    except ScopeError as exc:
        raise _forbidden(exc) from exc


@router.get("/communities", summary="Derived community summaries")
async def communities(
    principal: PrincipalDep,
    service: KnowledgeServiceDep,
    project: str | None = None,
    query: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    try:
        return await service.communities(principal.id, project=project, query=query, limit=limit)
    except ScopeError as exc:
        raise _forbidden(exc) from exc


@router.get("/communities/{community_id}/lineage", summary="Paginated community fact lineage")
async def community_lineage(
    community_id: str,
    principal: PrincipalDep,
    service: KnowledgeServiceDep,
    derivation_run_id: str | None = None,
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        result = await service.community_lineage(
            principal.id,
            community_id=community_id,
            derivation_run_id=derivation_run_id,
            cursor=cursor,
            limit=limit,
        )
    except ScopeError as exc:
        raise _forbidden(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="community not found")
    return result


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


@router.get("/facts/{fact_key}/evidence", summary="The evidence supporting a fact, for citation")
async def get_evidence(
    fact_key: str, principal: PrincipalDep, service: KnowledgeServiceDep
) -> list[dict[str, Any]]:
    evidence = await service.get_evidence(principal.id, fact_key=fact_key)
    if evidence is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="fact not found")
    return evidence


class FeedbackRequest(BaseModel):
    result_ref: str = Field(min_length=1)  # a fact_key or a context-pack id
    signal: str = Field(pattern="^(up|down)$")
    query: str = ""
    signals: dict[str, float] = Field(default_factory=dict)


@router.post("/feedback", summary="Record up/down feedback on a knowledge result")
async def record_feedback(
    body: FeedbackRequest, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    try:
        return await service.record_feedback(
            principal.id,
            result_ref=body.result_ref,
            signal=body.signal,
            query=body.query,
            signals=body.signals or None,
        )
    except ScopeError as exc:
        raise _forbidden(exc) from exc


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
    snapshot_id: UUID, principal: PrincipalDep, service: KnowledgeServiceDep
) -> dict[str, Any]:
    snap = await service.get_snapshot(principal.id, snapshot_id=str(snapshot_id))
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
    return await service.ontology()


@router.get("/ontology/diff", summary="Structured diff between ontology versions")
async def ontology_diff(
    principal: PrincipalDep,
    service: KnowledgeServiceDep,
    from_version: int = Query(alias="from", ge=1),
    to_version: int | None = Query(default=None, alias="to", ge=1),
) -> dict[str, Any]:
    try:
        return await service.ontology_diff(from_version=from_version, to_version=to_version)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


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
