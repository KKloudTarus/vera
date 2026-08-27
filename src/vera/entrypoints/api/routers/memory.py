"""Memory endpoints: the small, safe surface over the memory plane.

A client never chooses its scopes. VERA resolves the allowed group_ids from the
authenticated principal's memberships, so search only ever spans what the caller may
see and admitting a source requires membership in its target scope.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from vera.application.commands.ingest_source import IngestSource
from vera.application.queries.search_memory import SearchMemory
from vera.entrypoints.api.deps import (
    IngestHandlerDep,
    PrincipalDep,
    ScopesDep,
    SearchHandlerDep,
)
from vera.shared.errors import is_ok
from vera.shared.ids import deterministic_id
from vera.shared.types import GroupId, JsonDict, SourceId, empty_json

router = APIRouter(prefix="/memory", tags=["memory"])


class SearchRequest(BaseModel):
    text: str = Field(min_length=1)
    limit: int = Field(default=10, ge=1, le=100)


class RankedHitOut(BaseModel):
    fact: str
    score: float
    source_id: str | None = None
    verification: str | None = None
    authority: float = 0.5


class IngestRequest(BaseModel):
    source_id: str = Field(min_length=1)
    group_id: str = Field(min_length=1)
    payload: JsonDict = Field(default_factory=empty_json)


class IngestAcceptedOut(BaseModel):
    dedup_uuid: str
    status: str = "accepted"


@router.post("/search", response_model=list[RankedHitOut], summary="Search memory")
async def search(
    req: SearchRequest,
    principal: PrincipalDep,
    scopes: ScopesDep,
    handler: SearchHandlerDep,
) -> list[RankedHitOut]:
    group_ids = await scopes.allowed_group_ids(principal.id)
    if not group_ids:
        return []
    hits = await handler.handle(
        SearchMemory(
            text=req.text,
            group_ids=tuple(GroupId(g) for g in group_ids),
            limit=req.limit,
        )
    )
    return [
        RankedHitOut(
            fact=h.fact,
            score=h.score,
            source_id=h.source_id,
            verification=h.verification,
            authority=h.authority,
        )
        for h in hits
    ]


@router.post(
    "/ingest",
    response_model=IngestAcceptedOut,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Admit a source for async ingestion",
)
async def ingest(
    req: IngestRequest,
    principal: PrincipalDep,
    scopes: ScopesDep,
    handler: IngestHandlerDep,
) -> IngestAcceptedOut:
    if not await scopes.can_read(principal.id, req.group_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="not a member of the target scope",
        )
    result = await handler.handle(
        IngestSource(
            source_id=SourceId(req.source_id),
            group_id=GroupId(req.group_id),
            payload=req.payload,
        )
    )
    if is_ok(result):
        return IngestAcceptedOut(dedup_uuid=result.value.dedup_uuid)
    # Already ingested. Idempotent no-op that surfaces the existing acceptance.
    return IngestAcceptedOut(dedup_uuid=str(deterministic_id(req.source_id)), status="duplicate")
