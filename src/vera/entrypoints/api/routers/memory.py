"""Memory endpoints: the small, safe read surface over the memory plane.

A client never chooses its scopes. VERA resolves the allowed group_ids from the
authenticated principal's memberships, so search only ever spans what the caller may
see. Writing to memory goes through connectors and curation (trust tiers, verification,
provenance), never a raw admit, so the raw ingest endpoint is gone.
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

from vera.application.queries.search_memory import SearchMemory
from vera.entrypoints.api.deps import PrincipalDep, ScopesDep, SearchHandlerDep
from vera.shared.types import GroupId

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
