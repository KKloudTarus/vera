"""Memory endpoints: the small, safe read surface over the memory plane.

A client never chooses its scopes. VERA resolves the allowed group_ids from the
authenticated principal's memberships, so search only ever spans what the caller may
see. Writing to memory goes through connectors and curation (trust tiers, verification,
provenance), never a raw admit, so the raw ingest endpoint is gone.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from vera.adapters.persistence.retraction import RetractionService
from vera.application.queries.search_memory import SearchMemory
from vera.entrypoints.api.deps import ContainerDep, PrincipalDep, ScopesDep, SearchHandlerDep
from vera.shared.errors import Err
from vera.shared.types import GroupId

router = APIRouter(prefix="/memory", tags=["memory"])


class SearchRequest(BaseModel):
    text: str = Field(min_length=1)
    limit: int = Field(default=10, ge=1, le=100)
    # Point-in-time (valid-time) query: return the memory as it stood at this instant.
    # Omit for the current view, which hides superseded and retracted facts.
    as_of: datetime | None = None


class RankedHitOut(BaseModel):
    fact: str
    score: float
    source_id: str | None = None
    verification: str | None = None
    authority: float = 0.5
    signals: dict[str, float] = Field(default_factory=dict)


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
            as_of=req.as_of,
        )
    )
    return [
        RankedHitOut(
            fact=h.fact,
            score=h.score,
            source_id=h.source_id,
            verification=h.verification,
            authority=h.authority,
            signals=dict(h.signals),
        )
        for h in hits
    ]


class ExploreRequest(BaseModel):
    entity: str = Field(min_length=1)
    depth: int = Field(default=2, ge=1, le=3)
    limit: int = Field(default=20, ge=1, le=100)


class ConnectedFactOut(BaseModel):
    fact: str
    source_id: str | None = None
    verification: str | None = None


@router.post(
    "/explore",
    response_model=list[ConnectedFactOut],
    summary="Multi-hop: facts within N hops of an entity",
)
async def explore(
    req: ExploreRequest,
    principal: PrincipalDep,
    scopes: ScopesDep,
    container: ContainerDep,
) -> list[ConnectedFactOut]:
    group_ids = await scopes.allowed_group_ids(principal.id)
    if not group_ids:
        return []
    hits = await container.memory.neighbors(
        group_ids=tuple(GroupId(g) for g in group_ids),
        center=req.entity,
        depth=req.depth,
        limit=req.limit,
    )
    edge_uuids = [h.edge_uuid for h in hits if h.edge_uuid]
    provenance = await container.retrieval_read.enrich(
        group_ids=list(group_ids), edge_uuids=edge_uuids
    )
    out: list[ConnectedFactOut] = []
    for hit in hits:
        prov = provenance.get(hit.edge_uuid or "")
        out.append(
            ConnectedFactOut(
                fact=hit.fact,
                source_id=prov.source_id if prov else None,
                verification=prov.verification if prov else None,
            )
        )
    return out


@router.delete(
    "/sources/{source_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Retract (or erase) a published source",
)
async def retract_source(
    source_id: str,
    principal: PrincipalDep,
    scopes: ScopesDep,
    container: ContainerDep,
    erase: bool = False,
) -> None:
    # A published source_id is "<group_id>:<claim_uuid>". Retraction and erasure are
    # destructive, so they require ADMIN or higher on the group, not mere read access.
    group_id = source_id.rsplit(":", 1)[0]
    if not await scopes.can_administer(principal.id, group_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="retraction requires an admin role"
        )
    service = RetractionService(container.sessionmaker, container.memory, container.object_store)
    result = await service.retract_source(
        group_id=group_id,
        source_id=source_id,
        actor_principal_id=principal.id,
        erase_artifact=erase,
    )
    if isinstance(result, Err):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=result.error.message)
