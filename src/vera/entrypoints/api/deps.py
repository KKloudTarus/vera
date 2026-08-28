"""Request-scoped dependency wiring (FastAPI ``Depends``).

Singletons come from ``app.state.container`` (built once at startup); handlers are
constructed per-request from ports. The API edge composes its object graph here and
authenticates the bearer credential into a principal before any protected handler runs.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from vera.adapters.persistence.repositories.scope import SqlAlchemyScopeResolver
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.commands import IngestSourceHandler
from vera.application.identity import IdentityService, ScopeResolutionService
from vera.application.queries import SearchMemoryHandler
from vera.bootstrap import Container
from vera.domain.identity.models import AuthenticatedPrincipal
from vera.entrypoints.knowledge import KnowledgeService


def get_container(request: Request) -> Container:
    return request.app.state.container


ContainerDep = Annotated[Container, Depends(get_container)]


def get_search_handler(container: ContainerDep) -> SearchMemoryHandler:
    return SearchMemoryHandler(
        container.memory,
        container.retrieval_read,
        read_timeout_s=container.settings.resilience.read_timeout_s,
        weights=container.rerank_weights,
        reranker=container.reranker,
        cross_encoder_weight=container.settings.rerank.cross_encoder_weight,
        cross_encoder_top_n=container.settings.rerank.cross_encoder_top_n,
    )


def get_ingest_handler(container: ContainerDep) -> IngestSourceHandler:
    return IngestSourceHandler(container.queue)


def get_scopes(container: ContainerDep) -> ScopeResolutionService:
    return container.scopes


def get_knowledge_service(container: ContainerDep) -> KnowledgeService:
    return KnowledgeService(container, SqlAlchemyScopeResolver(container.sessionmaker))


SearchHandlerDep = Annotated[SearchMemoryHandler, Depends(get_search_handler)]
IngestHandlerDep = Annotated[IngestSourceHandler, Depends(get_ingest_handler)]
ScopesDep = Annotated[ScopeResolutionService, Depends(get_scopes)]
KnowledgeServiceDep = Annotated[KnowledgeService, Depends(get_knowledge_service)]


async def get_uow(container: ContainerDep) -> AsyncIterator[SqlAlchemyUnitOfWork]:
    # Commit after a clean handler return. If the handler raises (including an
    # HTTPException from a rejected authorization), the commit is skipped and the
    # context manager rolls back, so a failed request writes nothing.
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        yield uow
        await uow.commit()


UnitOfWorkDep = Annotated[SqlAlchemyUnitOfWork, Depends(get_uow)]


def get_identity_service(uow: UnitOfWorkDep) -> IdentityService:
    return IdentityService(uow)


IdentityServiceDep = Annotated[IdentityService, Depends(get_identity_service)]

_bearer = HTTPBearer(auto_error=False, description="API key or OIDC bearer token")


async def get_principal(
    container: ContainerDep,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> AuthenticatedPrincipal:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer credential",
            headers={"WWW-Authenticate": "Bearer"},
        )
    principal = await container.authenticator.authenticate(credentials.credentials)
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credential",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return principal


PrincipalDep = Annotated[AuthenticatedPrincipal, Depends(get_principal)]
