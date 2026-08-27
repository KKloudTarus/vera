"""Composition root helpers shared by all three processes.

Builds the singleton object graph (engine, session factory, queue, memory engine,
object store) once. Each entrypoint (api/worker/mcp) calls ``build_container`` and
injects the pieces it needs. Construction is explicit, with no DI container.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from vera.adapters.graph import build_memory_engine
from vera.adapters.identity import (
    ApiKeyAuthenticator,
    CompositeAuthenticator,
    OidcAuthenticator,
    OidcTokenVerifier,
)
from vera.adapters.objectstore.s3_adapter import S3ObjectStore
from vera.adapters.persistence.base import create_engine, create_sessionmaker
from vera.adapters.persistence.repositories import SqlAlchemyRetrievalReadModel
from vera.adapters.persistence.repositories.scope import SqlAlchemyScopeResolver
from vera.adapters.persistence.repositories.sync import SqlAlchemySyncStateStore
from vera.adapters.persistence.repositories.usage import SqlAlchemyUsageSink
from vera.adapters.queue.postgres_queue import PostgresJobQueue
from vera.application.identity import ScopeResolutionService
from vera.config.settings import Settings
from vera.domain.ports.connectors import SyncStateStore
from vera.domain.ports.curation import ClaimExtractor
from vera.domain.ports.identity import Authenticator
from vera.domain.ports.job_queue import JobQueue
from vera.domain.ports.memory_engine import MemoryEngine
from vera.domain.ports.object_store import ObjectStore
from vera.domain.ports.retrieval import RetrievalReadModel
from vera.observability.cost import UsageSink


@dataclass(slots=True)
class Container:
    """Process-wide singletons. Request/message-scoped objects are built per-op."""

    settings: Settings
    engine: AsyncEngine
    sessionmaker: async_sessionmaker[AsyncSession]
    queue: JobQueue
    memory: MemoryEngine
    object_store: ObjectStore
    retrieval_read: RetrievalReadModel
    authenticator: Authenticator
    scopes: ScopeResolutionService
    usage_sink: UsageSink | None
    sync_state: SyncStateStore
    extractor: ClaimExtractor


def build_container(settings: Settings) -> Container:
    engine = create_engine(settings.db)
    sessionmaker = create_sessionmaker(engine)
    queue: JobQueue = PostgresJobQueue(
        sessionmaker, visibility_timeout_s=settings.worker.visibility_timeout_s
    )
    usage_sink: UsageSink | None = (
        SqlAlchemyUsageSink(sessionmaker) if settings.observability.cost_tracking_enabled else None
    )
    memory: MemoryEngine = build_memory_engine(settings, usage_sink)
    object_store: ObjectStore = S3ObjectStore(settings.objectstore)
    retrieval_read: RetrievalReadModel = SqlAlchemyRetrievalReadModel(sessionmaker)

    scopes = ScopeResolutionService(SqlAlchemyScopeResolver(sessionmaker))
    oidc: OidcAuthenticator | None = None
    if settings.api.oidc_signing_key is not None or settings.api.oidc_jwks_url is not None:
        signing_key = (
            settings.api.oidc_signing_key.get_secret_value()
            if settings.api.oidc_signing_key
            else None
        )
        oidc = OidcAuthenticator(
            sessionmaker,
            OidcTokenVerifier(
                signing_key=signing_key,
                jwks_url=settings.api.oidc_jwks_url,
                algorithms=settings.api.oidc_algorithms,
                issuer=settings.api.oidc_issuer,
                audience=settings.api.oidc_audience,
            ),
        )
    authenticator: Authenticator = CompositeAuthenticator(
        api_key=ApiKeyAuthenticator(sessionmaker), oidc=oidc
    )

    # LLM extraction turns free text into trust-tiered claims; without a key, only
    # structured metadata (triples/claims) is ingested.
    extractor: ClaimExtractor
    if settings.memory.openai_api_key is not None:
        from vera.adapters.curation.llm_extractor import LlmClaimExtractor

        extractor = LlmClaimExtractor(
            api_key=settings.memory.openai_api_key.get_secret_value(),
            model=settings.memory.small_llm_model,
        )
    else:
        from vera.adapters.curation.extractor import StructuredClaimExtractor

        extractor = StructuredClaimExtractor()

    return Container(
        settings=settings,
        engine=engine,
        sessionmaker=sessionmaker,
        queue=queue,
        memory=memory,
        object_store=object_store,
        retrieval_read=retrieval_read,
        authenticator=authenticator,
        scopes=scopes,
        usage_sink=usage_sink,
        sync_state=SqlAlchemySyncStateStore(sessionmaker),
        extractor=extractor,
    )


async def dispose_container(container: Container) -> None:
    await container.engine.dispose()
