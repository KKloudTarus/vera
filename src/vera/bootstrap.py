"""Composition root helpers shared by all three processes.

Builds the singleton object graph (engine, session factory, queue, memory engine,
object store) once. Each entrypoint (api/worker/mcp) calls ``build_container`` and
injects the pieces it needs. Construction is explicit, with no DI container.
"""

from __future__ import annotations

from dataclasses import dataclass, field

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
from vera.application.queries.search_memory import RerankWeights
from vera.config.settings import Settings
from vera.domain.ports.connectors import SyncStateStore
from vera.domain.ports.curation import (
    ClaimExtractor,
    ContradictionJudge,
    EntityResolutionJudge,
)
from vera.domain.ports.embedder import Embedder
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
    judge: ContradictionJudge | None
    entity_judge: EntityResolutionJudge | None
    embedder: Embedder | None
    # Active rerank weights: the configured defaults until refresh_rerank_weights loads a
    # calibrated set from the database at startup.
    rerank_weights: RerankWeights = field(default_factory=RerankWeights)


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
    embedder: Embedder | None = None
    if settings.memory.semantic_dedup_enabled and settings.memory.provider == "graphiti":
        from vera.adapters.graph import build_embedder

        embedder = build_embedder(settings)  # type: ignore[assignment]
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
    judge: ContradictionJudge | None = None
    entity_judge: EntityResolutionJudge | None = None
    if settings.memory.openai_api_key is not None:
        from vera.adapters.curation.entity_judge import LlmEntityResolutionJudge
        from vera.adapters.curation.judge import LlmContradictionJudge
        from vera.adapters.curation.llm_extractor import LlmClaimExtractor

        key = settings.memory.openai_api_key.get_secret_value()
        extractor = LlmClaimExtractor(api_key=key, model=settings.memory.small_llm_model)
        judge = LlmContradictionJudge(api_key=key, model=settings.memory.small_llm_model)
        # Entity equivalence is a subtle call where a false merge corrupts the graph; the
        # small model is measurably unstable on sibling-vs-same, so use the larger model.
        entity_judge = LlmEntityResolutionJudge(api_key=key, model=settings.memory.llm_model)
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
        judge=judge,
        entity_judge=entity_judge,
        embedder=embedder,
        rerank_weights=build_rerank_weights(settings),
    )


def build_rerank_weights(settings: Settings) -> RerankWeights:
    r = settings.rerank
    return RerankWeights(
        relevance=r.w_relevance,
        authority=r.w_authority,
        verification=r.w_verification,
        recency=r.w_recency,
        feedback=r.w_feedback,
        confidence=r.w_confidence,
        half_life_s=r.recency_half_life_days * 86400.0,
    )


async def refresh_rerank_weights(container: Container) -> None:
    """Load the latest calibrated weights from the database into the container, if any, so
    the ranker uses feedback-calibrated weights in place of the configured defaults.
    """
    from vera.adapters.persistence.repositories.rerank_weights import (
        SqlAlchemyRerankWeightsRepository,
    )

    active = await SqlAlchemyRerankWeightsRepository(container.sessionmaker).get_active()
    if active is not None:
        container.rerank_weights = active


async def dispose_container(container: Container) -> None:
    await container.engine.dispose()
