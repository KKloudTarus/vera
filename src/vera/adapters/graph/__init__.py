"""Memory-engine adapters and their factory.

The Graphiti adapter is the only module that imports ``graphiti_core``; the null
engine lets the app and tests run without a graph. ``build_memory_engine`` chooses
between them from settings.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from vera.adapters.graph.null import NullMemoryEngine
from vera.config.settings import Settings, active_embedding
from vera.domain.ports.memory_engine import MemoryEngine
from vera.domain.ports.projection import FactProjection
from vera.observability.cost import UsageSink

if TYPE_CHECKING:
    from graphiti_core import Graphiti

__all__ = [
    "NullMemoryEngine",
    "build_graphiti_client",
    "build_memory_engine",
    "maybe_fact_projection",
]


def _embedder(settings: Settings) -> object:
    from vera.adapters.graph.offline import DeterministicEmbedder

    memory = settings.memory
    if memory.embedder == "deterministic":
        return DeterministicEmbedder(memory.embedding_dim)
    if memory.embedder == "voyage":
        from vera.adapters.embedding.voyage import VoyageClient
        from vera.adapters.graph.voyage_embedder import GraphitiVoyageEmbedder
        from vera.config.settings import voyage_api_key

        return GraphitiVoyageEmbedder(
            VoyageClient(api_key=voyage_api_key(settings), base_url=settings.voyage.base_url),
            model=settings.voyage.embedding_model,
            dim=settings.voyage.embedding_dim,
        )
    from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig

    key = memory.openai_api_key.get_secret_value() if memory.openai_api_key else None
    return OpenAIEmbedder(
        config=OpenAIEmbedderConfig(
            api_key=key,
            embedding_model=memory.embedding_model,
            embedding_dim=memory.embedding_dim,
            base_url=memory.openai_base_url,
        )
    )


def _llm_client(settings: Settings, usage_sink: UsageSink | None) -> object:
    memory = settings.memory
    if not memory.openai_api_key:
        from vera.adapters.graph.offline import NoLLMClient

        return NoLLMClient()
    from graphiti_core.llm_client.config import LLMConfig

    from vera.adapters.graph.metered import build_metered_llm_client
    from vera.adapters.resilience.policy import build_resilience_policy

    return build_metered_llm_client(
        LLMConfig(
            api_key=memory.openai_api_key.get_secret_value(),
            model=memory.llm_model,
            small_model=memory.small_llm_model,
            base_url=memory.openai_base_url,
        ),
        llm_model=memory.llm_model,
        sink=usage_sink,
        policy=build_resilience_policy(settings.resilience, name="openai-llm"),
    )


def build_graphiti_client(settings: Settings, usage_sink: UsageSink | None = None) -> Graphiti:
    from graphiti_core import Graphiti

    from vera.adapters.graph.caching import CachingEmbedder
    from vera.adapters.graph.metered import MeteredEmbedder
    from vera.adapters.graph.offline import NoCrossEncoder

    password = settings.neo4j.password.get_secret_value() if settings.neo4j.password else None
    model_name, dim = active_embedding(settings)
    real = _embedder(settings)
    if settings.memory.embedder in ("openai", "voyage"):
        from vera.adapters.graph.resilient import ResilientEmbedder
        from vera.adapters.resilience.policy import build_resilience_policy

        real = ResilientEmbedder(
            real,  # type: ignore[arg-type]
            build_resilience_policy(
                settings.resilience, name=f"{settings.memory.embedder}-embedder"
            ),
        )
    # Meter inside the cache so only real (cache-miss) provider calls are counted.
    metered = MeteredEmbedder(
        real,  # type: ignore[arg-type]
        model=model_name,
        sink=usage_sink,
    )
    namespace = f"{model_name}:{dim}"
    l2 = None
    if settings.resilience.valkey_url:
        from redis.asyncio import Redis

        from vera.adapters.graph.valkey_cache import ValkeyEmbeddingCache

        client = Redis.from_url(settings.resilience.valkey_url)  # pyright: ignore[reportUnknownMemberType]
        l2 = ValkeyEmbeddingCache(client)
    embedder = CachingEmbedder(metered, namespace=namespace, l2=l2)
    common = {
        "embedder": embedder,
        "llm_client": _llm_client(settings, usage_sink),
        "cross_encoder": NoCrossEncoder(),
    }
    if settings.memory.graph_backend == "falkordb":
        from graphiti_core.driver.falkordb_driver import FalkorDriver

        falkor = settings.falkor
        driver = FalkorDriver(
            host=falkor.host,
            port=falkor.port,
            password=falkor.password.get_secret_value() if falkor.password else None,
            database=falkor.database,
        )
        return Graphiti(graph_driver=driver, **common)  # type: ignore[arg-type]
    return Graphiti(
        uri=settings.neo4j.uri,
        user=settings.neo4j.user,
        password=password,
        **common,  # type: ignore[arg-type]
    )


def build_embedder(settings: Settings) -> object:
    """A standalone cached embedder (same model as ingestion) for entity linking."""
    from vera.adapters.graph.caching import CachingEmbedder
    from vera.adapters.graph.embedder_port import GraphitiEmbedderAdapter

    model_name, dim = active_embedding(settings)
    namespace = f"{model_name}:{dim}"
    cached = CachingEmbedder(_embedder(settings), namespace=namespace)  # type: ignore[arg-type]
    return GraphitiEmbedderAdapter(cached)


def build_memory_engine(settings: Settings, usage_sink: UsageSink | None = None) -> MemoryEngine:
    # Neo4j needs a URI; FalkorDB configures via its own host/port, so don't require one.
    if settings.memory.provider != "graphiti":
        return NullMemoryEngine()
    if settings.memory.graph_backend == "neo4j" and not settings.neo4j.uri:
        return NullMemoryEngine()
    from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine

    return GraphitiMemoryEngine(build_graphiti_client(settings, usage_sink))


def maybe_fact_projection(memory: MemoryEngine) -> FactProjection | None:
    """The fact projection for the active memory engine, or None when the graph is a no-op
    (no Graphiti configured). Lets the composition root wire outbox-driven projection only
    when there is a real graph to project into.
    """
    from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine

    if isinstance(memory, GraphitiMemoryEngine):
        return memory.fact_projection()
    return None
