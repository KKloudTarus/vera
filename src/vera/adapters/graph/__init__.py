"""Memory-engine adapters and their factory.

The Graphiti adapter is the only module that imports ``graphiti_core``; the null
engine lets the app and tests run without a graph. ``build_memory_engine`` chooses
between them from settings.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from vera.adapters.graph.null import NullMemoryEngine
from vera.config.settings import MemorySettings, Settings
from vera.domain.ports.memory_engine import MemoryEngine
from vera.observability.cost import UsageSink

if TYPE_CHECKING:
    from graphiti_core import Graphiti

__all__ = ["NullMemoryEngine", "build_graphiti_client", "build_memory_engine"]


def _embedder(memory: MemorySettings) -> object:
    from vera.adapters.graph.offline import DeterministicEmbedder

    if memory.embedder == "deterministic":
        return DeterministicEmbedder(memory.embedding_dim)
    from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig

    key = memory.openai_api_key.get_secret_value() if memory.openai_api_key else None
    return OpenAIEmbedder(
        config=OpenAIEmbedderConfig(
            api_key=key,
            embedding_model=memory.embedding_model,
            embedding_dim=memory.embedding_dim,
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
    real = _embedder(settings.memory)
    if settings.memory.embedder == "openai":
        from vera.adapters.graph.resilient import ResilientEmbedder
        from vera.adapters.resilience.policy import build_resilience_policy

        real = ResilientEmbedder(
            real,  # type: ignore[arg-type]
            build_resilience_policy(settings.resilience, name="openai-embedder"),
        )
    # Meter inside the cache so only real (cache-miss) provider calls are counted.
    metered = MeteredEmbedder(
        real,  # type: ignore[arg-type]
        model=settings.memory.embedding_model,
        sink=usage_sink,
    )
    namespace = f"{settings.memory.embedding_model}:{settings.memory.embedding_dim}"
    l2 = None
    if settings.resilience.valkey_url:
        from redis.asyncio import Redis

        from vera.adapters.graph.valkey_cache import ValkeyEmbeddingCache

        client = Redis.from_url(settings.resilience.valkey_url)  # pyright: ignore[reportUnknownMemberType]
        l2 = ValkeyEmbeddingCache(client)
    embedder = CachingEmbedder(metered, namespace=namespace, l2=l2)
    return Graphiti(
        uri=settings.neo4j.uri,
        user=settings.neo4j.user,
        password=password,
        embedder=embedder,
        llm_client=_llm_client(settings, usage_sink),  # type: ignore[arg-type]
        cross_encoder=NoCrossEncoder(),
    )


def build_memory_engine(settings: Settings, usage_sink: UsageSink | None = None) -> MemoryEngine:
    if settings.memory.provider != "graphiti" or not settings.neo4j.uri:
        return NullMemoryEngine()
    from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine

    return GraphitiMemoryEngine(build_graphiti_client(settings, usage_sink))
