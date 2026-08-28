"""Metering wrappers around Graphiti's embedder and LLM clients.

Each wrapper delegates to the inner client, then records a usage event tagged with the
current request context (set by the worker for ingest, by the search handler for a
query). Wrap the embedder INSIDE the cache so only real provider calls are metered:
a cache hit costs nothing and must not be counted.

Token counts come from the provider when it reports them; the offline embedder does
not, so its usage is estimated from text length. Estimated or exact, the event still
carries model, operation, request kind, and group, which is what makes cost per
episode and per query queryable.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from graphiti_core.embedder.client import EmbedderClient
from graphiti_core.llm_client.client import LLMClient
from graphiti_core.llm_client.config import LLMConfig

from vera.adapters.resilience.policy import ResiliencePolicy
from vera.observability.cost import (
    UsageSink,
    build_usage_event,
    emit_usage,
    estimate_tokens,
)


class MeteredEmbedder(EmbedderClient):
    def __init__(self, inner: EmbedderClient, *, model: str, sink: UsageSink | None) -> None:
        self._inner = inner
        self._model = model
        self._sink = sink

    async def _meter(self, tokens: int) -> None:
        event = build_usage_event(
            model=self._model,
            operation="embedding",
            prompt_tokens=tokens,
            completion_tokens=0,
        )
        await emit_usage(self._sink, event)

    async def create(
        self, input_data: str | list[str] | Iterable[int] | Iterable[Iterable[int]]
    ) -> list[float]:
        vector = await self._inner.create(input_data)
        text = input_data if isinstance(input_data, str) else str(input_data)
        await self._meter(estimate_tokens(text))
        return vector

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        vectors = await self._inner.create_batch(input_data_list)
        await self._meter(sum(estimate_tokens(text) for text in input_data_list))
        return vectors


def build_metered_llm_client(
    config: LLMConfig,
    *,
    llm_model: str,
    sink: UsageSink | None,
    policy: ResiliencePolicy | None = None,
) -> LLMClient:
    """Build a resilient, metered Graphiti client for OpenAI or a compatible endpoint."""
    if config.base_url:
        from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient

        class _MeteredOpenAIGenericClient(OpenAIGenericClient):
            async def _generate_response(
                self,
                messages: list[Any],
                response_model: type[Any] | None = None,
                max_tokens: int = 16384,
                model_size: Any = None,
            ) -> dict[str, Any]:
                async def _raw() -> dict[str, Any]:
                    return await OpenAIGenericClient._generate_response(
                        self, messages, response_model, max_tokens, model_size
                    )

                prompt_estimate = sum(
                    estimate_tokens(str(getattr(m, "content", "") or "")) for m in messages
                )
                if policy is not None:
                    response = await policy.call(_raw, tokens=prompt_estimate)
                else:
                    response = await _raw()
                event = build_usage_event(
                    model=llm_model,
                    operation="llm",
                    prompt_tokens=prompt_estimate,
                    completion_tokens=estimate_tokens(str(response)),
                )
                await emit_usage(sink, event)
                return response

        return _MeteredOpenAIGenericClient(config=config, structured_output_mode="json_object")

    from graphiti_core.llm_client.openai_client import OpenAIClient

    class _MeteredOpenAIClient(OpenAIClient):
        async def _generate_response(
            self,
            messages: list[Any],
            response_model: type[Any] | None = None,
            max_tokens: int = 16384,
            model_size: Any = None,
        ) -> tuple[dict[str, Any], int, int]:
            async def _raw() -> tuple[dict[str, Any], int, int]:
                return await OpenAIClient._generate_response(
                    self, messages, response_model, max_tokens, model_size
                )

            prompt_estimate = sum(
                estimate_tokens(str(getattr(m, "content", "") or "")) for m in messages
            )
            if policy is not None:
                response, input_tokens, output_tokens = await policy.call(
                    _raw, tokens=prompt_estimate
                )
            else:
                response, input_tokens, output_tokens = await _raw()
            event = build_usage_event(
                model=llm_model,
                operation="llm",
                prompt_tokens=input_tokens,
                completion_tokens=output_tokens,
            )
            await emit_usage(sink, event)
            return response, input_tokens, output_tokens

    return _MeteredOpenAIClient(config=config)
