"""Metering wrappers around Graphiti's embedder and LLM clients.

Each wrapper delegates to the inner client, then records a usage event tagged with
the current request context. Wrap the embedder inside the cache so only real provider
calls are metered. A cache hit costs nothing and must not be counted.

Token counts come from the provider when it reports them; the offline embedder does
not, so its usage is estimated from text length. Estimated or exact, the event still
carries model, operation, request kind, and group, which is what makes cost per
episode and per query queryable.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any, cast

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
        import openai
        from graphiti_core.llm_client.errors import EmptyResponseError, RateLimitError
        from graphiti_core.llm_client.openai_generic_client import (
            DEFAULT_MODEL,
            OpenAIGenericClient,
        )
        from openai.types.chat import ChatCompletionMessageParam
        from openai.types.shared_params import ResponseFormatJSONObject

        class _MeteredOpenAIGenericClient(OpenAIGenericClient):
            async def _generate_response(
                self,
                messages: list[Any],
                response_model: type[Any] | None = None,
                max_tokens: int = 16384,
                model_size: Any = None,
            ) -> dict[str, Any]:
                async def _raw() -> tuple[dict[str, Any], int, int]:
                    openai_messages: list[ChatCompletionMessageParam] = []
                    for message in messages:
                        message.content = self._clean_input(message.content)
                        if message.role in {"user", "system"}:
                            openai_messages.append(
                                cast(
                                    "ChatCompletionMessageParam",
                                    {"role": message.role, "content": message.content},
                                )
                            )
                    try:
                        provider_response = await self.client.chat.completions.create(
                            model=self.model or DEFAULT_MODEL,
                            messages=openai_messages,
                            temperature=self.temperature,
                            max_tokens=max_tokens,
                            response_format=cast(
                                "ResponseFormatJSONObject",
                                self._build_response_format(response_model),
                            ),
                        )
                        content = provider_response.choices[0].message.content or ""
                        if not content:
                            raise EmptyResponseError("LLM returned an empty response")
                        usage = provider_response.usage
                        if usage is None:
                            raise RuntimeError("OpenAI-compatible response omitted token usage")
                        parsed = json.loads(self._strip_code_fences(content))
                        return parsed, int(usage.prompt_tokens), int(usage.completion_tokens)
                    except openai.RateLimitError as exc:
                        raise RateLimitError from exc

                prompt_estimate = sum(
                    estimate_tokens(str(getattr(m, "content", "") or "")) for m in messages
                )
                if policy is not None:
                    response, prompt_tokens, completion_tokens = await policy.call(
                        _raw, tokens=prompt_estimate
                    )
                else:
                    response, prompt_tokens, completion_tokens = await _raw()
                event = build_usage_event(
                    model=llm_model,
                    operation="llm",
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
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
