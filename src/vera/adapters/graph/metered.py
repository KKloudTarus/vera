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
from openai import APITimeoutError

from vera.adapters.resilience.policy import ResiliencePolicy
from vera.observability.cost import (
    UsageAccountingError,
    UsageSink,
    build_usage_event,
    cost_usd,
    emit_usage,
    estimate_tokens,
    guard_provider_call,
    maximum_prompt_tokens,
    provider_reported_cost,
    settle_provider_call,
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
            usage_complete=False,
        )
        await emit_usage(self._sink, event)

    async def create(
        self, input_data: str | list[str] | Iterable[int] | Iterable[Iterable[int]]
    ) -> list[float]:
        text = input_data if isinstance(input_data, str) else str(input_data)

        async def attempt() -> list[float]:
            vector = await self._inner.create(input_data)
            await self._meter(estimate_tokens(text))
            return vector

        return await guard_provider_call(
            attempt,
            self._sink,
            model=self._model,
            operation="embedding",
            prompt_token_limit=maximum_prompt_tokens((text,)),
            completion_token_limit=0,
            timeout_exceptions=(APITimeoutError,),
        )

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        async def attempt() -> list[list[float]]:
            vectors = await self._inner.create_batch(input_data_list)
            await self._meter(sum(estimate_tokens(text) for text in input_data_list))
            return vectors

        return await guard_provider_call(
            attempt,
            self._sink,
            model=self._model,
            operation="embedding",
            prompt_token_limit=maximum_prompt_tokens(input_data_list),
            completion_token_limit=0,
            timeout_exceptions=(APITimeoutError,),
        )


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

                async def _request() -> Any:
                    try:
                        return await self.client.chat.completions.create(
                            model=self.model or DEFAULT_MODEL,
                            messages=openai_messages,
                            temperature=self.temperature,
                            max_tokens=max_tokens,
                            response_format=cast(
                                "ResponseFormatJSONObject",
                                self._build_response_format(response_model),
                            ),
                        )
                    except openai.RateLimitError as exc:
                        raise RateLimitError from exc

                async def _raw() -> Any:
                    return await guard_provider_call(
                        _request,
                        sink,
                        model=llm_model,
                        operation="llm",
                        prompt_token_limit=maximum_prompt_tokens(
                            str(getattr(message, "content", "") or "") for message in messages
                        ),
                        completion_token_limit=max_tokens,
                        timeout_exceptions=(APITimeoutError,),
                    )

                prompt_estimate = sum(
                    estimate_tokens(str(getattr(m, "content", "") or "")) for m in messages
                )
                if policy is not None:
                    provider_response = await policy.call(_raw, tokens=prompt_estimate)
                else:
                    provider_response = await _raw()

                async def _record() -> dict[str, Any]:
                    if getattr(provider_response, "model", None) != llm_model:
                        raise UsageAccountingError(
                            "provider response model differs from reserved model"
                        )
                    content = provider_response.choices[0].message.content or ""
                    if not content:
                        raise EmptyResponseError("LLM returned an empty response")
                    usage = provider_response.usage
                    if usage is None:
                        raise RuntimeError("OpenAI-compatible response omitted token usage")
                    parsed = json.loads(self._strip_code_fences(content))
                    event = build_usage_event(
                        model=llm_model,
                        operation="llm",
                        prompt_tokens=int(usage.prompt_tokens),
                        completion_tokens=int(usage.completion_tokens),
                        exact_cost_usd=provider_reported_cost(provider_response),
                        usage_complete=False,
                    )
                    await emit_usage(sink, event)
                    if event.cost_complete:
                        await settle_provider_call(
                            sink,
                            reserved_cost_usd=cost_usd(
                                llm_model,
                                maximum_prompt_tokens(
                                    str(getattr(message, "content", "") or "")
                                    for message in messages
                                ),
                                max_tokens,
                            ),
                            actual_cost_usd=event.cost_usd,
                        )
                    return parsed

                return await guard_provider_call(
                    _record,
                    sink,
                    model=llm_model,
                    operation="llm",
                    reserve_budget=False,
                )

        return _MeteredOpenAIGenericClient(
            config=config,
            client=openai.AsyncOpenAI(
                api_key=config.api_key, base_url=config.base_url, max_retries=0
            ),
            structured_output_mode="json_object",
        )

    from graphiti_core.llm_client.openai_client import OpenAIClient

    class _MeteredOpenAIClient(OpenAIClient):
        async def _metered_provider_response(
            self,
            request: Any,
            *,
            model: str,
            messages: list[Any],
            max_tokens: int,
            structured: bool,
        ) -> Any:
            prompt_limit = maximum_prompt_tokens(
                str(message.get("content", "") or "") for message in messages
            )

            async def _raw() -> Any:
                return await guard_provider_call(
                    request,
                    sink,
                    model=model,
                    operation="llm",
                    prompt_token_limit=prompt_limit,
                    completion_token_limit=max_tokens,
                    timeout_exceptions=(APITimeoutError,),
                )

            prompt_estimate = sum(
                estimate_tokens(str(message.get("content", "") or "")) for message in messages
            )
            if policy is not None:
                provider_response = await policy.call(_raw, tokens=prompt_estimate)
            else:
                provider_response = await _raw()

            async def _record() -> Any:
                if getattr(provider_response, "model", None) != model:
                    raise UsageAccountingError(
                        "provider response model differs from reserved model"
                    )
                usage = getattr(provider_response, "usage", None)
                prompt_value = getattr(
                    usage, "input_tokens" if structured else "prompt_tokens", None
                )
                completion_value = getattr(
                    usage, "output_tokens" if structured else "completion_tokens", None
                )
                if (
                    not isinstance(prompt_value, int)
                    or isinstance(prompt_value, bool)
                    or prompt_value < 0
                    or not isinstance(completion_value, int)
                    or isinstance(completion_value, bool)
                    or completion_value < 0
                ):
                    raise UsageAccountingError("OpenAI response omitted exact token usage")
                event = build_usage_event(
                    model=model,
                    operation="llm",
                    prompt_tokens=prompt_value,
                    completion_tokens=completion_value,
                    exact_cost_usd=provider_reported_cost(provider_response),
                    usage_complete=False,
                )
                await emit_usage(sink, event)
                if event.cost_complete:
                    await settle_provider_call(
                        sink,
                        reserved_cost_usd=cost_usd(model, prompt_limit, max_tokens),
                        actual_cost_usd=event.cost_usd,
                    )
                return provider_response

            return await guard_provider_call(
                _record,
                sink,
                model=model,
                operation="llm",
                reserve_budget=False,
            )

        async def _create_completion(
            self,
            model: str,
            messages: list[Any],
            temperature: float | None,
            max_tokens: int,
            response_model: type[Any] | None = None,
            reasoning: str | None = None,
            verbosity: str | None = None,
        ) -> Any:
            async def _request() -> Any:
                return cast(
                    Any,
                    await OpenAIClient._create_completion(
                        self,
                        model,
                        messages,
                        temperature,
                        max_tokens,
                        response_model,
                        reasoning,
                        verbosity,
                    ),
                )

            return await self._metered_provider_response(
                _request,
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                structured=False,
            )

        async def _create_structured_completion(
            self,
            model: str,
            messages: list[Any],
            temperature: float | None,
            max_tokens: int,
            response_model: type[Any],
            reasoning: str | None = None,
            verbosity: str | None = None,
        ) -> Any:
            async def _request() -> Any:
                return cast(
                    Any,
                    await OpenAIClient._create_structured_completion(
                        self,
                        model,
                        messages,
                        temperature,
                        max_tokens,
                        response_model,
                        reasoning,
                        verbosity,
                    ),
                )

            return await self._metered_provider_response(
                _request,
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                structured=True,
            )

    from openai import AsyncOpenAI

    return _MeteredOpenAIClient(
        config=config,
        client=AsyncOpenAI(api_key=config.api_key, base_url=config.base_url, max_retries=0),
    )
