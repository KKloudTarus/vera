"""Resilience and cost metering for the curation LLM adapters.

The curation adapters (claim extractor, contradiction and entity judges, LLM reranker)
each call ``chat.completions.create`` on an injected client. This wrapper presents the same
surface, but routes every call through a shared ``ResiliencePolicy`` (breaker, retry, rate
limit, per-call timeout) and records a usage event from the provider's reported token
counts. Curation LLM calls are then protected and priced the same way the graph-path calls
already are, instead of hitting the provider raw and untracked.
"""

from __future__ import annotations

from typing import Any

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


class _MeteredCompletions:
    def __init__(self, inner: Any, policy: ResiliencePolicy, sink: UsageSink | None) -> None:
        self._inner = inner
        self._policy = policy
        self._sink = sink

    async def create(self, **kwargs: Any) -> Any:
        # Estimate prompt tokens up front so the rate limiter can charge the request before
        # the provider reports exact counts; the meter below prefers the reported numbers.
        messages = kwargs.get("messages", [])
        prompt_estimate = sum(estimate_tokens(str(m.get("content", "") or "")) for m in messages)
        model = str(kwargs.get("model", "unknown"))
        max_tokens = kwargs.setdefault("max_tokens", 16384)
        completion_limit = (
            max_tokens if isinstance(max_tokens, int) and not isinstance(max_tokens, bool) else -1
        )
        prompt_limit = maximum_prompt_tokens(
            str(message.get("content", "") or "") for message in messages
        )

        async def attempt() -> Any:
            return await guard_provider_call(
                lambda: self._inner.create(**kwargs),
                self._sink,
                model=model,
                operation="llm",
                prompt_token_limit=prompt_limit,
                completion_token_limit=completion_limit,
                timeout_exceptions=(APITimeoutError,),
            )

        response = await self._policy.call(attempt, tokens=prompt_estimate)

        async def record() -> Any:
            if getattr(response, "model", None) != model:
                raise UsageAccountingError("provider response model differs from reserved model")
            usage = getattr(response, "usage", None)
            prompt_value = getattr(usage, "prompt_tokens", None)
            completion_value = getattr(usage, "completion_tokens", None)
            if (
                isinstance(prompt_value, int)
                and not isinstance(prompt_value, bool)
                and prompt_value >= 0
                and isinstance(completion_value, int)
                and not isinstance(completion_value, bool)
                and completion_value >= 0
            ):
                prompt_tokens = prompt_value
                completion_tokens = completion_value
            else:
                prompt_tokens = prompt_estimate
                completion_tokens = 0
            event = build_usage_event(
                model=model,
                operation="llm",
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                exact_cost_usd=provider_reported_cost(response),
                # Aggregate token counts omit billing dimensions such as cached input.
                usage_complete=False,
            )
            await emit_usage(self._sink, event)
            if event.cost_complete:
                await settle_provider_call(
                    self._sink,
                    reserved_cost_usd=cost_usd(model, prompt_limit, completion_limit),
                    actual_cost_usd=event.cost_usd,
                )
            return response

        return await guard_provider_call(
            record,
            self._sink,
            model=model,
            operation="llm",
            reserve_budget=False,
        )


class _MeteredChat:
    def __init__(self, inner: Any, policy: ResiliencePolicy, sink: UsageSink | None) -> None:
        self.completions = _MeteredCompletions(inner.chat.completions, policy, sink)


class MeteredChatClient:
    """Wrap an ``AsyncOpenAI``-shaped client so ``chat.completions.create`` is resilient and
    metered. Only the surface the curation adapters use is proxied; the request context set
    by the worker (ingest) or search handler (rerank) attributes the cost.
    """

    def __init__(
        self,
        inner: Any,
        *,
        policy: ResiliencePolicy,
        sink: UsageSink | None,
    ) -> None:
        self._inner = inner
        self.chat = _MeteredChat(inner, policy, sink)
