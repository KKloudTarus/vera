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

from vera.adapters.resilience.policy import ResiliencePolicy
from vera.observability.cost import (
    UsageSink,
    build_usage_event,
    emit_usage,
    estimate_tokens,
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

        async def _raw() -> Any:
            return await self._inner.create(**kwargs)

        response = await self._policy.call(_raw, tokens=prompt_estimate)

        usage = getattr(response, "usage", None)
        prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0) or prompt_estimate
        completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
        event = build_usage_event(
            model=model,
            operation="llm",
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )
        await emit_usage(self._sink, event)
        return response


class _MeteredChat:
    def __init__(self, inner: Any, policy: ResiliencePolicy, sink: UsageSink | None) -> None:
        self.completions = _MeteredCompletions(inner.chat.completions, policy, sink)


class MeteredChatClient:
    """Wrap an ``AsyncOpenAI``-shaped client so ``chat.completions.create`` is resilient and
    metered. Only the surface the curation adapters use is proxied; the request context set
    by the worker (ingest) or search handler (rerank) attributes the cost.
    """

    def __init__(self, inner: Any, *, policy: ResiliencePolicy, sink: UsageSink | None) -> None:
        self._inner = inner
        self.chat = _MeteredChat(inner, policy, sink)
