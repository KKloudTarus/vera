"""LLM cost tracking: attribute provider token usage to an episode or a query.

A ``contextvars`` context, set by the worker before ingest and by the search handler
before a query, rides across await boundaries so the instrumented provider clients can
tag each usage event with its group and request kind without threading arguments
through Graphiti. Events go to a sink (a row in ``llm_usage``) and to Prometheus, so
cost per episode and per query is both queryable and dashboarded.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Protocol

from vera.observability.metrics import record_llm_usage

# USD per one million tokens, (prompt, completion). Embedding and rerank models bill prompt
# only. Every provider VERA can use is priced here so cost tracking is provider-neutral;
# an unknown model costs 0 (metered, not priced). Keep in sync with the configured models.
_PRICES_PER_MTOK: dict[str, tuple[float, float]] = {
    # OpenAI
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1-nano": (0.10, 0.40),
    "gpt-4o-mini": (0.15, 0.60),
    "text-embedding-3-small": (0.02, 0.0),
    "text-embedding-3-large": (0.13, 0.0),
    # Voyage AI (embeddings and rerankers)
    "voyage-3.5": (0.06, 0.0),
    "voyage-3.5-lite": (0.02, 0.0),
    "voyage-4": (0.06, 0.0),
    "voyage-4-lite": (0.02, 0.0),
    "voyage-4-large": (0.12, 0.0),
    "voyage-code-4": (0.12, 0.0),
    "voyage-context-4": (0.12, 0.0),
    "rerank-2.5": (0.05, 0.0),
    "rerank-2.5-lite": (0.02, 0.0),
}


def cost_usd(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    prompt_price, completion_price = _PRICES_PER_MTOK.get(model, (0.0, 0.0))
    return (prompt_tokens * prompt_price + completion_tokens * completion_price) / 1_000_000


@dataclass(frozen=True, slots=True)
class UsageContext:
    """What a provider call is being made for. ``request_kind`` is 'ingest' or 'search'."""

    request_kind: str
    group_id: str | None = None
    ref: str | None = None  # source_id for ingest; the query is not stored


@dataclass(frozen=True, slots=True)
class UsageEvent:
    model: str
    operation: str  # 'llm' (including reranking) or 'embedding'
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
    request_kind: str
    group_id: str | None
    ref: str | None


class UsageSink(Protocol):
    async def record(self, event: UsageEvent) -> None: ...


_usage_context: ContextVar[UsageContext | None] = ContextVar("vera_usage_context", default=None)


def current_usage_context() -> UsageContext | None:
    return _usage_context.get()


def set_usage_context(context: UsageContext) -> object:
    """Bind the usage context for the current task. Returns a token for ``reset``."""
    return _usage_context.set(context)


def reset_usage_context(token: object) -> None:
    _usage_context.reset(token)  # type: ignore[arg-type]


def build_usage_event(
    *, model: str, operation: str, prompt_tokens: int, completion_tokens: int
) -> UsageEvent:
    context = current_usage_context()
    return UsageEvent(
        model=model,
        operation=operation,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        cost_usd=cost_usd(model, prompt_tokens, completion_tokens),
        request_kind=context.request_kind if context else "unknown",
        group_id=context.group_id if context else None,
        ref=context.ref if context else None,
    )


async def emit_usage(sink: UsageSink | None, event: UsageEvent) -> None:
    """Record an event to Prometheus and, if configured, the durable sink."""
    record_llm_usage(
        model=event.model,
        operation=event.operation,
        prompt_tokens=event.prompt_tokens,
        completion_tokens=event.completion_tokens,
        cost_usd=event.cost_usd,
    )
    if sink is not None:
        await sink.record(event)


def estimate_tokens(text: str) -> int:
    """A cheap, provider-agnostic token estimate (~4 chars per token) for metering
    when a provider does not return a usage count (e.g. the offline embedder).
    """
    return max(1, (len(text) + 3) // 4)
