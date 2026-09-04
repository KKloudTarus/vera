"""LLM cost tracking: attribute provider token usage to an episode or a query.

A ``contextvars`` context, set by the worker before ingest and by the search handler
before a query, rides across await boundaries so the instrumented provider clients can
tag each usage event with its group and request kind without threading arguments
through Graphiti. Events go to a sink (a row in ``llm_usage``) and to Prometheus, so
cost per episode and per query is both queryable and dashboarded.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable, Iterable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Protocol, TypeVar, cast

from vera.observability.metrics import record_llm_usage
from vera.shared.types import JsonDict

_T = TypeVar("_T")

# USD per one million tokens, (prompt, completion). Embedding and rerank models bill prompt
# only. Routed evaluation models use deliberately high policy ceilings so reservations cannot
# understate spend when the provider reports the exact charge after dispatch.
_PRICES_PER_MTOK: dict[str, tuple[float, float]] = {
    # OpenAI
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1-nano": (0.10, 0.40),
    "gpt-4o-mini": (0.15, 0.60),
    "codex/gpt-5.6-sol": (100.0, 100.0),
    "codex/gpt-5.6-terra": (100.0, 100.0),
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


def model_price_known(model: str) -> bool:
    return model in _PRICES_PER_MTOK


def provider_reported_cost(value: object) -> float | None:
    nested_usage: object = (
        cast("dict[str, object]", value).get("usage")
        if isinstance(value, dict)
        else getattr(value, "usage", None)
    )
    candidates: tuple[object, object] = (cast("object", value), nested_usage)
    for candidate in candidates:
        if isinstance(candidate, dict):
            values = cast("dict[str, object]", candidate)
            raw: object = values.get("cost_usd")
        else:
            raw = getattr(candidate, "cost_usd", None)
        if (
            isinstance(raw, (int, float))
            and not isinstance(raw, bool)
            and math.isfinite(raw)
            and raw >= 0
        ):
            return float(raw)
    return None


def cost_usd(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    prompt_price, completion_price = _PRICES_PER_MTOK.get(model, (0.0, 0.0))
    return (prompt_tokens * prompt_price + completion_tokens * completion_price) / 1_000_000


def _is_nonnegative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


@dataclass(frozen=True, slots=True)
class UsageContext:
    """What a provider call is being made for. ``request_kind`` is 'ingest' or 'search'."""

    request_kind: str
    group_id: str | None = None
    ref: str | None = None  # source_id for ingest; the query is not stored
    job_id: str | None = None
    claim_token: str | None = None


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
    cost_complete: bool = True


class UsageSink(Protocol):
    async def record(self, event: UsageEvent) -> None: ...

    async def fence_provider_attempt(self, job_id: str, claim_token: str) -> None: ...

    async def initialize_provider_budget(
        self,
        action_key: str,
        max_cost_usd: float,
        *,
        run_key: str,
        run_max_cost_usd: float,
    ) -> None: ...

    async def reserve_provider_budget(self, action_key: str, max_cost_usd: float) -> bool: ...

    async def settle_provider_budget(
        self, action_key: str, reserved_cost_usd: float, actual_cost_usd: float
    ) -> bool: ...


class UsageAccountingError(RuntimeError):
    """A paid provider response cannot be accounted for safely."""


class UsagePersistenceError(UsageAccountingError):
    """Durable metering failed, so repeating the paid operation is unsafe."""


class ProviderCallTimeoutError(UsageAccountingError, TimeoutError):
    """A provider timed out after dispatch, so repeating it may duplicate spend."""


class ProviderBudgetExceededError(UsageAccountingError):
    """A provider call cannot fit within its action's durable cost reservation."""


@dataclass(frozen=True, slots=True)
class ProviderBudgetContext:
    action_key: str


_usage_context: ContextVar[UsageContext | None] = ContextVar("vera_usage_context", default=None)
_provider_budget_context: ContextVar[ProviderBudgetContext | None] = ContextVar(
    "vera_provider_budget_context", default=None
)


def current_usage_context() -> UsageContext | None:
    return _usage_context.get()


def set_usage_context(context: UsageContext) -> object:
    """Bind the usage context for the current task. Returns a token for ``reset``."""
    return _usage_context.set(context)


def reset_usage_context(token: object) -> None:
    _usage_context.reset(token)  # type: ignore[arg-type]


def current_provider_budget_context() -> ProviderBudgetContext | None:
    return _provider_budget_context.get()


def set_provider_budget_context(context: ProviderBudgetContext) -> object:
    return _provider_budget_context.set(context)


def reset_provider_budget_context(token: object) -> None:
    _provider_budget_context.reset(token)  # type: ignore[arg-type]


def maximum_prompt_tokens(values: Iterable[str]) -> int:
    """Conservatively bound tokenization by UTF-8 bytes plus chat framing overhead."""
    return 1024 + sum(len(value.encode("utf-8")) for value in values)


def provider_budget_trace_context(trace_context: JsonDict | None) -> JsonDict:
    result = dict(trace_context or {})
    result.pop("_provider_budget_key", None)
    budget = current_provider_budget_context()
    if budget is not None:
        result["_provider_budget_key"] = budget.action_key
    return result


async def reserve_provider_call(
    sink: UsageSink | None,
    *,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> float | None:
    budget = current_provider_budget_context()
    if budget is None:
        return None
    if sink is None:
        raise ProviderBudgetExceededError("provider budget has no durable reservation sink")
    if not model_price_known(model):
        raise ProviderBudgetExceededError(f"provider model {model!r} has no price ceiling")
    if not _is_nonnegative_int(prompt_tokens) or not _is_nonnegative_int(completion_tokens):
        raise ProviderBudgetExceededError("provider token ceiling is invalid")
    maximum_cost = cost_usd(model, prompt_tokens, completion_tokens)
    if not math.isfinite(maximum_cost) or maximum_cost <= 0:
        raise ProviderBudgetExceededError("provider cost ceiling is invalid")
    if not await sink.reserve_provider_budget(budget.action_key, maximum_cost):
        raise ProviderBudgetExceededError(
            f"provider call cannot fit action budget: {maximum_cost:.6f} USD requested"
        )
    return maximum_cost


async def settle_provider_call(
    sink: UsageSink | None,
    *,
    reserved_cost_usd: float | None,
    actual_cost_usd: float,
) -> None:
    budget = current_provider_budget_context()
    if budget is None or reserved_cost_usd is None:
        return
    if sink is None:
        raise UsagePersistenceError("provider budget settlement has no durable sink")
    if (
        not math.isfinite(actual_cost_usd)
        or actual_cost_usd < 0
        or actual_cost_usd > reserved_cost_usd
    ):
        raise UsageAccountingError("provider actual cost exceeds its conservative reservation")
    try:
        settled = await sink.settle_provider_budget(
            budget.action_key, reserved_cost_usd, actual_cost_usd
        )
    except Exception as exc:
        raise UsagePersistenceError("durable provider budget settlement failed") from exc
    if not settled:
        raise UsagePersistenceError("durable provider budget settlement was rejected")


async def _refund_pre_dispatch_reservation(
    sink: UsageSink | None, reserved_cost_usd: float
) -> None:
    settlement = asyncio.create_task(
        settle_provider_call(
            sink,
            reserved_cost_usd=reserved_cost_usd,
            actual_cost_usd=0.0,
        )
    )
    try:
        await asyncio.shield(settlement)
    except asyncio.CancelledError:
        await settlement
        raise


def build_usage_event(
    *,
    model: str,
    operation: str,
    prompt_tokens: int,
    completion_tokens: int,
    exact_cost_usd: float | None = None,
    usage_complete: bool = False,
) -> UsageEvent:
    if not _is_nonnegative_int(prompt_tokens) or not _is_nonnegative_int(completion_tokens):
        raise ValueError("provider token counts must be non-negative integers")
    if exact_cost_usd is not None and (
        isinstance(exact_cost_usd, bool) or not math.isfinite(exact_cost_usd) or exact_cost_usd < 0
    ):
        raise ValueError("exact provider cost must be finite and non-negative")
    context = current_usage_context()
    complete = exact_cost_usd is not None or (usage_complete and model_price_known(model))
    event_cost = (
        float(exact_cost_usd)
        if exact_cost_usd is not None
        else cost_usd(model, prompt_tokens, completion_tokens)
    )
    if not math.isfinite(event_cost) or event_cost < 0:
        raise ValueError("calculated provider cost must be finite and non-negative")
    return UsageEvent(
        model=model,
        operation=operation,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        cost_usd=event_cost,
        request_kind=context.request_kind if context else "unknown",
        group_id=context.group_id if context else None,
        ref=context.ref if context else None,
        cost_complete=complete,
    )


async def mark_usage_incomplete(sink: UsageSink | None, *, model: str, operation: str) -> None:
    if sink is None:
        return
    context = current_usage_context()
    try:
        await sink.record(
            UsageEvent(
                model=model,
                operation=operation,
                prompt_tokens=0,
                completion_tokens=0,
                cost_usd=0.0,
                request_kind=context.request_kind if context else "unknown",
                group_id=context.group_id if context else None,
                ref=context.ref if context else None,
                cost_complete=False,
            )
        )
    except Exception as exc:
        raise UsagePersistenceError("durable provider usage marker failed") from exc


async def guard_provider_call(
    call: Callable[[], Awaitable[_T]],
    sink: UsageSink | None,
    *,
    model: str,
    operation: str,
    prompt_token_limit: int | None = None,
    completion_token_limit: int | None = None,
    reserve_budget: bool = True,
    timeout_exceptions: tuple[type[Exception], ...] = (),
) -> _T:
    context = current_usage_context()
    reserved_cost_usd: float | None = None
    if reserve_budget and current_provider_budget_context() is not None:
        if prompt_token_limit is None or completion_token_limit is None:
            raise ProviderBudgetExceededError("provider call omitted its token ceiling")
        reserved_cost_usd = await reserve_provider_call(
            sink,
            model=model,
            prompt_tokens=prompt_token_limit,
            completion_tokens=completion_token_limit,
        )
    if (
        sink is not None
        and context is not None
        and context.job_id is not None
        and context.claim_token is not None
    ):
        try:
            await sink.fence_provider_attempt(context.job_id, context.claim_token)
        except asyncio.CancelledError:
            if reserved_cost_usd is not None:
                await _refund_pre_dispatch_reservation(sink, reserved_cost_usd)
            raise
        except Exception as exc:
            if reserved_cost_usd is not None:
                await _refund_pre_dispatch_reservation(sink, reserved_cost_usd)
            raise UsagePersistenceError("durable provider attempt fence failed") from exc
    try:
        return await call()
    except TimeoutError as exc:
        await asyncio.shield(mark_usage_incomplete(sink, model=model, operation=operation))
        raise ProviderCallTimeoutError(str(exc)) from exc
    except asyncio.CancelledError:
        await asyncio.shield(mark_usage_incomplete(sink, model=model, operation=operation))
        raise
    except Exception as exc:
        await asyncio.shield(mark_usage_incomplete(sink, model=model, operation=operation))
        if isinstance(exc, timeout_exceptions):
            raise ProviderCallTimeoutError(str(exc)) from exc
        raise


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
        try:
            await sink.record(event)
        except Exception as exc:
            raise UsagePersistenceError("durable provider usage event failed") from exc


def estimate_tokens(text: str) -> int:
    """A cheap, provider-agnostic token estimate (~4 chars per token) for metering
    when a provider does not return a usage count (e.g. the offline embedder).
    """
    return max(1, (len(text) + 3) // 4)
