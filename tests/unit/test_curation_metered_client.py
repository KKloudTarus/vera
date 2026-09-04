"""The curation LLM calls are resilience-wrapped and cost-metered (fake client + sink)."""

from __future__ import annotations

from typing import Any

import pytest

from vera.adapters.curation.metered_client import MeteredChatClient
from vera.adapters.resilience.breaker import CircuitBreaker
from vera.adapters.resilience.limiter import InProcessRateLimiter
from vera.adapters.resilience.policy import ResiliencePolicy
from vera.observability.cost import (
    ProviderBudgetContext,
    ProviderBudgetExceededError,
    UsageEvent,
    UsagePersistenceError,
    reset_provider_budget_context,
    set_provider_budget_context,
)


class _Usage:
    def __init__(self, prompt: int, completion: int) -> None:
        self.prompt_tokens = prompt
        self.completion_tokens = completion


class _Response:
    def __init__(self, usage: _Usage | None, *, model: str, cost_usd: float | None = None) -> None:
        self.usage = usage
        self.model = model
        self.cost_usd = cost_usd
        self.choices: list[Any] = []


class _FakeCompletions:
    def __init__(
        self, *, fail_times: int = 0, usage: _Usage | None, cost_usd: float | None = None
    ) -> None:
        self.calls = 0
        self._fail_times = fail_times
        self._usage = usage
        self._cost_usd = cost_usd

    async def create(self, **kwargs: Any) -> _Response:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise RuntimeError("transient provider error")
        return _Response(self._usage, model=str(kwargs["model"]), cost_usd=self._cost_usd)


class _FakeInner:
    def __init__(self, completions: _FakeCompletions) -> None:
        self.chat = type("Chat", (), {"completions": completions})()


class _CapturingSink:
    def __init__(self) -> None:
        self.events: list[UsageEvent] = []

    async def record(self, event: UsageEvent) -> None:
        self.events.append(event)


def _policy() -> ResiliencePolicy:
    return ResiliencePolicy(
        limiter=InProcessRateLimiter(requests_per_minute=1000, tokens_per_minute=1_000_000),
        breaker=CircuitBreaker(name="test", failure_threshold=5, reset_timeout_s=1.0),
        retry_attempts=3,
        initial_backoff_s=0.0,
        max_backoff_s=0.0,
        per_call_timeout_s=5.0,
    )


@pytest.mark.asyncio
async def test_meters_reported_tokens() -> None:
    completions = _FakeCompletions(usage=_Usage(120, 30))
    sink = _CapturingSink()
    client = MeteredChatClient(_FakeInner(completions), policy=_policy(), sink=sink)

    await client.chat.completions.create(
        model="gpt-4.1-nano", messages=[{"role": "user", "content": "hi"}]
    )

    assert len(sink.events) == 1
    event = sink.events[0]
    assert event.model == "gpt-4.1-nano"
    assert event.operation == "llm"
    assert event.prompt_tokens == 120
    assert event.completion_tokens == 30
    assert event.cost_complete is False


@pytest.mark.asyncio
async def test_retries_transient_failures_then_meters_once() -> None:
    # Failed attempts have ambiguous cost and remain visible even when a retry succeeds.
    completions = _FakeCompletions(fail_times=2, usage=_Usage(10, 5))
    sink = _CapturingSink()
    client = MeteredChatClient(_FakeInner(completions), policy=_policy(), sink=sink)

    await client.chat.completions.create(
        model="gpt-4.1-nano", messages=[{"role": "user", "content": "hi"}]
    )

    assert completions.calls == 3  # 2 failures + 1 success, retried by the policy
    assert len(sink.events) == 3
    assert [event.cost_complete for event in sink.events] == [False, False, False]


@pytest.mark.asyncio
async def test_falls_back_to_estimate_when_provider_omits_usage() -> None:
    completions = _FakeCompletions(usage=None)
    sink = _CapturingSink()
    client = MeteredChatClient(_FakeInner(completions), policy=_policy(), sink=sink)

    await client.chat.completions.create(
        model="gpt-4.1-nano",
        messages=[{"role": "user", "content": "some words to estimate tokens from"}],
    )

    assert len(sink.events) == 1
    assert sink.events[0].prompt_tokens > 0  # estimated from the message content
    assert sink.events[0].cost_complete is False


@pytest.mark.asyncio
async def test_preserves_exact_cost_for_an_unpriced_model() -> None:
    completions = _FakeCompletions(usage=_Usage(10, 5), cost_usd=0.125)
    sink = _CapturingSink()
    client = MeteredChatClient(_FakeInner(completions), policy=_policy(), sink=sink)

    await client.chat.completions.create(
        model="future-model", messages=[{"role": "user", "content": "hi"}]
    )

    assert sink.events[0].cost_usd == 0.125
    assert sink.events[0].cost_complete is True


@pytest.mark.asyncio
async def test_usage_sink_failure_does_not_repeat_a_successful_provider_call() -> None:
    completions = _FakeCompletions(usage=_Usage(10, 5), cost_usd=0.125)

    class FailingSink:
        async def record(self, _event: UsageEvent) -> None:
            raise RuntimeError("database unavailable")

    client = MeteredChatClient(_FakeInner(completions), policy=_policy(), sink=FailingSink())

    with pytest.raises(UsagePersistenceError, match="durable provider usage marker failed"):
        await client.chat.completions.create(
            model="future-model", messages=[{"role": "user", "content": "hi"}]
        )

    assert completions.calls == 1


@pytest.mark.asyncio
async def test_action_budget_rejects_completion_before_provider_dispatch() -> None:
    completions = _FakeCompletions(usage=_Usage(10, 5))

    class RejectingSink(_CapturingSink):
        async def reserve_provider_budget(self, action_key: str, max_cost_usd: float) -> bool:
            assert action_key == "run:case:step"
            assert max_cost_usd > 0
            return False

    client = MeteredChatClient(_FakeInner(completions), policy=_policy(), sink=RejectingSink())
    token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        with pytest.raises(ProviderBudgetExceededError, match="cannot fit action budget"):
            await client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[{"role": "user", "content": "bounded request"}],
            )
    finally:
        reset_provider_budget_context(token)

    assert completions.calls == 0


@pytest.mark.asyncio
async def test_rejects_provider_model_substitution() -> None:
    completions = _FakeCompletions(usage=_Usage(10, 5))

    async def substitute(**_kwargs: Any) -> _Response:
        completions.calls += 1
        return _Response(_Usage(10, 5), model="substituted-model")

    completions.create = substitute  # type: ignore[method-assign]
    client = MeteredChatClient(_FakeInner(completions), policy=_policy(), sink=_CapturingSink())

    with pytest.raises(RuntimeError, match="differs from reserved model"):
        await client.chat.completions.create(
            model="gpt-4.1-mini", messages=[{"role": "user", "content": "hi"}]
        )

    assert completions.calls == 1


@pytest.mark.asyncio
async def test_provider_reported_cost_settles_the_successful_call() -> None:
    completions = _FakeCompletions(usage=_Usage(10, 5), cost_usd=0.00001)

    class BudgetSink(_CapturingSink):
        def __init__(self) -> None:
            super().__init__()
            self.reserved: list[tuple[str, float]] = []
            self.settled: list[tuple[str, float, float]] = []

        async def reserve_provider_budget(self, action_key: str, max_cost_usd: float) -> bool:
            self.reserved.append((action_key, max_cost_usd))
            return True

        async def settle_provider_budget(
            self, action_key: str, reserved_cost_usd: float, actual_cost_usd: float
        ) -> bool:
            self.settled.append((action_key, reserved_cost_usd, actual_cost_usd))
            return True

    sink = BudgetSink()
    client = MeteredChatClient(
        _FakeInner(completions),
        policy=_policy(),
        sink=sink,
    )
    token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        await client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": "bounded request"}],
            max_tokens=100,
        )
    finally:
        reset_provider_budget_context(token)

    assert sink.events[0].cost_complete is True
    assert sink.reserved == [("run:case:step", sink.settled[0][1])]
    assert sink.settled[0][0] == "run:case:step"
    assert sink.settled[0][2] == sink.events[0].cost_usd
