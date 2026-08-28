"""The curation LLM calls are resilience-wrapped and cost-metered (fake client + sink)."""

from __future__ import annotations

from typing import Any

import pytest

from vera.adapters.curation.metered_client import MeteredChatClient
from vera.adapters.resilience.breaker import CircuitBreaker
from vera.adapters.resilience.limiter import InProcessRateLimiter
from vera.adapters.resilience.policy import ResiliencePolicy
from vera.observability.cost import UsageEvent


class _Usage:
    def __init__(self, prompt: int, completion: int) -> None:
        self.prompt_tokens = prompt
        self.completion_tokens = completion


class _Response:
    def __init__(self, usage: _Usage | None) -> None:
        self.usage = usage
        self.choices: list[Any] = []


class _FakeCompletions:
    def __init__(self, *, fail_times: int = 0, usage: _Usage | None) -> None:
        self.calls = 0
        self._fail_times = fail_times
        self._usage = usage

    async def create(self, **_kwargs: Any) -> _Response:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise RuntimeError("transient provider error")
        return _Response(self._usage)


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


@pytest.mark.asyncio
async def test_retries_transient_failures_then_meters_once() -> None:
    # Two failures, then success: the resilience policy retries, and exactly one usage
    # event is recorded (for the successful call), proving the wrapper is in the path.
    completions = _FakeCompletions(fail_times=2, usage=_Usage(10, 5))
    sink = _CapturingSink()
    client = MeteredChatClient(_FakeInner(completions), policy=_policy(), sink=sink)

    await client.chat.completions.create(
        model="gpt-4.1-nano", messages=[{"role": "user", "content": "hi"}]
    )

    assert completions.calls == 3  # 2 failures + 1 success, retried by the policy
    assert len(sink.events) == 1


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
