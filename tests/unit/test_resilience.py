"""Circuit breaker, retry/timeout policy, and the in-process rate limiter."""

from __future__ import annotations

import asyncio

import pytest

from vera.adapters.resilience.breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
)
from vera.adapters.resilience.limiter import InProcessRateLimiter
from vera.adapters.resilience.policy import ResiliencePolicy
from vera.domain.ports.resilience import RateLimiter


class _NoLimiter:
    async def acquire(self, *, tokens: int = 0) -> None:
        return None


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _policy(
    *,
    limiter: RateLimiter | None = None,
    breaker: CircuitBreaker | None = None,
    retry_attempts: int = 3,
    per_call_timeout_s: float = 1.0,
) -> ResiliencePolicy:
    return ResiliencePolicy(
        limiter=limiter or _NoLimiter(),
        breaker=breaker or CircuitBreaker(name="t", failure_threshold=1000, reset_timeout_s=1.0),
        retry_attempts=retry_attempts,
        initial_backoff_s=0.0001,
        max_backoff_s=0.001,
        per_call_timeout_s=per_call_timeout_s,
    )


# ---------------------------------------------------------------- breaker ---


@pytest.mark.asyncio
async def test_breaker_opens_after_threshold_and_rejects_fast() -> None:
    clock = _Clock()
    breaker = CircuitBreaker(name="dep", failure_threshold=3, reset_timeout_s=30.0, monotonic=clock)
    for _ in range(3):
        await breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        await breaker.before_call()


@pytest.mark.asyncio
async def test_breaker_half_opens_after_cooldown_and_recovers() -> None:
    clock = _Clock()
    breaker = CircuitBreaker(name="dep", failure_threshold=2, reset_timeout_s=30.0, monotonic=clock)
    await breaker.record_failure()
    await breaker.record_failure()
    assert breaker.state is CircuitState.OPEN

    clock.advance(31.0)
    await breaker.before_call()  # cooldown elapsed: trial allowed
    assert breaker.state is CircuitState.HALF_OPEN
    await breaker.record_success()
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_breaker_reopens_when_trial_fails() -> None:
    clock = _Clock()
    breaker = CircuitBreaker(name="dep", failure_threshold=1, reset_timeout_s=10.0, monotonic=clock)
    await breaker.record_failure()
    clock.advance(11.0)
    await breaker.before_call()
    assert breaker.state is CircuitState.HALF_OPEN
    await breaker.record_failure()
    assert breaker.state is CircuitState.OPEN


# ----------------------------------------------------------------- policy ---


@pytest.mark.asyncio
async def test_policy_retries_until_success() -> None:
    calls = {"n": 0}

    async def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    result = await _policy(retry_attempts=5).call(flaky)
    assert result == "ok"
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_policy_opens_breaker_after_repeated_failures() -> None:
    breaker = CircuitBreaker(name="dep", failure_threshold=2, reset_timeout_s=30.0)
    policy = _policy(breaker=breaker, retry_attempts=1)

    async def always_fail() -> None:
        raise RuntimeError("provider down")

    with pytest.raises(RuntimeError):
        await policy.call(always_fail)
    with pytest.raises(RuntimeError):
        await policy.call(always_fail)
    assert breaker.state is CircuitState.OPEN
    # Now the breaker short-circuits without touching the dependency.
    with pytest.raises(CircuitOpenError):
        await policy.call(always_fail)


@pytest.mark.asyncio
async def test_policy_cancels_a_hung_call_and_retries() -> None:
    cancelled = {"n": 0}

    async def hang() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled["n"] += 1
            raise

    policy = _policy(retry_attempts=2, per_call_timeout_s=0.02)
    with pytest.raises(TimeoutError):
        await policy.call(hang)
    # Each attempt timed out and was cancelled, not left running.
    assert cancelled["n"] == 2


# ---------------------------------------------------------------- limiter ---


@pytest.mark.asyncio
async def test_request_bucket_forces_a_wait_when_exhausted() -> None:
    clock = _Clock()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(seconds)

    limiter = InProcessRateLimiter(
        requests_per_minute=60,  # capacity 60, refill 1/sec
        tokens_per_minute=6_000_000,
        clock=clock,
        sleep=fake_sleep,
    )
    for _ in range(60):
        await limiter.acquire()
    assert sleeps == []  # within capacity, no waiting

    await limiter.acquire()  # 61st: must wait ~1s for one unit to refill
    assert sleeps and sleeps[-1] == pytest.approx(1.0, abs=0.01)


@pytest.mark.asyncio
async def test_token_bucket_is_enforced_independently() -> None:
    clock = _Clock()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(seconds)

    limiter = InProcessRateLimiter(
        requests_per_minute=1_000_000,
        tokens_per_minute=6000,  # capacity 6000, refill 100/sec
        clock=clock,
        sleep=fake_sleep,
    )
    await limiter.acquire(tokens=6000)  # drains the token bucket
    assert sleeps == []
    await limiter.acquire(tokens=6000)  # needs a full refill: 6000 / 100 = 60s
    assert sleeps and sleeps[-1] == pytest.approx(60.0, abs=0.1)
