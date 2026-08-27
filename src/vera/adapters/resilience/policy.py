"""ResiliencePolicy: breaker over retry over limiter over a timed call.

Composition, outermost first:
- the circuit breaker guards the whole call and fails fast when the dependency is down;
- tenacity retries transient failures with full-jitter backoff;
- each attempt first passes the rate limiter, then runs under a per-call deadline, so a
  hung call is cancelled and retried rather than pinning the caller (an ingestion lane).

A retry sequence that ultimately fails records one breaker failure; a success closes it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from vera.adapters.resilience.breaker import CircuitBreaker, CircuitOpenError
from vera.config.settings import ResilienceSettings
from vera.domain.ports.resilience import RateLimiter

_T = TypeVar("_T")


class ResiliencePolicy:
    def __init__(
        self,
        *,
        limiter: RateLimiter,
        breaker: CircuitBreaker,
        retry_attempts: int,
        initial_backoff_s: float,
        max_backoff_s: float,
        per_call_timeout_s: float,
    ) -> None:
        self._limiter = limiter
        self._breaker = breaker
        self._retry_attempts = retry_attempts
        self._initial_backoff_s = initial_backoff_s
        self._max_backoff_s = max_backoff_s
        self._per_call_timeout_s = per_call_timeout_s

    async def call(self, fn: Callable[[], Awaitable[_T]], *, tokens: int = 0) -> _T:
        await self._breaker.before_call()  # raises CircuitOpenError if open
        try:
            result = await self._retry(fn, tokens)
        except CircuitOpenError:
            raise
        except Exception:
            await self._breaker.record_failure()
            raise
        await self._breaker.record_success()
        return result

    async def _retry(self, fn: Callable[[], Awaitable[_T]], tokens: int) -> _T:
        retrying = AsyncRetrying(
            stop=stop_after_attempt(self._retry_attempts),
            wait=wait_random_exponential(
                multiplier=self._initial_backoff_s, max=self._max_backoff_s
            ),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        async for attempt in retrying:
            with attempt:
                return await self._attempt(fn, tokens)
        raise AssertionError("unreachable: reraise=True re-raises the last error")

    async def _attempt(self, fn: Callable[[], Awaitable[_T]], tokens: int) -> _T:
        await self._limiter.acquire(tokens=tokens)
        async with asyncio.timeout(self._per_call_timeout_s):
            return await fn()


def build_rate_limiter(settings: ResilienceSettings, *, provider: str) -> RateLimiter:
    if settings.valkey_url:
        from redis.asyncio import Redis

        from vera.adapters.resilience.limiter import ValkeyRateLimiter

        client = Redis.from_url(settings.valkey_url)  # pyright: ignore[reportUnknownMemberType]
        return ValkeyRateLimiter(
            client,
            provider=provider,
            requests_per_minute=settings.requests_per_minute,
            tokens_per_minute=settings.tokens_per_minute,
        )
    from vera.adapters.resilience.limiter import InProcessRateLimiter

    return InProcessRateLimiter(
        requests_per_minute=settings.requests_per_minute,
        tokens_per_minute=settings.tokens_per_minute,
    )


def build_resilience_policy(
    settings: ResilienceSettings, *, name: str, limiter: RateLimiter | None = None
) -> ResiliencePolicy:
    breaker = CircuitBreaker(
        name=name,
        failure_threshold=settings.breaker_failure_threshold,
        reset_timeout_s=settings.breaker_reset_timeout_s,
    )
    return ResiliencePolicy(
        limiter=limiter or build_rate_limiter(settings, provider=name),
        breaker=breaker,
        retry_attempts=settings.retry_attempts,
        initial_backoff_s=settings.retry_initial_backoff_s,
        max_backoff_s=settings.retry_max_backoff_s,
        per_call_timeout_s=settings.per_call_timeout_s,
    )
