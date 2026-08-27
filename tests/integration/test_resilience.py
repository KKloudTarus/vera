"""The Valkey-backed rate limiter against the live Valkey (compose).

Proves the distributed limiter enforces the request budget and shares one bucket across
instances (as separate replicas would). To keep the test fast, an injected sleep raises
a sentinel the moment the limiter decides it must wait, so no real backoff elapses.
"""

from __future__ import annotations

import os

import pytest

from vera.adapters.resilience.limiter import ValkeyRateLimiter
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_VALKEY_URL = os.environ.get("VERA_RESILIENCE__VALKEY_URL", "redis://localhost:6379/0")


class _WaitRequested(Exception):
    pass


async def _client() -> object:
    from redis.asyncio import Redis

    client = Redis.from_url(_VALKEY_URL)
    try:
        await client.ping()
    except Exception:
        pytest.skip("Valkey not reachable")
    return client


async def test_valkey_limiter_enforces_the_request_budget() -> None:
    client = await _client()
    provider = f"test-{uuid7().hex[:12]}"
    waits: list[float] = []

    async def capture_sleep(seconds: float) -> None:
        waits.append(seconds)
        raise _WaitRequested

    limiter = ValkeyRateLimiter(
        client,
        provider=provider,
        requests_per_minute=60,  # capacity 60, refill 1/sec
        tokens_per_minute=6_000_000,
        sleep=capture_sleep,
    )
    try:
        for _ in range(60):
            await limiter.acquire()  # within capacity, returns immediately
        assert waits == []
        with pytest.raises(_WaitRequested):
            await limiter.acquire()  # 61st is over budget -> a wait is requested
        assert waits and waits[0] > 0
    finally:
        await client.aclose()  # pyright: ignore[reportAttributeAccessIssue]


async def test_valkey_limiter_is_shared_across_instances() -> None:
    client = await _client()
    provider = f"test-{uuid7().hex[:12]}"
    waits: list[float] = []

    async def capture_sleep(seconds: float) -> None:
        waits.append(seconds)
        raise _WaitRequested

    def _limiter() -> ValkeyRateLimiter:
        return ValkeyRateLimiter(
            client,
            provider=provider,  # same key => one shared bucket
            requests_per_minute=60,
            tokens_per_minute=6_000_000,
            sleep=capture_sleep,
        )

    replica_a, replica_b = _limiter(), _limiter()
    try:
        for _ in range(30):
            await replica_a.acquire()
        for _ in range(30):
            await replica_b.acquire()
        # The shared bucket is now empty, so either replica must wait.
        with pytest.raises(_WaitRequested):
            await replica_b.acquire()
        assert waits and waits[0] > 0
    finally:
        await client.aclose()  # pyright: ignore[reportAttributeAccessIssue]
