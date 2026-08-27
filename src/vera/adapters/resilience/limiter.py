"""Rate limiters: dual token buckets for requests-per-minute and tokens-per-minute.

A provider call must respect two independent budgets, so ``acquire`` waits until both
the request bucket (one unit per call) and the token bucket (the call's token cost)
have room, then consumes from both. Two backends implement the same port: an
in-process limiter for a single replica, and a Valkey-backed one that shares the
buckets across replicas via an atomic Lua script.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

_REFILL_PER_SECOND = 60.0  # per-minute limits refill continuously each second


class _Bucket:
    """A classic token bucket: capacity refilled at ``rate`` units per second."""

    def __init__(self, capacity: float, rate_per_second: float, clock: Callable[[], float]) -> None:
        self._capacity = capacity
        self._rate = rate_per_second
        self._clock = clock
        self._tokens = capacity
        self._updated = clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._updated = now

    def time_until(self, amount: float) -> float:
        """Seconds until ``amount`` units are available (0 if available now)."""
        self._refill()
        if self._tokens >= amount:
            return 0.0
        needed = min(amount, self._capacity) - self._tokens
        return needed / self._rate if self._rate > 0 else float("inf")

    def consume(self, amount: float) -> None:
        self._refill()
        self._tokens -= amount


class InProcessRateLimiter:
    """Single-replica limiter. A lock serializes bucket math so concurrent callers do
    not oversubscribe; each waits out the longer of the two budgets before consuming.
    """

    def __init__(
        self,
        *,
        requests_per_minute: int,
        tokens_per_minute: int,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._clock = clock or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._requests = _Bucket(
            requests_per_minute, requests_per_minute / _REFILL_PER_SECOND, self._clock
        )
        self._tokens = _Bucket(
            tokens_per_minute, tokens_per_minute / _REFILL_PER_SECOND, self._clock
        )
        self._lock = asyncio.Lock()

    async def acquire(self, *, tokens: int = 0) -> None:
        while True:
            async with self._lock:
                wait = max(self._requests.time_until(1), self._tokens.time_until(tokens))
                if wait <= 0:
                    self._requests.consume(1)
                    self._tokens.consume(tokens)
                    return
            await self._sleep(wait)


# Atomically refill both buckets and, if both have room, consume; else report the wait.
# KEYS: request bucket, token bucket. ARGV: rpm, tpm, token_cost, now_ms.
_LUA_ACQUIRE = """
local function step(key, capacity, rate_ms, cost, now)
  local state = redis.call('HMGET', key, 'tokens', 'ts')
  local tokens = tonumber(state[1])
  local ts = tonumber(state[2])
  if tokens == nil then tokens = capacity; ts = now end
  local elapsed = math.max(0, now - ts)
  tokens = math.min(capacity, tokens + elapsed * rate_ms)
  return tokens, cost
end
local rpm = tonumber(ARGV[1])
local tpm = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local req_rate = rpm / 60000.0
local tok_rate = tpm / 60000.0
local req_tokens = step(KEYS[1], rpm, req_rate, 1, now)
local tok_tokens = step(KEYS[2], tpm, tok_rate, cost, now)
local req_wait = 0.0
if req_tokens < 1 then req_wait = (1 - req_tokens) / req_rate end
local tok_wait = 0.0
if cost > 0 and tok_tokens < cost then tok_wait = (cost - tok_tokens) / tok_rate end
local wait = math.max(req_wait, tok_wait)
if wait <= 0 then
  redis.call('HMSET', KEYS[1], 'tokens', req_tokens - 1, 'ts', now)
  redis.call('HMSET', KEYS[2], 'tokens', tok_tokens - cost, 'ts', now)
  redis.call('PEXPIRE', KEYS[1], 120000)
  redis.call('PEXPIRE', KEYS[2], 120000)
  return 0
end
return math.ceil(wait)
"""


class ValkeyRateLimiter:
    """Distributed limiter sharing buckets across replicas via an atomic Lua script.

    Speaks the RESP protocol, so it works against Valkey or any redis-compatible server.
    """

    def __init__(
        self,
        client: object,
        *,
        provider: str,
        requests_per_minute: int,
        tokens_per_minute: int,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._client = client
        self._rpm = requests_per_minute
        self._tpm = tokens_per_minute
        self._sleep = sleep or asyncio.sleep
        self._keys = [f"vera:ratelimit:{provider}:req", f"vera:ratelimit:{provider}:tok"]

    async def acquire(self, *, tokens: int = 0) -> None:
        from redis.asyncio import Redis

        client: Redis = self._client  # type: ignore[type-arg]
        while True:
            now_ms = int(time.time() * 1000)
            wait_ms = await client.eval(  # pyright: ignore[reportUnknownMemberType]
                _LUA_ACQUIRE, 2, *self._keys, self._rpm, self._tpm, tokens, now_ms
            )
            if int(wait_ms) <= 0:
                return
            await self._sleep(int(wait_ms) / 1000.0)
