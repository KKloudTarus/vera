"""Fixed-window quota counters for per-principal, per-tool abuse control.

The MCP surface needs to reject an abusive caller fast, not queue it, and persisted
context packs and snapshots need budgets separate from ordinary reads. A fixed window
(count resets every ``window_seconds``) is enough for that: it is cheap, needs no
background sweeping, and its worst case (a burst straddling a boundary admits up to
twice the limit) is acceptable for admission control. Two backends implement the port:
an in-process counter for a single replica and a Valkey-backed one that shares the
window across replicas through ``INCR`` plus a first-hit ``EXPIRE``.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from vera.config.settings import ResilienceSettings
from vera.domain.ports.resilience import QuotaLimiter


class InProcessQuota:
    """Single-replica fixed-window counter. A lock serializes the read-modify-write so
    concurrent calls on one key cannot both slip past a full window.
    """

    def __init__(self, *, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic
        self._lock = asyncio.Lock()
        # key -> (window_start, count_so_far)
        self._windows: dict[str, tuple[float, int]] = {}

    async def allow(self, key: str, *, limit: int, window_seconds: int) -> bool:
        if limit <= 0:
            return True
        now = self._clock()
        async with self._lock:
            start, count = self._windows.get(key, (now, 0))
            if now - start >= window_seconds:
                start, count = now, 0
            if count >= limit:
                return False
            self._windows[key] = (start, count + 1)
            return True


class ValkeyQuota:
    """Distributed fixed-window counter sharing the window across replicas.

    ``INCR`` returns the new count; the first hit (count == 1) also arms ``EXPIRE`` so
    the key clears itself when the window ends. A crash between the two commands only
    leaks one non-expiring key, so the fallback still bounds the window on the next hit.
    """

    def __init__(self, client: object, *, namespace: str = "vera:mcpquota") -> None:
        self._client = client
        self._namespace = namespace

    async def allow(self, key: str, *, limit: int, window_seconds: int) -> bool:
        if limit <= 0:
            return True
        from redis.asyncio import Redis

        client: Redis = self._client  # type: ignore[type-arg]
        full = f"{self._namespace}:{key}"
        count = int(await client.incr(full))  # pyright: ignore[reportUnknownMemberType]
        if count == 1:
            await client.expire(full, window_seconds)  # pyright: ignore[reportUnknownMemberType]
        return count <= limit


def build_quota_limiter(settings: ResilienceSettings) -> QuotaLimiter:
    """Valkey-backed when a shared URL is configured, else an in-process counter."""
    if settings.valkey_url:
        from redis.asyncio import Redis

        client = Redis.from_url(settings.valkey_url)  # pyright: ignore[reportUnknownMemberType]
        return ValkeyQuota(client)
    return InProcessQuota()
