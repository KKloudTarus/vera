"""Resilience ports: the rate limiter seen by callers.

A limiter admits work only when both the request and token budgets allow it, so a
provider call never exceeds the provider's RPM or TPM. The implementation (in-process
or Valkey-backed) is an adapter; callers depend only on this port.
"""

from __future__ import annotations

from typing import Protocol


class RateLimiter(Protocol):
    async def acquire(self, *, tokens: int = 0) -> None:
        """Wait until one request and ``tokens`` tokens are available, then consume them."""
        ...


class QuotaLimiter(Protocol):
    """A fixed-window admission counter, keyed by an opaque string.

    Unlike ``RateLimiter``, this never waits: it records the hit and reports whether the
    caller stayed within ``limit`` for the current ``window_seconds``, so an abusive
    principal is rejected fast rather than queued. A non-positive ``limit`` disables the
    bucket (always admitted).
    """

    async def allow(self, key: str, *, limit: int, window_seconds: int) -> bool:
        """Record a hit on ``key`` and return whether it stayed within ``limit``."""
        ...
