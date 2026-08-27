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
