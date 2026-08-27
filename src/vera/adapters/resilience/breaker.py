"""A small async circuit breaker.

Three states. CLOSED lets calls through and counts consecutive failures; at the
threshold it trips to OPEN. OPEN rejects immediately until the reset timeout elapses,
then the first call is allowed as a HALF_OPEN trial: success closes the breaker, a
failure re-opens it. This bounds the blast radius of a failing dependency, so callers
fail fast instead of piling up on a dead provider.

In-house rather than a third-party library: the breaker is a few lines of state, and
avoiding the dependency keeps the surface small and cloud-portable.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from enum import StrEnum

from vera.shared.errors import VeraError


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(VeraError):
    """The breaker is open; the call was rejected without reaching the dependency."""


class CircuitBreaker:
    def __init__(
        self,
        *,
        name: str,
        failure_threshold: int,
        reset_timeout_s: float,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self._name = name
        self._failure_threshold = failure_threshold
        self._reset_timeout_s = reset_timeout_s
        # Injectable clock for deterministic tests; defaults to a monotonic clock.
        self._clock: Callable[[], float] = monotonic or time.monotonic
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def before_call(self) -> None:
        """Raise ``CircuitOpenError`` if the circuit is open and not yet ready to retry."""
        async with self._lock:
            if self._state is CircuitState.OPEN:
                if self._clock() - self._opened_at < self._reset_timeout_s:
                    raise CircuitOpenError(f"circuit '{self._name}' is open")
                # Cooldown elapsed: allow a single trial call.
                self._state = CircuitState.HALF_OPEN

    async def record_success(self) -> None:
        async with self._lock:
            self._failures = 0
            self._state = CircuitState.CLOSED

    async def record_failure(self) -> None:
        async with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._trip()
                return
            self._failures += 1
            if self._failures >= self._failure_threshold:
                self._trip()

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()
        self._failures = self._failure_threshold
