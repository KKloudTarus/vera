"""The in-process fixed-window quota counter."""

from __future__ import annotations

import pytest

from vera.adapters.resilience.quota import InProcessQuota

pytestmark = pytest.mark.asyncio


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def test_admits_up_to_the_limit_then_rejects() -> None:
    quota = InProcessQuota(clock=_Clock())
    assert await quota.allow("p:read", limit=2, window_seconds=60) is True
    assert await quota.allow("p:read", limit=2, window_seconds=60) is True
    assert await quota.allow("p:read", limit=2, window_seconds=60) is False


async def test_window_resets_after_it_elapses() -> None:
    clock = _Clock()
    quota = InProcessQuota(clock=clock)
    assert await quota.allow("p:read", limit=1, window_seconds=60) is True
    assert await quota.allow("p:read", limit=1, window_seconds=60) is False
    clock.now += 60
    assert await quota.allow("p:read", limit=1, window_seconds=60) is True


async def test_keys_are_independent() -> None:
    quota = InProcessQuota(clock=_Clock())
    assert await quota.allow("a:read", limit=1, window_seconds=60) is True
    assert await quota.allow("b:read", limit=1, window_seconds=60) is True
    assert await quota.allow("a:read", limit=1, window_seconds=60) is False


async def test_non_positive_limit_disables_the_bucket() -> None:
    quota = InProcessQuota(clock=_Clock())
    for _ in range(5):
        assert await quota.allow("p:read", limit=0, window_seconds=60) is True
