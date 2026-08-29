"""The null memory engine satisfies the MemoryEngine port as a no-op."""

from __future__ import annotations

import pytest

from vera.adapters.graph.null import NullMemoryEngine

pytestmark = pytest.mark.asyncio


async def test_build_communities_is_a_noop_returning_no_rows() -> None:
    assert await NullMemoryEngine().build_communities(group_id="p:x") == ()
