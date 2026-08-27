"""The null engine honors the MemoryEngine contract."""

from __future__ import annotations

import pytest

from tests.contracts import assert_memory_contract
from vera.adapters.graph.null import NullMemoryEngine


@pytest.mark.asyncio
async def test_null_engine_satisfies_contract() -> None:
    await assert_memory_contract(NullMemoryEngine(), group="p:contract")
