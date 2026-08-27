"""The MCP server exposes only the safe, minimal tool surface."""

from __future__ import annotations

import pytest

from vera.bootstrap import build_container, dispose_container
from vera.config.settings import get_settings
from vera.entrypoints.mcp.main import build_server

_EXPECTED = {
    "memory_search",
    "memory_get_context",
    "memory_explain",
    "memory_get_source",
    "memory_recent_changes",
    "memory_propose",
    "memory_feedback",
}


@pytest.mark.asyncio
async def test_server_exposes_the_seven_memory_tools() -> None:
    settings = get_settings()
    container = build_container(settings)
    try:
        server = build_server(container, settings)
        names = {tool.name for tool in await server.list_tools()}
        assert names == _EXPECTED
    finally:
        await dispose_container(container)
