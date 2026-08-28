"""The MCP server exposes only the safe, minimal tool surface."""

from __future__ import annotations

import pytest

from vera.bootstrap import build_container, dispose_container
from vera.config.settings import McpSettings, get_settings
from vera.entrypoints.mcp import main as mcp_main
from vera.entrypoints.mcp.main import build_server
from vera.shared.ids import deterministic_id

_EXPECTED = {
    # Legacy memory_* tools (kept for backward compatibility).
    "memory_search",
    "memory_get_context",
    "memory_explore",
    "memory_explain",
    "memory_get_source",
    "memory_recent_changes",
    "memory_propose",
    "memory_feedback",
    # Generic knowledge_* contracts (Phase 6).
    "knowledge_get_context",
    "knowledge_get_context_pack",
    "knowledge_search",
    "knowledge_get_fact",
    "knowledge_get_entity",
    "knowledge_get_source",
    "knowledge_explore",
    "knowledge_explain_fact",
    "knowledge_get_evidence",
    "knowledge_feedback",
    "knowledge_get_changes",
    "knowledge_get_conflicts",
    "knowledge_create_snapshot",
    "knowledge_get_snapshot",
    "knowledge_propose",
}
_JWT_SECRET = "test-secret"  # noqa: S105


@pytest.mark.asyncio
async def test_server_exposes_the_memory_tools() -> None:
    settings = get_settings()
    container = build_container(settings)
    try:
        server = build_server(container, settings)
        names = {tool.name for tool in await server.list_tools()}
        assert names == _EXPECTED
    finally:
        await dispose_container(container)


def test_local_server_uses_stable_principal_without_jwt() -> None:
    principal_id = deterministic_id("test", "local-mcp")
    settings = get_settings().model_copy(
        update={"mcp": McpSettings(local_principal_id=principal_id)}
    )

    assert mcp_main._principal_id(settings) == principal_id


def test_jwt_server_never_falls_back_to_local_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings().model_copy(
        update={"mcp": McpSettings(jwt_secret=_JWT_SECRET)}  # type: ignore[arg-type]
    )
    monkeypatch.setattr(mcp_main, "get_access_token", lambda: None)

    with pytest.raises(PermissionError, match="no authenticated principal"):
        mcp_main._principal_id(settings)
