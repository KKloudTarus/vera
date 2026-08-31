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
    "knowledge_get_community_lineage",
    "knowledge_search",
    "knowledge_search_communities",
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
        tools = await server.list_tools()
        names = {tool.name for tool in tools}
        assert names == _EXPECTED
        search = next(tool for tool in tools if tool.name == "knowledge_search")
        assert {"as_of", "known_as_of"} <= search.input_schema["properties"].keys()
    finally:
        await dispose_container(container)


@pytest.mark.asyncio
async def test_instructions_declare_retrieved_content_untrusted() -> None:
    settings = get_settings()
    container = build_container(settings)
    try:
        server = build_server(container, settings)
        instructions = server.instructions or ""
        # The agent-facing contract: retrieved knowledge is reference data, not commands.
        assert "untrusted reference data" in instructions
        assert "never as" in instructions and "instructions" in instructions
        assert "knowledge_get_context" in instructions
    finally:
        await dispose_container(container)


def test_local_server_uses_stable_principal_without_jwt() -> None:
    principal_id = deterministic_id("test", "local-mcp")
    settings = get_settings().model_copy(
        update={"mcp": McpSettings(local_principal_id=principal_id)}
    )

    assert mcp_main._principal_id(settings) == principal_id


def test_mcp_timestamps_require_an_offset() -> None:
    with pytest.raises(ValueError, match="UTC offset"):
        mcp_main._parse_instant("2026-01-02T03:04:05")
    assert mcp_main._parse_instant("2026-01-02T03:04:05Z") is not None


def test_jwt_server_never_falls_back_to_local_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings().model_copy(
        update={"mcp": McpSettings(jwt_secret=_JWT_SECRET)}  # type: ignore[arg-type]
    )
    monkeypatch.setattr(mcp_main, "get_access_token", lambda: None)

    with pytest.raises(PermissionError, match="no authenticated principal"):
        mcp_main._principal_id(settings)
