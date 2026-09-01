"""The MCP server exposes only the safe, minimal tool surface."""

from __future__ import annotations

import pytest
from mcp.server.auth.provider import AccessToken

from vera.bootstrap import build_container, dispose_container
from vera.config.settings import McpSettings, get_settings
from vera.entrypoints.knowledge import InputError
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
    "knowledge_bootstrap",
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
    "knowledge_retract_proposal",
    "knowledge_proposal_report",
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
        context = next(tool for tool in tools if tool.name == "knowledge_get_context")
        assert context.input_schema["properties"]["persist"]["default"] is False
        assert context.annotations is not None
        assert context.annotations.read_only_hint is False
        assert context.annotations.idempotent_hint is False
        propose = next(tool for tool in tools if tool.name == "knowledge_propose")
        assert {"runtime", "session_ref", "task_ref", "repository_ref"} <= (
            propose.input_schema["properties"].keys()
        )
        feedback = next(tool for tool in tools if tool.name == "knowledge_feedback")
        assert "context_pack_id" in feedback.input_schema["required"]
        assert "signals" not in feedback.input_schema["properties"]
        legacy_feedback = next(tool for tool in tools if tool.name == "memory_feedback")
        assert set(legacy_feedback.input_schema["required"]) == {"result_ref", "signal"}
        assert {"query", "signals", "context_pack_id"} <= (
            legacy_feedback.input_schema["properties"].keys()
        )
        for tool_name in ("memory_propose", "knowledge_propose"):
            tool = next(item for item in tools if item.name == tool_name)
            assert tool.annotations is not None
            assert tool.annotations.read_only_hint is False
            assert tool.annotations.idempotent_hint is False
        for tool_name in ("knowledge_feedback", "knowledge_retract_proposal"):
            tool = next(item for item in tools if item.name == tool_name)
            assert tool.annotations is not None
            assert tool.annotations.read_only_hint is False
            assert tool.annotations.idempotent_hint is True
        assert legacy_feedback.annotations is not None
        assert legacy_feedback.annotations.idempotent_hint is False
        retract = next(tool for tool in tools if tool.name == "knowledge_retract_proposal")
        assert retract.annotations is not None
        assert retract.annotations.destructive_hint is True
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
    assert mcp_main.auth_profile(settings) == "local-dev"
    assert mcp_main._capability_classes(settings) == (
        "read",
        "personal-proposal",
        "feedback",
        "snapshot",
    )


def test_remote_capabilities_follow_token_scopes(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = get_settings().model_copy(
        update={"mcp": McpSettings(jwt_secret=_JWT_SECRET)}  # type: ignore[arg-type]
    )
    token = AccessToken(
        token="token",  # noqa: S106
        client_id="client",
        subject="00000000-0000-0000-0000-000000000001",
        scopes=["memory:read", "memory:feedback"],
    )
    monkeypatch.setattr(mcp_main, "get_access_token", lambda: token)

    assert mcp_main.auth_profile(settings) == "remote-authenticated"
    assert mcp_main._capability_classes(settings) == ("read", "feedback")


def test_mcp_timestamps_require_an_offset() -> None:
    with pytest.raises(InputError, match="UTC offset"):
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
