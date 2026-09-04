"""The MCP server exposes only the safe, minimal tool surface."""

from __future__ import annotations

import pytest
from mcp.server.auth.provider import AccessToken
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import ValidationError

from vera.bootstrap import build_container, dispose_container
from vera.config.settings import McpSettings, get_settings
from vera.entrypoints.knowledge import InputError
from vera.entrypoints.mcp import main as mcp_main
from vera.entrypoints.mcp.main import build_server
from vera.shared.ids import deterministic_id

_LEGACY = {
    "memory_search",
    "memory_get_context",
    "memory_explore",
    "memory_explain",
    "memory_get_source",
    "memory_recent_changes",
    "memory_propose",
    "memory_feedback",
}
_CANONICAL = {
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
_CODING = {
    "knowledge_bootstrap",
    "knowledge_get_context",
    "knowledge_get_context_pack",
    "knowledge_search",
    "knowledge_explain_fact",
    "knowledge_get_evidence",
    "knowledge_feedback",
    "knowledge_propose",
    "knowledge_retract_proposal",
    "knowledge_proposal_report",
}
_JWT_SECRET = "test-secret"  # noqa: S105


@pytest.mark.asyncio
async def test_default_server_exposes_the_small_coding_profile() -> None:
    settings = get_settings()
    container = build_container(settings)
    try:
        server = build_server(container, settings)
        tools = await server.list_tools()
        assert {tool.name for tool in tools} == _CODING
        assert [type(item).__name__ for item in server.middleware] == [
            "RequestStateBoundary",
            "_KnownToolMiddleware",
        ]
    finally:
        await dispose_container(container)


@pytest.mark.asyncio
async def test_advanced_profile_exposes_only_canonical_tools() -> None:
    settings = get_settings().model_copy(update={"mcp": McpSettings(tool_profile="advanced")})
    container = build_container(settings)
    try:
        server = build_server(container, settings)
        tools = await server.list_tools()
        assert {tool.name for tool in tools} == _CANONICAL
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
        assert propose.annotations is not None
        assert propose.annotations.read_only_hint is False
        assert propose.annotations.idempotent_hint is False
        for tool_name in ("knowledge_feedback", "knowledge_retract_proposal"):
            tool = next(item for item in tools if item.name == tool_name)
            assert tool.annotations is not None
            assert tool.annotations.read_only_hint is False
            assert tool.annotations.idempotent_hint is True
        retract = next(tool for tool in tools if tool.name == "knowledge_retract_proposal")
        assert retract.annotations is not None
        assert retract.annotations.destructive_hint is True
    finally:
        await dispose_container(container)


@pytest.mark.asyncio
async def test_compatibility_profile_explicitly_adds_legacy_aliases() -> None:
    settings = get_settings().model_copy(update={"mcp": McpSettings(tool_profile="compatibility")})
    container = build_container(settings)
    try:
        tools = await build_server(container, settings).list_tools()
        assert {tool.name for tool in tools} == _CANONICAL | _LEGACY
        legacy_feedback = next(tool for tool in tools if tool.name == "memory_feedback")
        assert set(legacy_feedback.input_schema["required"]) == {"result_ref", "signal"}
        assert {"query", "signals", "context_pack_id"} <= (
            legacy_feedback.input_schema["properties"].keys()
        )
        assert legacy_feedback.annotations is not None
        assert legacy_feedback.annotations.idempotent_hint is False
        legacy_propose = next(tool for tool in tools if tool.name == "memory_propose")
        assert legacy_propose.annotations is not None
        assert legacy_propose.annotations.read_only_hint is False
        assert legacy_propose.annotations.idempotent_hint is False
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


@pytest.mark.asyncio
async def test_startup_failure_disposes_container(monkeypatch: pytest.MonkeyPatch) -> None:
    base = get_settings()
    settings = base.model_copy(
        update={
            "mcp": McpSettings(jwt_secret=_JWT_SECRET),  # type: ignore[arg-type]
            "observability": base.observability.model_copy(update={"metrics_enabled": True}),
        }
    )
    container = build_container(settings)
    disposed: list[object] = []

    async def refresh(_container: object) -> None:
        pass

    async def dispose(candidate: object) -> None:
        disposed.append(candidate)

    def fail_metrics(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("port occupied")

    monkeypatch.setattr(mcp_main, "refresh_rerank_weights", refresh)
    monkeypatch.setattr(mcp_main, "dispose_container", dispose)
    monkeypatch.setattr(mcp_main, "start_metrics_server", fail_metrics)
    server = build_server(container, settings)
    app = server.streamable_http_app(json_response=True, stateless_http=True)

    try:
        with pytest.raises(RuntimeError, match="port occupied"):
            async with app.router.lifespan_context(app):
                pass
        assert disposed == [container]
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


def test_external_oauth_configuration_disables_local_principal() -> None:
    settings = get_settings().model_copy(
        update={
            "mcp": McpSettings(
                oauth_issuer="https://login.example.com",
                oauth_signing_key=_JWT_SECRET,  # type: ignore[arg-type]
                oauth_algorithms=["HS256"],
            )
        }
    )

    assert mcp_main.auth_profile(settings) == "remote-authenticated"


@pytest.mark.parametrize("field", ["oauth_issuer", "oauth_jwks_url"])
def test_external_oauth_urls_require_tls_except_on_loopback(field: str) -> None:
    unsafe = {
        "oauth_issuer": "https://idp.example",
        "oauth_jwks_url": "https://idp.example/jwks",
    }
    unsafe[field] = "http://idp.example/value"
    with pytest.raises(ValidationError, match="must use HTTPS"):
        McpSettings(**unsafe)

    loopback = {
        "oauth_issuer": "http://127.0.0.1:9000",
        "oauth_jwks_url": "http://127.0.0.1:9000/jwks",
    }
    assert McpSettings(**loopback).model_dump()[field]


@pytest.mark.parametrize(
    "partial",
    [
        {"oauth_issuer": "https://idp.example"},
        {"oauth_jwks_url": "https://idp.example/jwks"},
    ],
)
def test_external_oauth_configuration_must_be_complete(partial: dict[str, str]) -> None:
    with pytest.raises(ValidationError, match="must be set together"):
        McpSettings(**partial)


def test_required_scopes_are_limited_to_capability_scopes() -> None:
    with pytest.raises(ValidationError, match="four MCP capability scopes"):
        McpSettings(required_scopes=["memory:read", "memory:admin"])


@pytest.mark.parametrize(
    "invalid",
    [
        {"auth_issuer": "HTTPS://IDP.EXAMPLE:443"},
        {"auth_audience": "https://mcp.example?tenant=one"},
        {
            "oauth_issuer": "https://idp.example?tenant=one",
            "oauth_jwks_url": "https://idp.example/jwks",
        },
    ],
)
def test_oauth_identifiers_reject_noncanonical_or_query_urls(invalid: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        McpSettings(**invalid)


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


def test_create_app_configures_tracing_before_building_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    events: list[str] = []
    app_kwargs: dict[str, object] = {}
    expected_app = object()

    class Server:
        def streamable_http_app(self, **kwargs: object) -> object:
            app_kwargs.update(kwargs)
            return expected_app

    monkeypatch.setattr(mcp_main, "get_settings", lambda: settings)
    monkeypatch.setattr(mcp_main, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(mcp_main, "configure_tracing", lambda _settings: events.append("trace"))
    monkeypatch.setattr(
        mcp_main,
        "build_container",
        lambda _settings: events.append("container") or object(),
    )
    monkeypatch.setattr(mcp_main, "build_server", lambda *_args: Server())

    assert mcp_main.create_app() is expected_app
    assert events == ["trace", "container"]
    security = app_kwargs["transport_security"]
    assert isinstance(security, TransportSecuritySettings)
    assert security.enable_dns_rebinding_protection is True
    assert security.allowed_hosts == settings.mcp.allowed_hosts
    assert security.allowed_origins == settings.mcp.allowed_origins
