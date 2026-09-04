"""Guard enforcement: authorization class, quota, bounds, and scope-error redaction.

The guard reads the authenticated token from the SDK's request contextvar, so these
tests patch ``get_access_token`` in the guard module to simulate a caller rather than
driving the ASGI auth middleware.
"""

from __future__ import annotations

import pytest
from mcp.server.auth.provider import AccessToken
from mcp.server.mcpserver import MCPServer
from mcp.shared.exceptions import MCPError

from vera.adapters.resilience.quota import InProcessQuota
from vera.config.settings import McpSettings, Settings, get_settings
from vera.entrypoints.knowledge.service import ScopeError
from vera.entrypoints.mcp import guard as guard_module
from vera.entrypoints.mcp.guard import Guard, _map_scope_error
from vera.entrypoints.mcp.policy import ToolClass

pytestmark = pytest.mark.asyncio


def _guard(settings: Settings) -> Guard:
    return Guard(MCPServer(name="test"), settings, InProcessQuota())


def _remote(**mcp: object) -> Settings:
    base = {"jwt_secret": "secret-long-enough-for-a-test-000"}
    base.update(mcp)
    return get_settings().model_copy(update={"mcp": McpSettings(**base)})  # type: ignore[arg-type]


def _token(*scopes: str, subject: str = "principal-1") -> AccessToken:
    return AccessToken(token="t", client_id=subject, scopes=list(scopes), subject=subject)  # noqa: S106


def _code(exc: MCPError) -> str:
    assert isinstance(exc.data, dict)
    return str(exc.data["code"])


async def test_local_profile_grants_every_class_without_a_token() -> None:
    settings = get_settings()  # local environment, no jwt secret
    guard = _guard(settings)
    principal = str(settings.mcp.local_principal_id)
    assert guard._authorize(ToolClass.PROPOSE) == principal
    assert guard._authorize(ToolClass.SNAPSHOT) == principal


async def test_remote_without_token_is_unauthenticated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(guard_module, "get_access_token", lambda: None)
    with pytest.raises(MCPError) as exc:
        _guard(_remote())._authorize(ToolClass.READ)
    assert _code(exc.value) == "unauthenticated"


async def test_read_only_credential_cannot_call_a_write_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(guard_module, "get_access_token", lambda: _token("memory:read"))
    guard = _guard(_remote())
    # A read scope still resolves the principal for reads.
    assert guard._authorize(ToolClass.READ) == "principal-1"
    for write in (ToolClass.PROPOSE, ToolClass.FEEDBACK, ToolClass.SNAPSHOT):
        with pytest.raises(MCPError) as exc:
            guard._authorize(write)
        assert _code(exc.value) == "unauthorized"


async def test_oauth_only_read_credential_cannot_call_a_write_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(guard_module, "get_access_token", lambda: _token("memory:read"))
    settings = _remote(
        jwt_secret=None,
        oauth_issuer="https://login.example.com",
        oauth_signing_key="external-test-key",
        oauth_algorithms=["HS256"],
    )

    with pytest.raises(MCPError) as exc:
        _guard(settings)._authorize(ToolClass.PROPOSE)

    assert _code(exc.value) == "unauthorized"


async def test_write_scope_authorizes_the_write_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        guard_module, "get_access_token", lambda: _token("memory:read", "memory:propose")
    )
    assert _guard(_remote())._authorize(ToolClass.PROPOSE) == "principal-1"


async def test_enforce_rejects_out_of_bounds_arguments() -> None:
    with pytest.raises(MCPError) as exc:
        await _guard(get_settings())._enforce("memory_search", ToolClass.READ, {"limit": 999})
    assert _code(exc.value) == "invalid_input"


async def test_enforce_rejects_once_the_quota_is_spent() -> None:
    guard = _guard(get_settings().model_copy(update={"mcp": McpSettings(quota_reads_per_minute=1)}))
    await guard._enforce("memory_search", ToolClass.READ, {})
    with pytest.raises(MCPError) as exc:
        await guard._enforce("memory_search", ToolClass.READ, {})
    assert _code(exc.value) == "quota_exceeded"


async def test_scope_error_is_redacted_to_a_stable_code() -> None:
    ambiguous = _map_scope_error(ScopeError("ambiguous scope: specify a project"))
    leaky = _map_scope_error(ScopeError("principal 0f8e-... has no scope"))
    assert ambiguous.data["code"] == "ambiguous_project"
    # The principal id in the raw message never reaches the mapped error.
    assert leaky.data["code"] == "project_out_of_scope"
    assert "principal" not in leaky.message
