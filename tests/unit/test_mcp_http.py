"""MCP server as an OAuth 2.1 Resource Server over HTTP (RFC 9728).

Drives the real ASGI app the SDK builds from VERA's auth wiring: the protected-resource
metadata endpoint, the 401 with a metadata pointer for an unauthenticated call, and a
token with the wrong scope being rejected. All of this happens before any tool or database
access, so the test is hermetic (no live DB).
"""

from __future__ import annotations

import httpx
import jwt
import pytest

from vera.bootstrap import build_container
from vera.config.settings import McpSettings, get_settings
from vera.entrypoints.mcp.main import build_server

pytestmark = pytest.mark.asyncio

_SECRET = "mcp-test-secret-long-enough-for-hs256-000"  # noqa: S105
_ISS = "https://idp.example"
_AUD = "https://api.vera.local"


def _app() -> httpx.ASGITransport:
    settings = get_settings().model_copy(
        update={
            "mcp": McpSettings(
                jwt_secret=_SECRET,  # type: ignore[arg-type]
                auth_issuer=_ISS,
                auth_audience=_AUD,
                required_scopes=["memory.read"],
            )
        }
    )
    container = build_container(settings)
    app = build_server(container, settings).streamable_http_app()
    return httpx.ASGITransport(app=app)


async def test_metadata_endpoint_advertises_the_resource() -> None:
    async with httpx.AsyncClient(transport=_app(), base_url="http://t") as client:
        resp = await client.get("/.well-known/oauth-protected-resource")
    assert resp.status_code == 200
    body = resp.json()
    assert body["resource"].startswith(_AUD)
    assert _ISS + "/" in body["authorization_servers"]
    assert body["scopes_supported"] == ["memory.read"]


async def test_unauthenticated_call_gets_401_with_metadata_pointer() -> None:
    async with httpx.AsyncClient(transport=_app(), base_url="http://t") as client:
        resp = await client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={"Accept": "application/json, text/event-stream"},
        )
    assert resp.status_code == 401
    www_auth = resp.headers["www-authenticate"]
    assert "Bearer" in www_auth
    assert "/.well-known/oauth-protected-resource" in www_auth


async def test_token_with_wrong_scope_is_rejected() -> None:
    payload = jwt.encode(
        {"iss": _ISS, "aud": _AUD, "sub": "00000000-0000-0000-0000-000000000001", "scope": "nope"},
        _SECRET,
        algorithm="HS256",
    )
    async with httpx.AsyncClient(transport=_app(), base_url="http://t") as client:
        resp = await client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={
                "Accept": "application/json, text/event-stream",
                "Authorization": f"Bearer {payload}",
            },
        )
    # Valid signature/issuer/audience but missing the required scope -> still unauthorized.
    assert resp.status_code == 401
