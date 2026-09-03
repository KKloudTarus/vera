"""MCP server as an OAuth 2.1 Resource Server over HTTP (RFC 9728).

Drives the real ASGI app the SDK builds from VERA's auth wiring: the protected-resource
metadata endpoint, the 401 with a metadata pointer for an unauthenticated call, and a
token with the wrong scope being rejected. All of this happens before any tool or database
access, so the test is hermetic (no live DB).
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import jwt
import pytest

from vera.bootstrap import build_container
from vera.config.settings import McpSettings, get_settings
from vera.entrypoints.knowledge import KnowledgeService
from vera.entrypoints.mcp import main as mcp_main
from vera.entrypoints.mcp.main import build_server

pytestmark = pytest.mark.asyncio

_SECRET = "mcp-test-secret-long-enough-for-hs256-000"  # noqa: S105
_WRONG_SECRET = "not-the-server-secret-long-enough"  # noqa: S105
_ISS = "https://idp.example"
_OAUTH_ISS = "https://login.example"
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
    mcp_main._replace_protected_resource_metadata(app, settings)
    return httpx.ASGITransport(app=app)


def _oauth_app() -> httpx.ASGITransport:
    settings = get_settings().model_copy(
        update={
            "mcp": McpSettings(
                oauth_issuer=_OAUTH_ISS,
                oauth_signing_key=_SECRET,  # type: ignore[arg-type]
                oauth_algorithms=["HS256"],
                auth_audience=_AUD,
                required_scopes=["memory:read"],
            )
        }
    )
    container = build_container(settings)
    app = build_server(container, settings).streamable_http_app()
    mcp_main._replace_protected_resource_metadata(app, settings)
    return httpx.ASGITransport(app=app)


def _token(
    *,
    scopes: str = "memory:read",
    secret: str = _SECRET,
    subject: str = "00000000-0000-0000-0000-000000000001",
) -> str:
    return jwt.encode(
        {
            "iss": _ISS,
            "aud": _AUD,
            "sub": subject,
            "scope": scopes,
            "exp": int(time.time()) + 300,
        },
        secret,
        algorithm="HS256",
    )


@asynccontextmanager
async def _authenticated_client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    read_quota: int | None = None,
    metrics_enabled: bool = False,
    allowed_hosts: list[str] | None = None,
    allowed_origins: list[str] | None = None,
    base_url: str = "http://127.0.0.1:8000",
) -> AsyncIterator[tuple[httpx.AsyncClient, str]]:
    async def principal_exists(*_args: object, **_kwargs: object) -> bool:
        return True

    async def bootstrap(
        _self: KnowledgeService,
        principal_id: object,
        **kwargs: object,
    ) -> dict[str, Any]:
        return {
            "principal": {"id": str(principal_id)},
            "auth_profile": kwargs["auth_profile"],
            "tool_profile": {"active": kwargs["tool_profile"]},
        }

    monkeypatch.setattr(mcp_main, "_principal_exists", principal_exists)
    monkeypatch.setattr(KnowledgeService, "bootstrap", bootstrap)
    base = get_settings()
    settings = base.model_copy(
        update={
            "resilience": base.resilience.model_copy(update={"valkey_url": None}),
            "observability": base.observability.model_copy(
                update={"metrics_enabled": metrics_enabled}
            ),
            "mcp": McpSettings(
                jwt_secret=_SECRET,  # type: ignore[arg-type]
                auth_issuer=_ISS,
                auth_audience=_AUD,
                required_scopes=["memory:read"],
                quota_enabled=read_quota is not None,
                quota_reads_per_minute=read_quota or 120,
                allowed_hosts=allowed_hosts or base.mcp.allowed_hosts,
                allowed_origins=allowed_origins or base.mcp.allowed_origins,
            ),
        }
    )
    container = build_container(settings)
    server = build_server(container, settings)
    app = server.streamable_http_app(
        json_response=True,
        stateless_http=True,
        transport_security=mcp_main._transport_security(settings),
        host="127.0.0.1",
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=base_url) as client,
    ):
        yield client, _token()


async def _rpc(
    client: httpx.AsyncClient,
    token: str,
    *,
    method: str,
    params: dict[str, Any] | None = None,
    request_id: int = 1,
    extra_headers: dict[str, str] | None = None,
) -> httpx.Response:
    headers = {
        "Accept": "application/json, text/event-stream",
        "Authorization": f"Bearer {token}",
    }
    headers.update(extra_headers or {})
    return await client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}},
        headers=headers,
    )


async def test_metadata_endpoint_advertises_the_resource() -> None:
    async with httpx.AsyncClient(transport=_app(), base_url="http://t") as client:
        resp = await client.get("/.well-known/oauth-protected-resource")
    assert resp.status_code == 200
    body = resp.json()
    assert body["resource"] == _AUD
    assert body["authorization_servers"] == [_ISS]
    assert set(body["scopes_supported"]) == {
        "memory:read",
        "memory:propose",
        "memory:feedback",
        "memory:snapshot",
        "memory.read",
    }


async def test_external_oauth_metadata_advertises_the_real_authorization_server() -> None:
    async with httpx.AsyncClient(transport=_oauth_app(), base_url="http://t") as client:
        resp = await client.get("/.well-known/oauth-protected-resource")

    assert resp.status_code == 200
    assert resp.json()["authorization_servers"] == [_OAUTH_ISS]


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
        {
            "iss": _ISS,
            "aud": _AUD,
            "sub": "00000000-0000-0000-0000-000000000001",
            "scope": "nope",
            "exp": int(time.time()) + 300,
        },
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


async def _post_with_token(claims: dict[str, object]) -> httpx.Response:
    token = jwt.encode(claims, _SECRET, algorithm="HS256")
    async with httpx.AsyncClient(transport=_app(), base_url="http://t") as client:
        return await client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={
                "Accept": "application/json, text/event-stream",
                "Authorization": f"Bearer {token}",
            },
        )


async def test_expired_token_is_rejected() -> None:
    sub = "00000000-0000-0000-0000-000000000001"
    resp = await _post_with_token(
        {"iss": _ISS, "aud": _AUD, "sub": sub, "scope": "memory.read", "exp": int(time.time()) - 30}
    )
    assert resp.status_code == 401


async def test_wrong_audience_token_is_rejected() -> None:
    sub = "00000000-0000-0000-0000-000000000001"
    resp = await _post_with_token(
        {
            "iss": _ISS,
            "aud": "https://someone.else",
            "sub": sub,
            "scope": "memory.read",
            "exp": int(time.time()) + 300,
        }
    )
    assert resp.status_code == 401


async def test_invalid_signature_is_rejected_before_tool_dispatch() -> None:
    async with httpx.AsyncClient(transport=_app(), base_url="http://t") as client:
        resp = await client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={
                "Accept": "application/json, text/event-stream",
                "Authorization": f"Bearer {_token(secret=_WRONG_SECRET)}",
            },
        )
    assert resp.status_code == 401
    assert "Bearer" in resp.headers["www-authenticate"]


async def test_valid_token_lists_default_profile_and_calls_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _authenticated_client(monkeypatch) as (client, token):
        listed = await _rpc(client, token, method="tools/list")
        called = await _rpc(
            client,
            token,
            method="tools/call",
            params={"name": "knowledge_bootstrap", "arguments": {}},
            request_id=2,
        )

    assert listed.status_code == 200
    names = {tool["name"] for tool in listed.json()["result"]["tools"]}
    assert "knowledge_bootstrap" in names
    assert not any(name.startswith("memory_") for name in names)
    assert called.status_code == 200
    payload = called.json()["result"]["structuredContent"]
    assert payload["principal"]["id"] == "00000000-0000-0000-0000-000000000001"
    assert payload["auth_profile"] == "remote-authenticated"
    assert payload["tool_profile"]["active"] == "coding"


async def test_transport_security_allows_configured_host_and_origin_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _authenticated_client(
        monkeypatch,
        allowed_hosts=["mcp.example"],
        allowed_origins=["https://client.example"],
        base_url="https://mcp.example",
    ) as (client, token):
        accepted = await _rpc(
            client,
            token,
            method="tools/list",
            extra_headers={"Origin": "https://client.example"},
        )
        hostile_host = await _rpc(
            client,
            token,
            method="tools/list",
            extra_headers={"Host": "attacker.example"},
        )
        hostile_origin = await _rpc(
            client,
            token,
            method="tools/list",
            extra_headers={"Origin": "https://attacker.example"},
        )

    assert accepted.status_code == 200
    assert hostile_host.status_code == 421
    assert hostile_origin.status_code == 403


async def test_read_token_cannot_call_proposal_or_persist_context_over_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _authenticated_client(monkeypatch) as (client, token):
        proposal = await _rpc(
            client,
            token,
            method="tools/call",
            params={
                "name": "knowledge_propose",
                "arguments": {"subject": "svc", "predicate": "RUNS_ON", "object": "prod"},
            },
        )
        context = await _rpc(
            client,
            token,
            method="tools/call",
            params={
                "name": "knowledge_get_context",
                "arguments": {"query": "deployment", "persist": True},
            },
            request_id=2,
        )
        oversized = await _rpc(
            client,
            token,
            method="tools/call",
            params={
                "name": "knowledge_search",
                "arguments": {"query": "x" * 8193},
            },
            request_id=3,
        )

    assert proposal.status_code == 200
    assert proposal.json()["error"]["data"] == {
        "code": "unauthorized",
        "required_scope": "memory:propose",
    }
    assert context.status_code == 200
    assert context.json()["error"]["data"] == {
        "code": "unauthorized",
        "required_scope": "memory:snapshot",
    }
    assert oversized.json()["error"]["data"] == {
        "code": "invalid_input",
        "field": "query",
    }


async def test_http_read_quota_is_isolated_per_authenticated_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second_subject = "00000000-0000-0000-0000-000000000002"
    async with _authenticated_client(monkeypatch, read_quota=1) as (client, first_token):
        first = await _rpc(
            client,
            first_token,
            method="tools/call",
            params={"name": "knowledge_bootstrap", "arguments": {}},
        )
        spent = await _rpc(
            client,
            first_token,
            method="tools/call",
            params={"name": "knowledge_bootstrap", "arguments": {}},
            request_id=2,
        )
        independent = await _rpc(
            client,
            _token(subject=second_subject),
            method="tools/call",
            params={"name": "knowledge_bootstrap", "arguments": {}},
            request_id=3,
        )

    assert first.status_code == 200 and "result" in first.json()
    assert spent.json()["error"]["data"] == {"code": "quota_exceeded", "bucket": "read"}
    assert independent.status_code == 200
    assert independent.json()["result"]["structuredContent"]["principal"]["id"] == second_subject


async def test_mcp_process_exposes_bounded_tool_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    async with _authenticated_client(monkeypatch, metrics_enabled=True) as (client, token):
        await _rpc(
            client,
            token,
            method="tools/call",
            params={"name": "knowledge_bootstrap", "arguments": {}},
        )
        public_response = await client.get(
            "/metrics",
            headers={"Host": "attacker.example", "Origin": "https://attacker.example"},
        )
        async with httpx.AsyncClient() as scraper:
            response = await scraper.get("http://127.0.0.1:9101/metrics")

    assert response.status_code == 200
    assert public_response.status_code == 404
    assert "vera_mcp_tool_calls_total" not in public_response.text
    assert "vera_mcp_tool_calls_total" in response.text
    assert 'tool="knowledge_bootstrap"' in response.text
    assert "vera_mcp_tool_duration_seconds" in response.text


async def test_metrics_endpoint_is_absent_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    async with _authenticated_client(monkeypatch, metrics_enabled=False) as (client, _token_value):
        response = await client.get("/metrics")

    assert response.status_code == 404
