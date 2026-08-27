"""The OAuth 2.1 Resource Server token verifier."""

from __future__ import annotations

import time

import jwt
import pytest

from vera.adapters.mcp.auth import JwtTokenVerifier

_SECRET = "test-secret-that-is-long-enough-for-hs256"  # noqa: S105
_ISS = "https://auth.vera.local"
_AUD = "https://mcp.vera.local"


def _verifier() -> JwtTokenVerifier:
    return JwtTokenVerifier(
        secret=_SECRET,
        algorithm="HS256",
        issuer=_ISS,
        audience=_AUD,
        required_scopes=["memory:read"],
    )


def _token(**overrides: object) -> str:
    claims: dict[str, object] = {
        "sub": "principal-1",
        "iss": _ISS,
        "aud": _AUD,
        "scope": "memory:read",
        "exp": int(time.time()) + 3600,
    }
    claims.update(overrides)
    return jwt.encode(claims, _SECRET, algorithm="HS256")


@pytest.mark.asyncio
async def test_valid_token_yields_access_token() -> None:
    access = await _verifier().verify_token(_token(sub="alice"))
    assert access is not None
    assert access.subject == "alice"
    assert "memory:read" in access.scopes


@pytest.mark.asyncio
async def test_wrong_audience_is_rejected() -> None:
    assert await _verifier().verify_token(_token(aud="https://evil.example")) is None


@pytest.mark.asyncio
async def test_missing_required_scope_is_rejected() -> None:
    assert await _verifier().verify_token(_token(scope="memory:other")) is None


@pytest.mark.asyncio
async def test_tampered_token_is_rejected() -> None:
    assert await _verifier().verify_token(_token() + "x") is None
