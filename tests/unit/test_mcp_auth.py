"""The OAuth 2.1 Resource Server token verifier."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from uuid import UUID

import jwt
import pytest

from vera.adapters.mcp.auth import JwtTokenVerifier

_SECRET = "test-secret-that-is-long-enough-for-hs256"  # noqa: S105
_ISS = "https://auth.vera.local"
_AUD = "https://mcp.vera.local"
_PRINCIPAL_ID = UUID("00000000-0000-0000-0000-000000000001")


async def _principal_exists(principal_id: UUID) -> bool:
    return principal_id == _PRINCIPAL_ID


def _verifier(
    principal_exists: Callable[[UUID], Awaitable[bool]] = _principal_exists,
) -> JwtTokenVerifier:
    return JwtTokenVerifier(
        secret=_SECRET,
        algorithm="HS256",
        issuer=_ISS,
        audience=_AUD,
        required_scopes=["memory:read"],
        principal_exists=principal_exists,
    )


def _token(**overrides: object) -> str:
    claims: dict[str, object] = {
        "sub": str(_PRINCIPAL_ID),
        "iss": _ISS,
        "aud": _AUD,
        "scope": "memory:read",
        "exp": int(time.time()) + 3600,
    }
    claims.update(overrides)
    return jwt.encode(claims, _SECRET, algorithm="HS256")


@pytest.mark.asyncio
async def test_valid_token_yields_access_token() -> None:
    access = await _verifier().verify_token(_token())
    assert access is not None
    assert access.subject == str(_PRINCIPAL_ID)
    assert "memory:read" in access.scopes


@pytest.mark.asyncio
async def test_token_without_expiry_is_rejected() -> None:
    token = jwt.encode(
        {
            "sub": str(_PRINCIPAL_ID),
            "iss": _ISS,
            "aud": _AUD,
            "scope": "memory:read",
        },
        _SECRET,
        algorithm="HS256",
    )
    assert await _verifier().verify_token(token) is None


@pytest.mark.asyncio
async def test_non_uuid_subject_is_rejected() -> None:
    assert await _verifier().verify_token(_token(sub="alice")) is None


@pytest.mark.asyncio
async def test_unknown_principal_is_rejected() -> None:
    async def missing_principal(_principal_id: UUID) -> bool:
        return False

    assert await _verifier(missing_principal).verify_token(_token()) is None


@pytest.mark.asyncio
async def test_wrong_audience_is_rejected() -> None:
    assert await _verifier().verify_token(_token(aud="https://evil.example")) is None


@pytest.mark.asyncio
async def test_missing_required_scope_is_rejected() -> None:
    assert await _verifier().verify_token(_token(scope="memory:other")) is None


@pytest.mark.asyncio
async def test_tampered_token_is_rejected() -> None:
    assert await _verifier().verify_token(_token() + "x") is None
