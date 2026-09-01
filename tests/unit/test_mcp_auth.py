"""The OAuth 2.1 Resource Server token verifier."""

from __future__ import annotations

import asyncio
import base64
import time
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import jwt
import pytest

from vera.adapters.identity.oidc import OidcTokenVerifier
from vera.adapters.mcp.auth import (
    CompositeTokenVerifier,
    JwtTokenVerifier,
    OidcMcpTokenVerifier,
    issue_mcp_jwt,
)
from vera.domain.identity.models import AuthenticatedPrincipal, PrincipalKind

_SECRET = "test-secret-that-is-long-enough-for-hs256"  # noqa: S105
_ISS = "https://auth.vera.local"
_AUD = "https://mcp.vera.local"
_PRINCIPAL_ID = UUID("00000000-0000-0000-0000-000000000001")
_OIDC_SECRET = "external-idp-test-signing-secret"  # noqa: S105
_OIDC_ISS = "https://login.example.com"


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


def test_issue_mcp_jwt_binds_principal_resource_scopes_without_expiry() -> None:
    token = issue_mcp_jwt(
        principal_id=_PRINCIPAL_ID,
        secret=_SECRET,
        algorithm="HS256",
        issuer=_ISS,
        audience=_AUD,
        scopes=["memory:read", "memory:propose", "memory:read"],
        now=1_800_000_000,
    )

    claims = jwt.decode(
        token,
        _SECRET,
        algorithms=["HS256"],
        audience=_AUD,
        issuer=_ISS,
        options={"verify_exp": False, "verify_iat": False},
    )
    assert claims == {
        "sub": str(_PRINCIPAL_ID),
        "iss": _ISS,
        "aud": _AUD,
        "scope": "memory:read memory:propose",
        "iat": 1_800_000_000,
    }


@pytest.mark.asyncio
async def test_valid_token_yields_access_token() -> None:
    access = await _verifier().verify_token(_token())
    assert access is not None
    assert access.subject == str(_PRINCIPAL_ID)
    assert "memory:read" in access.scopes


@pytest.mark.asyncio
async def test_non_expiring_token_is_accepted() -> None:
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
    assert await _verifier().verify_token(token) is not None


@pytest.mark.asyncio
async def test_unknown_principal_is_rejected() -> None:
    async def missing_principal(_principal_id: UUID) -> bool:
        return False

    assert await _verifier(missing_principal).verify_token(_token()) is None


@pytest.mark.asyncio
async def test_non_uuid_subject_is_rejected_without_lookup() -> None:
    called = False

    async def principal_exists(_principal_id: UUID) -> bool:
        nonlocal called
        called = True
        return True

    assert await _verifier(principal_exists).verify_token(_token(sub="unknown")) is None
    assert called is False


@pytest.mark.asyncio
async def test_wrong_audience_is_rejected() -> None:
    assert await _verifier().verify_token(_token(aud="https://evil.example")) is None


@pytest.mark.asyncio
async def test_missing_required_scope_is_rejected() -> None:
    assert await _verifier().verify_token(_token(scope="memory:other")) is None


@pytest.mark.asyncio
async def test_tampered_token_is_rejected() -> None:
    assert await _verifier().verify_token(_token() + "x") is None


def _oauth_token(**overrides: object) -> str:
    claims: dict[str, object] = {
        "sub": "external-user-42",
        "iss": _OIDC_ISS,
        "aud": _AUD,
        "scope": "memory:read memory:propose",
        "azp": "coding-tool",
        "exp": int(time.time()) + 3600,
    }
    claims.update(overrides)
    return jwt.encode(claims, _OIDC_SECRET, algorithm="HS256")


def _oauth_verifier() -> OidcMcpTokenVerifier:
    async def authenticate_claims(
        claims: dict[str, Any],
    ) -> AuthenticatedPrincipal | None:
        if claims.get("sub") != "external-user-42":
            return None
        return AuthenticatedPrincipal(
            id=_PRINCIPAL_ID,
            kind=PrincipalKind.USER,
            display_name="External User",
            personal_group_id=f"u:{_PRINCIPAL_ID}",
        )

    return OidcMcpTokenVerifier(
        verifier=OidcTokenVerifier(
            signing_key=_OIDC_SECRET,
            algorithms=["HS256"],
            issuer=_OIDC_ISS,
            audience=_AUD,
        ),
        authenticate_claims=authenticate_claims,
        audience=_AUD,
        required_scopes=["memory:read"],
    )


@pytest.mark.asyncio
async def test_external_oauth_token_maps_subject_to_vera_principal() -> None:
    access = await _oauth_verifier().verify_token(_oauth_token())

    assert access is not None
    assert access.subject == str(_PRINCIPAL_ID)
    assert access.client_id == "coding-tool"
    assert access.scopes == ["memory:read", "memory:propose"]


@pytest.mark.asyncio
async def test_external_oauth_token_requires_mcp_scope() -> None:
    assert await _oauth_verifier().verify_token(_oauth_token(scope="openid profile")) is None


@pytest.mark.asyncio
async def test_external_oauth_token_requires_expiry() -> None:
    token = jwt.encode(
        {
            "sub": "external-user-42",
            "iss": _OIDC_ISS,
            "aud": _AUD,
            "scope": "memory:read",
        },
        _OIDC_SECRET,
        algorithm="HS256",
    )

    assert await _oauth_verifier().verify_token(token) is None


@pytest.mark.asyncio
async def test_jwks_token_without_kid_is_rejected_without_a_fetch() -> None:
    verifier = OidcTokenVerifier(
        jwks_url="https://login.example.com/jwks",
        algorithms=["RS256"],
        issuer=_OIDC_ISS,
        audience=_AUD,
    )

    class NoFetch:
        def get_signing_keys(self, *, refresh: bool = False) -> list[object]:
            raise AssertionError(f"unexpected JWKS fetch, refresh={refresh}")

    verifier._jwks = NoFetch()  # type: ignore[reportPrivateUsage]

    assert await verifier.verify_async(_token()) is None


@pytest.mark.asyncio
async def test_unknown_jwks_kids_share_a_bounded_refresh_window() -> None:
    verifier = OidcTokenVerifier(
        jwks_url="https://login.example.com/jwks",
        algorithms=["RS256"],
        issuer=_OIDC_ISS,
        audience=_AUD,
    )

    class MissingKeys:
        def __init__(self) -> None:
            self.refreshes = 0

        def get_signing_keys(self, *, refresh: bool = False) -> list[object]:
            if refresh:
                self.refreshes += 1
            return []

        def match_kid(self, _keys: list[object], _kid: str) -> None:
            return None

    keys = MissingKeys()
    verifier._jwks = keys  # type: ignore[reportPrivateUsage]
    first = jwt.encode(
        {"sub": "one"}, _OIDC_SECRET, algorithm="HS256", headers={"kid": "unknown-one"}
    )
    second = jwt.encode(
        {"sub": "two"}, _OIDC_SECRET, algorithm="HS256", headers={"kid": "unknown-two"}
    )

    assert await verifier.verify_async(first) is None
    assert await verifier.verify_async(second) is None
    assert keys.refreshes == 1


@pytest.mark.asyncio
async def test_concurrent_jwks_outage_uses_one_bounded_fetch() -> None:
    verifier = OidcTokenVerifier(
        jwks_url="https://login.example.com/jwks",
        algorithms=["RS256"],
        issuer=_OIDC_ISS,
        audience=_AUD,
    )

    class Outage:
        def __init__(self) -> None:
            self.fetches = 0

        def get_signing_keys(self, *, refresh: bool = False) -> list[object]:
            self.fetches += 1
            raise jwt.PyJWKClientConnectionError(f"offline, refresh={refresh}")

        def match_kid(self, _keys: list[object], _kid: str) -> None:
            return None

    keys = Outage()
    verifier._jwks = keys  # type: ignore[reportPrivateUsage]
    token = jwt.encode(
        {"sub": "external"},
        _OIDC_SECRET,
        algorithm="HS256",
        headers={"kid": "outage-key"},
    )

    results = await asyncio.gather(*(verifier.verify_async(token) for _ in range(10)))

    assert results == [None] * 10
    assert keys.fetches == 1


@pytest.mark.asyncio
async def test_unknown_kid_cooldown_keeps_keys_from_the_healthy_jwks_usable() -> None:
    verifier = OidcTokenVerifier(
        jwks_url="https://login.example.com/jwks",
        algorithms=["HS256"],
        issuer=_OIDC_ISS,
        audience=_AUD,
    )
    valid_key = jwt.PyJWK.from_dict(
        {
            "kty": "oct",
            "kid": "valid-key",
            "alg": "HS256",
            "k": base64.urlsafe_b64encode(_OIDC_SECRET.encode()).rstrip(b"=").decode(),
        }
    )

    class HealthyKeys:
        def get_signing_keys(self, *, refresh: bool = False) -> list[object]:
            return [valid_key]

    verifier._jwks = HealthyKeys()  # type: ignore[reportPrivateUsage]
    unknown = jwt.encode(
        {"sub": "attacker"},
        _OIDC_SECRET,
        algorithm="HS256",
        headers={"kid": "unknown-key"},
    )
    valid = jwt.encode(
        {
            "sub": "external-user",
            "iss": _OIDC_ISS,
            "aud": _AUD,
            "exp": int(time.time()) + 300,
        },
        _OIDC_SECRET,
        algorithm="HS256",
        headers={"kid": "valid-key"},
    )

    assert await verifier.verify_async(unknown) is None
    assert await verifier.verify_async(valid) is not None


@pytest.mark.asyncio
async def test_concurrent_malformed_jwks_response_enters_cooldown() -> None:
    verifier = OidcTokenVerifier(
        jwks_url="https://login.example.com/jwks",
        algorithms=["RS256"],
        issuer=_OIDC_ISS,
        audience=_AUD,
    )

    class MalformedKeys:
        def __init__(self) -> None:
            self.fetches = 0

        def get_signing_keys(self, *, refresh: bool = False) -> list[object]:
            self.fetches += 1
            raise ValueError(f"malformed JWKS, refresh={refresh}")

    keys = MalformedKeys()
    verifier._jwks = keys  # type: ignore[reportPrivateUsage]
    token = jwt.encode(
        {"sub": "external"},
        _OIDC_SECRET,
        algorithm="HS256",
        headers={"kid": "malformed-key"},
    )

    results = await asyncio.gather(*(verifier.verify_async(token) for _ in range(10)))

    assert results == [None] * 10
    assert keys.fetches == 1


@pytest.mark.asyncio
async def test_composite_verifier_accepts_built_in_and_external_tokens() -> None:
    verifier = CompositeTokenVerifier(_verifier(), _oauth_verifier())

    built_in = await verifier.verify_token(_token())
    external = await verifier.verify_token(_oauth_token())
    assert built_in is not None and built_in.subject == str(_PRINCIPAL_ID)
    assert external is not None and external.client_id == "coding-tool"
