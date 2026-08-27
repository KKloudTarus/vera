"""Identity, tenancy, and access control against the live database.

Covers the write side (IdentityService with RBAC), both authentication paths (API key
and OIDC), and scope resolution: two principals in different workspaces resolve to
disjoint group_ids, so neither can read the other's memory.
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import jwt
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.identity import (
    ApiKeyAuthenticator,
    OidcAuthenticator,
    OidcTokenVerifier,
)
from vera.adapters.persistence.repositories.scope import SqlAlchemyScopeResolver
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.identity import IdentityService, ScopeResolutionService
from vera.domain.identity.models import AuthenticatedPrincipal, PrincipalKind, Role
from vera.shared.errors import Err, Ok

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_OIDC_SECRET = "oidc-signing-secret-long-enough-for-hs256"  # noqa: S105
_OIDC_ISS = "https://idp.example"
_OIDC_AUD = "https://api.vera.local"


@asynccontextmanager
async def _identity(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[IdentityService, None]:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        yield IdentityService(uow)
        await uow.commit()


async def _authed(
    sessionmaker: async_sessionmaker[AsyncSession], api_key: str
) -> AuthenticatedPrincipal:
    principal = await ApiKeyAuthenticator(sessionmaker).authenticate(api_key)
    assert principal is not None
    return principal


async def test_register_then_authenticate_with_api_key(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with _identity(sessionmaker) as svc:
        principal, issued = await svc.register(display_name="Alice", email="alice@example.com")

    authenticator = ApiKeyAuthenticator(sessionmaker)
    resolved = await authenticator.authenticate(issued.api_key)
    assert resolved is not None
    assert resolved.id == principal.id
    assert resolved.kind is PrincipalKind.USER
    assert resolved.personal_group_id == principal.personal_group_id

    assert await authenticator.authenticate("vera_unknown.secret") is None
    assert await authenticator.authenticate(issued.api_key + "tamper") is None


async def test_revoked_key_stops_authenticating(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with _identity(sessionmaker) as svc:
        _, issued = await svc.register(display_name="Temp")
    actor = await _authed(sessionmaker, issued.api_key)
    assert await ApiKeyAuthenticator(sessionmaker).authenticate(issued.api_key) is not None

    async with _identity(sessionmaker) as svc:
        result = await svc.revoke_credential(actor=actor, credential_id=issued.credential_id)
    assert isinstance(result, Ok)
    assert await ApiKeyAuthenticator(sessionmaker).authenticate(issued.api_key) is None


async def test_owner_builds_tenancy_and_member_is_denied(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with _identity(sessionmaker) as svc:
        _, admin_key = await svc.register(display_name="Admin")
    admin = await _authed(sessionmaker, admin_key.api_key)

    async with _identity(sessionmaker) as svc:
        org = await svc.create_organization(name="Acme", slug=f"acme-{org_suffix()}")
        workspace = await svc.create_workspace(
            actor=admin, org_id=org.id, name="Platform", slug="platform"
        )
        project = await svc.create_project(
            actor=admin, workspace_id=workspace.id, name="Api", slug="api"
        )
    assert isinstance(project, Ok)  # the owner may create a project

    async with _identity(sessionmaker) as svc:
        member, member_key = await svc.register(display_name="Bob")
    member_principal = await _authed(sessionmaker, member_key.api_key)

    async with _identity(sessionmaker) as svc:
        added = await svc.add_member(
            actor=admin, workspace_id=workspace.id, principal_id=member.id, role=Role.MEMBER
        )
    assert isinstance(added, Ok)

    # A plain member may not create projects or add members.
    async with _identity(sessionmaker) as svc:
        denied_project = await svc.create_project(
            actor=member_principal, workspace_id=workspace.id, name="X", slug="x"
        )
        denied_member = await svc.add_member(
            actor=member_principal,
            workspace_id=workspace.id,
            principal_id=admin.id,
            role=Role.ADMIN,
        )
    assert isinstance(denied_project, Err) and denied_project.error.code == "forbidden"
    assert isinstance(denied_member, Err) and denied_member.error.code == "forbidden"


async def test_scopes_are_isolated_between_workspaces(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with _identity(sessionmaker) as svc:
        _, admin_key = await svc.register(display_name="Admin")
    admin = await _authed(sessionmaker, admin_key.api_key)

    async with _identity(sessionmaker) as svc:
        org = await svc.create_organization(name="Acme", slug=f"acme-{org_suffix()}")
        ws_a = await svc.create_workspace(actor=admin, org_id=org.id, name="A", slug="a")
        ws_b = await svc.create_workspace(actor=admin, org_id=org.id, name="B", slug="b")
        alice, _ = await svc.register(display_name="Alice")
        bob, _ = await svc.register(display_name="Bob")
        await svc.add_member(
            actor=admin, workspace_id=ws_a.id, principal_id=alice.id, role=Role.MEMBER
        )
        await svc.add_member(
            actor=admin, workspace_id=ws_b.id, principal_id=bob.id, role=Role.MEMBER
        )

    scopes = ScopeResolutionService(SqlAlchemyScopeResolver(sessionmaker))
    alice_groups = await scopes.allowed_group_ids(alice.id)
    bob_groups = await scopes.allowed_group_ids(bob.id)

    assert ws_a.group_id in alice_groups
    assert ws_a.group_id not in bob_groups
    assert ws_b.group_id in bob_groups
    assert ws_b.group_id not in alice_groups
    assert alice.personal_group_id in alice_groups
    assert not await scopes.can_read(alice.id, ws_b.group_id)
    assert not await scopes.can_read(bob.id, ws_a.group_id)


async def test_service_account_key_resolves_a_member_principal(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with _identity(sessionmaker) as svc:
        _, admin_key = await svc.register(display_name="Admin")
    admin = await _authed(sessionmaker, admin_key.api_key)

    async with _identity(sessionmaker) as svc:
        org = await svc.create_organization(name="Acme", slug=f"acme-{org_suffix()}")
        workspace = await svc.create_workspace(actor=admin, org_id=org.id, name="W", slug="w")
        created = await svc.create_service_account(
            actor=admin, workspace_id=workspace.id, name="ci-bot"
        )
    assert isinstance(created, Ok)
    account, issued = created.value

    resolved = await ApiKeyAuthenticator(sessionmaker).authenticate(issued.api_key)
    assert resolved is not None
    assert resolved.kind is PrincipalKind.SERVICE_ACCOUNT
    assert resolved.via_service_account_id == account.id

    scopes = ScopeResolutionService(SqlAlchemyScopeResolver(sessionmaker))
    assert workspace.group_id in await scopes.allowed_group_ids(account.id)


async def test_oidc_login_provisions_then_resolves_the_same_principal(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    verifier = OidcTokenVerifier(
        signing_key=_OIDC_SECRET,
        algorithms=["HS256"],
        issuer=_OIDC_ISS,
        audience=_OIDC_AUD,
    )
    authenticator = OidcAuthenticator(sessionmaker, verifier)
    token = jwt.encode(
        {
            "sub": f"idp-user-{org_suffix()}",
            "iss": _OIDC_ISS,
            "aud": _OIDC_AUD,
            "email": f"user-{org_suffix()}@example.com",
            "exp": int(time.time()) + 3600,
        },
        _OIDC_SECRET,
        algorithm="HS256",
    )

    first = await authenticator.authenticate(token)
    second = await authenticator.authenticate(token)
    assert first is not None and second is not None
    assert first.id == second.id  # JIT provisioning is stable across logins
    assert first.kind is PrincipalKind.USER

    forged = jwt.encode(
        {"sub": "attacker", "iss": _OIDC_ISS, "aud": _OIDC_AUD, "exp": int(time.time()) + 3600},
        "a-different-wrong-signing-key-32-bytes!!",
        algorithm="HS256",
    )
    assert await authenticator.authenticate(forged) is None


async def test_admin_manages_a_members_api_key(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    # An admin can issue and revoke keys for a principal that belongs to a workspace it
    # administers; a fresh, unrelated principal cannot.
    async with _identity(sessionmaker) as svc:
        _, admin_key = await svc.register(display_name="Admin")
    admin = await _authed(sessionmaker, admin_key.api_key)

    async with _identity(sessionmaker) as svc:
        org = await svc.create_organization(name="Acme", slug=f"acme-{org_suffix()}")
        ws = await svc.create_workspace(actor=admin, org_id=org.id, name="Platform", slug="plat")
        member = await svc.create_principal(display_name="Member", email=None)
        await svc.add_member(
            actor=admin, workspace_id=ws.id, principal_id=member.id, role=Role.MEMBER
        )

    # The admin issues a key for the member.
    async with _identity(sessionmaker) as svc:
        issued = _ok(await svc.issue_api_key(actor=admin, principal_id=member.id))
    assert await ApiKeyAuthenticator(sessionmaker).authenticate(issued.api_key) is not None

    # An outsider (no shared workspace) cannot manage that credential.
    async with _identity(sessionmaker) as svc:
        _, outsider_key = await svc.register(display_name="Outsider")
    outsider = await _authed(sessionmaker, outsider_key.api_key)
    async with _identity(sessionmaker) as svc:
        denied = await svc.revoke_credential(actor=outsider, credential_id=issued.credential_id)
    assert isinstance(denied, Err)
    assert await ApiKeyAuthenticator(sessionmaker).authenticate(issued.api_key) is not None

    # The admin revokes it.
    async with _identity(sessionmaker) as svc:
        assert isinstance(
            await svc.revoke_credential(actor=admin, credential_id=issued.credential_id), Ok
        )
    assert await ApiKeyAuthenticator(sessionmaker).authenticate(issued.api_key) is None


async def test_rotate_replaces_the_key(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    async with _identity(sessionmaker) as svc:
        _, first = await svc.register(display_name="Rotator")
    actor = await _authed(sessionmaker, first.api_key)

    async with _identity(sessionmaker) as svc:
        rotated = _ok(await svc.rotate_api_key(actor=actor, credential_id=first.credential_id))

    auth = ApiKeyAuthenticator(sessionmaker)
    assert await auth.authenticate(first.api_key) is None  # old key revoked
    assert await auth.authenticate(rotated.api_key) is not None  # new key works
    # Rotating an already-revoked credential is a conflict.
    async with _identity(sessionmaker) as svc:
        again = await svc.rotate_api_key(actor=actor, credential_id=first.credential_id)
    assert isinstance(again, Err)


def _ok(result: object) -> object:
    assert isinstance(result, Ok)
    return result.value


def org_suffix() -> str:
    from vera.shared.ids import uuid7

    return uuid7().hex[:10]
