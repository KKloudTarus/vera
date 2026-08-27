"""Identity-context enums and value types.

Closed sets that both the domain and the persistence layer reference, so the
allowed values live in one place.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID


class PrincipalKind(StrEnum):
    USER = "user"
    SERVICE_ACCOUNT = "service_account"


class Role(StrEnum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"


# Roles are totally ordered by privilege. A higher rank subsumes the powers of every
# lower one, so an authorization check is a single comparison.
_ROLE_RANK: dict[Role, int] = {
    Role.VIEWER: 0,
    Role.MEMBER: 1,
    Role.ADMIN: 2,
    Role.OWNER: 3,
}


def role_at_least(role: Role, minimum: Role) -> bool:
    return _ROLE_RANK[role] >= _ROLE_RANK[minimum]


class CredentialKind(StrEnum):
    API_KEY = "api_key"
    OAUTH = "oauth"


@dataclass(frozen=True, slots=True)
class Organization:
    id: UUID
    slug: str
    name: str
    group_id: str


@dataclass(frozen=True, slots=True)
class Workspace:
    id: UUID
    org_id: UUID
    slug: str
    name: str
    group_id: str


@dataclass(frozen=True, slots=True)
class Project:
    id: UUID
    workspace_id: UUID
    slug: str
    name: str
    group_id: str


@dataclass(frozen=True, slots=True)
class Principal:
    id: UUID
    kind: PrincipalKind
    display_name: str
    email: str | None
    personal_group_id: str


@dataclass(frozen=True, slots=True)
class ServiceAccount:
    id: UUID
    owner_principal_id: UUID
    workspace_id: UUID
    name: str
    description: str | None


@dataclass(frozen=True, slots=True)
class Membership:
    id: UUID
    principal_id: UUID
    workspace_id: UUID
    project_id: UUID | None
    role: Role


@dataclass(frozen=True, slots=True)
class Credential:
    id: UUID
    principal_id: UUID | None
    service_account_id: UUID | None
    kind: CredentialKind
    key_prefix: str
    hashed_secret: str
    expires_at: datetime | None
    revoked_at: datetime | None


@dataclass(frozen=True, slots=True)
class AuthenticatedPrincipal:
    """The identity an authenticator resolves from a credential or token.

    A service-account credential resolves to its owning principal, so downstream
    scope resolution is uniform. ``via_service_account`` records the actor for audit.
    """

    id: UUID
    kind: PrincipalKind
    display_name: str
    personal_group_id: str
    via_service_account_id: UUID | None = None
