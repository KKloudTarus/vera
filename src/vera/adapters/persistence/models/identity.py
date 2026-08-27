"""Identity tables: principals, service_accounts, memberships, credentials.

A principal is a human user. A service account is a non-human actor owned by a
principal. Credentials belong to exactly one of them. Memberships grant a role on a
workspace, or on a single project when ``project_id`` is set.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from vera.adapters.persistence.base import Base
from vera.adapters.persistence.models._mixins import UUIDPK, Timestamps
from vera.domain.identity.models import CredentialKind, PrincipalKind, Role

_ROLES = ", ".join(f"'{r.value}'" for r in Role)
_CRED_KINDS = ", ".join(f"'{k.value}'" for k in CredentialKind)


class PrincipalRow(Base, UUIDPK, Timestamps):
    __tablename__ = "principals"

    kind: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=PrincipalKind.USER.value
    )
    email: Mapped[str | None] = mapped_column(String(320), nullable=True, unique=True)
    display_name: Mapped[str] = mapped_column(String(256), nullable=False)
    personal_group_id: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)


class ServiceAccountRow(Base, UUIDPK, Timestamps):
    __tablename__ = "service_accounts"

    name: Mapped[str] = mapped_column(String(256), nullable=False)
    owner_principal_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("principals.id", ondelete="CASCADE"), nullable=False
    )
    workspace_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False
    )
    description: Mapped[str | None] = mapped_column(String(512), nullable=True)


class MembershipRow(Base, UUIDPK, Timestamps):
    __tablename__ = "memberships"

    principal_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("principals.id", ondelete="CASCADE"), nullable=False
    )
    workspace_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False
    )
    project_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=True
    )
    role: Mapped[str] = mapped_column(String(32), nullable=False)

    __table_args__ = (
        # A NULL project_id means the membership covers the whole workspace.
        UniqueConstraint("principal_id", "workspace_id", "project_id", name="uq_membership_scope"),
        CheckConstraint(f"role IN ({_ROLES})", name="ck_membership_role"),
    )


class CredentialRow(Base, UUIDPK, Timestamps):
    __tablename__ = "credentials"

    principal_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("principals.id", ondelete="CASCADE"), nullable=True
    )
    service_account_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("service_accounts.id", ondelete="CASCADE"), nullable=True
    )
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    key_prefix: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    hashed_secret: Mapped[str] = mapped_column(String(256), nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(f"kind IN ({_CRED_KINDS})", name="ck_credential_kind"),
        # Exactly one owner: a principal or a service account, never both or neither.
        CheckConstraint(
            "(principal_id IS NOT NULL)::int + (service_account_id IS NOT NULL)::int = 1",
            name="ck_credential_one_owner",
        ),
    )
