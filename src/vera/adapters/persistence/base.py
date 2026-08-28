"""SQLAlchemy 2.0 async engine, session factory, and declarative base.

Async footguns handled here per the standards: ``expire_on_commit=False`` (avoid
lazy IO after commit) and ``pool_pre_ping`` (transparent reconnect). A session is
minted per unit of work, never shared across concurrent tasks.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import Connection, event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session

from vera.config.settings import DatabaseSettings
from vera.shared.errors import ConfigError

# The only roles a session may assume at runtime. Fixed so the role name is never attacker
# controlled when interpolated into SET LOCAL ROLE.
_RUNTIME_ROLES = frozenset({"vera_app", "vera_trusted", "vera_worker"})


class Base(DeclarativeBase):
    """Declarative base for adapter-owned ORM tables (kept out of the domain)."""


def create_engine(db: DatabaseSettings) -> AsyncEngine:
    return create_async_engine(
        str(db.dsn),
        pool_size=db.pool_size,
        max_overflow=db.max_overflow,
        pool_pre_ping=db.pool_pre_ping,
        pool_recycle=db.pool_recycle_s,
        echo=db.echo,
    )


def create_sessionmaker(
    engine: AsyncEngine, *, role: str | None = None
) -> async_sessionmaker[AsyncSession]:
    """A session factory. When ``role`` is given, every transaction begins with
    ``SET LOCAL ROLE <role>``, so the read and worker paths assume their non-superuser roles
    even when the connection logs in as a superuser. SET LOCAL is transaction-scoped and
    resets on commit or rollback, so it is safe under PgBouncer transaction pooling.
    """
    if role is None:
        return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    if role not in _RUNTIME_ROLES:
        raise ConfigError(f"unknown runtime role {role!r}")

    class _RoleSession(Session):
        pass

    @event.listens_for(_RoleSession, "after_begin")
    def _set_role(  # pyright: ignore[reportUnusedFunction]  (registered by the decorator)
        _session: Session, _transaction: Any, connection: Connection
    ) -> None:
        connection.exec_driver_sql(f"SET LOCAL ROLE {role}")

    return async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession, sync_session_class=_RoleSession
    )
