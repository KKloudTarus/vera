"""SQLAlchemy 2.0 async engine, session factory, and declarative base.

Async footguns handled here per the standards: ``expire_on_commit=False`` (avoid
lazy IO after commit) and ``pool_pre_ping`` (transparent reconnect). A session is
minted per unit of work, never shared across concurrent tasks.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from vera.config.settings import DatabaseSettings


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


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
