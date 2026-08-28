"""Role-bound session factories actually assume vera_trusted / vera_worker (gap 16)."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from vera.adapters.persistence.base import create_sessionmaker
from vera.shared.errors import ConfigError

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_read_and_worker_factories_set_their_roles(engine: AsyncEngine) -> None:
    reads = create_sessionmaker(engine, role="vera_trusted")
    workers = create_sessionmaker(engine, role="vera_worker")
    base = create_sessionmaker(engine)

    async with reads() as s:
        assert (await s.scalar(text("SELECT current_user"))) == "vera_trusted"
    async with workers() as s:
        assert (await s.scalar(text("SELECT current_user"))) == "vera_worker"
    # The base factory does not switch roles; it stays the login role.
    async with base() as s:
        assert (await s.scalar(text("SELECT current_user"))) != "vera_trusted"


async def test_role_resets_after_the_transaction(engine: AsyncEngine) -> None:
    # SET LOCAL is transaction-scoped, so the same pooled connection is clean on reuse.
    reads = create_sessionmaker(engine, role="vera_trusted")
    async with reads() as s:
        assert (await s.scalar(text("SELECT current_user"))) == "vera_trusted"
    base = create_sessionmaker(engine)
    async with base() as s:
        assert (await s.scalar(text("SELECT current_user"))) != "vera_trusted"


def test_unknown_role_is_rejected(engine: AsyncEngine) -> None:
    with pytest.raises(ConfigError):
        create_sessionmaker(engine, role="postgres")
