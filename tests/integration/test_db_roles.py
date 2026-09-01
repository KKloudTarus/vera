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


async def test_proposal_attempt_privileges_are_least_authority(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        for role, privilege, expected in (
            ("vera_app", "SELECT", True),
            ("vera_app", "INSERT", True),
            ("vera_app", "UPDATE", False),
            ("vera_app", "DELETE", False),
            ("vera_trusted", "SELECT", True),
            ("vera_trusted", "INSERT", False),
            ("vera_worker", "INSERT", False),
        ):
            granted = await connection.scalar(
                text("SELECT has_table_privilege(:role, 'proposal_attempts', :privilege)"),
                {"role": role, "privilege": privilege},
            )
            assert granted is expected, (role, privilege)


async def test_legal_hold_privileges_exclude_delete(engine: AsyncEngine) -> None:
    expected_by_role = {
        "vera_app": {"SELECT", "INSERT", "UPDATE"},
        "vera_trusted": {"SELECT"},
        "vera_worker": {"SELECT", "INSERT", "UPDATE"},
    }
    async with engine.connect() as connection:
        for role, expected in expected_by_role.items():
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                granted = await connection.scalar(
                    text("SELECT has_table_privilege(:role, 'legal_holds', :privilege)"),
                    {"role": role, "privilege": privilege},
                )
                assert granted is (privilege in expected), (role, privilege)


async def test_global_context_pack_cleanup_is_worker_only(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        assert (
            await connection.scalar(text("SELECT to_regclass('ix_context_packs_expires_at')"))
            == "ix_context_packs_expires_at"
        )
        assert await connection.scalar(
            text("SELECT has_column_privilege('vera_erasure', 'context_packs', 'id', 'UPDATE')")
        )
        for role, expected in (
            ("vera_worker", True),
            ("vera_app", False),
            ("vera_trusted", False),
        ):
            granted = await connection.scalar(
                text(
                    "SELECT has_function_privilege("
                    ":role, 'delete_all_expired_context_packs()', 'EXECUTE')"
                ),
                {"role": role},
            )
            assert granted is expected, role


def test_unknown_role_is_rejected(engine: AsyncEngine) -> None:
    with pytest.raises(ConfigError):
        create_sessionmaker(engine, role="postgres")
