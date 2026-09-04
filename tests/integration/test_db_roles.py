"""Role-bound session factories actually assume vera_trusted / vera_worker (gap 16)."""

from __future__ import annotations

import pytest
from sqlalchemy import exc, text
from sqlalchemy.ext.asyncio import AsyncEngine

from vera.adapters.persistence.base import create_sessionmaker
from vera.adapters.persistence.repositories.usage import SqlAlchemyUsageSink
from vera.adapters.queue.postgres_queue import PostgresJobQueue
from vera.observability.cost import UsageEvent
from vera.shared.errors import ConfigError
from vera.shared.ids import uuid7
from vera.shared.types import GroupId, SourceId

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


async def test_app_session_enqueues_and_records_usage_without_worker_role(
    engine: AsyncEngine,
) -> None:
    app = create_sessionmaker(engine, role="vera_app")
    workers = create_sessionmaker(engine, role="vera_worker")
    queue = PostgresJobQueue(app, worker_session_factory=workers)
    group_id = GroupId(f"p:{uuid7().hex[:12]}")

    assert await queue.enqueue(
        group_id=group_id,
        source_id=SourceId("role-bound-source"),
        dedup_uuid=uuid7(),
        payload={"kind": "role-test"},
    )
    sink = SqlAlchemyUsageSink(app)
    await sink.record(
        UsageEvent(
            model="gpt-4.1-mini",
            operation="llm",
            prompt_tokens=1,
            completion_tokens=1,
            cost_usd=0.000002,
            request_kind="ingest",
            group_id=str(group_id),
            ref="role-bound-source",
        )
    )
    run_key = f"role-bound-run:{uuid7()}"
    action_key = f"{run_key}:action"
    await sink.initialize_provider_budget(action_key, 1.0, run_key=run_key, run_max_cost_usd=2.0)
    assert await sink.reserve_provider_budget(action_key, 0.25) is True

    jobs = await queue.claim(batch_size=1)
    assert len(jobs) == 1
    assert jobs[0].group_id == group_id


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


async def test_llm_usage_is_append_only_for_runtime_roles(engine: AsyncEngine) -> None:
    expected_by_role = {
        "vera_app": {"SELECT", "INSERT"},
        "vera_trusted": {"SELECT"},
        "vera_worker": {"SELECT", "INSERT"},
    }
    async with engine.connect() as connection:
        for role, expected in expected_by_role.items():
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                granted = await connection.scalar(
                    text("SELECT has_table_privilege(:role, 'llm_usage', :privilege)"),
                    {"role": role, "privilege": privilege},
                )
                assert granted is (privilege in expected), (role, privilege)


async def test_provider_budget_privileges_protect_the_declared_ceiling(
    engine: AsyncEngine,
) -> None:
    expected_by_role = {
        "vera_app": {"SELECT", "INSERT", "UPDATE"},
        "vera_trusted": {"SELECT"},
        "vera_worker": {"SELECT", "INSERT", "UPDATE", "DELETE"},
    }
    async with engine.connect() as connection:
        for table in ("provider_budget_reservations", "provider_run_budget_reservations"):
            for role, expected in expected_by_role.items():
                for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                    granted = await connection.scalar(
                        text("SELECT has_table_privilege(:role, :table, :privilege)"),
                        {"role": role, "table": table, "privilege": privilege},
                    )
                    assert granted is (privilege in expected), (table, role, privilege)


async def test_provider_budget_ceiling_is_immutable_for_the_app_role(engine: AsyncEngine) -> None:
    app = create_sessionmaker(engine, role="vera_app")
    sink = SqlAlchemyUsageSink(app)
    run_key = f"immutable:{uuid7()}"
    await sink.initialize_provider_budget(
        f"{run_key}:action", 1.0, run_key=run_key, run_max_cost_usd=2.0
    )

    with pytest.raises(exc.DBAPIError, match="identity and ceiling are immutable"):
        async with app() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE provider_run_budget_reservations SET max_cost_usd=3 "
                    "WHERE run_key=:run_key"
                ),
                {"run_key": run_key},
            )


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


async def test_unknown_role_is_rejected(engine: AsyncEngine) -> None:
    with pytest.raises(ConfigError):
        create_sessionmaker(engine, role="postgres")


async def test_group_roles_have_only_approved_attributes_and_no_memberships(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as connection:
        for role, bypass_rls in (
            ("vera_app", False),
            ("vera_trusted", True),
            ("vera_worker", True),
        ):
            attributes = (
                await connection.execute(
                    text(
                        "SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole, "
                        "rolreplication, rolbypassrls, ARRAY(SELECT granted.rolname "
                        "FROM pg_auth_members membership JOIN pg_roles granted "
                        "ON granted.oid=membership.roleid WHERE membership.member=r.oid) "
                        "FROM pg_roles r WHERE r.rolname=:role"
                    ),
                    {"role": role},
                )
            ).one()

            assert attributes == (False, False, False, False, False, False, bypass_rls, [])


async def test_group_role_acl_and_membership_allowlists_are_enforced(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        assert not await connection.scalar(
            text("SELECT has_table_privilege('vera_trusted', 'facts', 'UPDATE')")
        )
        assert not await connection.scalar(
            text("SELECT has_table_privilege('vera_worker', 'proposal_attempts', 'INSERT')")
        )
        unsafe_members = await connection.scalar(
            text(
                "SELECT count(*) FROM pg_auth_members membership "
                "JOIN pg_roles granted ON granted.oid=membership.roleid "
                "JOIN pg_roles member ON member.oid=membership.member "
                "WHERE (granted.rolname IN ('vera_app', 'vera_trusted', 'vera_worker') "
                "AND ((granted.rolname, member.rolname) NOT IN ("
                "('vera_app', 'vera_runtime'), ('vera_trusted', 'vera_runtime'), "
                "('vera_app', 'vera_worker_runtime'), "
                "('vera_trusted', 'vera_worker_runtime'), "
                "('vera_worker', 'vera_worker_runtime'), ('vera_app', 'vera_legacy')) "
                "OR membership.admin_option)) "
                "OR granted.rolname IN "
                "('vera_runtime', 'vera_worker_runtime', 'vera_scaler_runtime', 'vera_legacy')"
            )
        )
        direct_column_acls = await connection.scalar(
            text(
                "SELECT count(*) FROM pg_attribute attribute "
                "CROSS JOIN LATERAL aclexplode(attribute.attacl) acl "
                "JOIN pg_roles grantee ON grantee.oid=acl.grantee "
                "WHERE attribute.attnum > 0 AND NOT attribute.attisdropped "
                "AND grantee.rolname IN "
                "('vera_app', 'vera_trusted', 'vera_worker', 'vera_runtime', "
                "'vera_worker_runtime', 'vera_scaler_runtime', 'vera_legacy')"
            )
        )

    assert unsafe_members == 0
    assert direct_column_acls == 0
