"""Control-plane schema: UoW atomicity, tenancy hierarchy, canonical resolution,
and row-level security isolation. Runs against the live database.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.shared.ids import deterministic_id, uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _suffix() -> str:
    return uuid7().hex[:12]


async def test_uow_commits_aggregate_and_outbox_atomically(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    sfx = _suffix()
    dedup = deterministic_id(f"source:{sfx}")
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        org = await uow.tenancy.create_organization(
            slug=f"org-{sfx}", name="Acme", group_id=f"o:{sfx}"
        )
        await uow.outbox.add(
            group_id=f"o:{sfx}", source_id=f"source:{sfx}", dedup_uuid=dedup, payload={"k": 1}
        )
        await uow.commit()

    async with sessionmaker() as s:
        org_count = await s.scalar(
            text("SELECT count(*) FROM organizations WHERE id = :id"), {"id": org.id}
        )
        job_count = await s.scalar(
            text("SELECT count(*) FROM ingestion_jobs WHERE dedup_uuid = :d"), {"d": dedup}
        )
    assert org_count == 1
    assert job_count == 1


async def test_uow_rolls_back_on_error(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    sfx = _suffix()
    with pytest.raises(RuntimeError):
        async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
            await uow.tenancy.create_organization(
                slug=f"org-{sfx}", name="Rollback", group_id=f"o:{sfx}"
            )
            raise RuntimeError("boom before commit")

    async with sessionmaker() as s:
        count = await s.scalar(
            text("SELECT count(*) FROM organizations WHERE slug = :slug"), {"slug": f"org-{sfx}"}
        )
    assert count == 0


async def test_tenancy_hierarchy_round_trip(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    sfx = _suffix()
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        org = await uow.tenancy.create_organization(
            slug=f"org-{sfx}", name="Org", group_id=f"o:{sfx}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"ws-{sfx}", name="Platform", group_id=f"w:{sfx}"
        )
        proj = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{sfx}", name="Landing", group_id=f"p:{sfx}"
        )
        await uow.commit()
        project_id = proj.id

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        fetched = await uow.tenancy.get_project(project_id)
    assert fetched is not None
    assert fetched.group_id == f"p:{sfx}"


async def test_canonical_resolve_requires_exact_normalized_alias(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    sfx = _suffix()
    group = f"p:{sfx}"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        await uow.canonical.create(
            group_id=group,
            entity_type="Service",
            canonical_name="payment-api",
            aliases=["Payment API"],
        )
        await uow.commit()

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        exact = await uow.canonical.resolve(group_id=group, name="Payment  API")
        near_miss = await uow.canonical.resolve(group_id=group, name="payment apis")
    assert exact is not None
    assert exact.canonical_name == "payment-api"
    assert near_miss is None


async def test_rls_blocks_cross_tenant_reads(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    sfx = _suffix()
    group_a = f"p:a-{sfx}"
    group_b = f"p:b-{sfx}"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group_a)
        await uow.canonical.create(
            group_id=group_a, entity_type="Service", canonical_name=f"svc-{sfx}", aliases=[]
        )
        await uow.commit()

    # Tenant B cannot resolve tenant A's entity.
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group_b)
        seen = await uow.canonical.resolve(group_id=group_a, name=f"svc-{sfx}")
    assert seen is None

    # As the app role with no tenant set, RLS hides every row.
    async with sessionmaker() as s, s.begin():
        await s.execute(text("SET LOCAL ROLE vera_app"))
        visible = await s.scalar(text("SELECT count(*) FROM canonical_entities"))
    assert visible == 0
