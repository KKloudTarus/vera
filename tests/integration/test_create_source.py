"""Creating a knowledge source (the row connectors ingest into) against the live DB."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.domain.knowledge.models import SourceKind
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_create_source_and_read_back(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    sfx = uuid7().hex[:12]
    group = f"p:{sfx}"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{sfx}", name="Org", group_id=f"o:{sfx}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{sfx}", name="WS", group_id=f"w:{sfx}"
        )
        proj = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{sfx}", name="Proj", group_id=group
        )
        source_id = await uow.sources.create(
            workspace_id=ws.id,
            project_id=proj.id,
            kind=SourceKind.FILESYSTEM.value,
            name="Team docs",
            trust_tier=1,
        )
        await uow.commit()

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        source = await uow.sources.get(source_id)
    assert source is not None
    assert source.trust_tier == 1
