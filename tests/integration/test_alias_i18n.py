"""Vietnamese entity aliases resolve with their diacritics preserved."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_vietnamese_alias_resolves_and_accents_are_significant(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        entity = await uow.canonical.create(
            group_id=group,
            entity_type="Team",
            canonical_name="Đội nền tảng thanh toán",
            aliases=["Nhóm Nền Tảng"],
        )
        await uow.commit()

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        # The same name in any case resolves back to the entity (alias_norm is Unicode-aware).
        found = await uow.canonical.resolve(group_id=group, name="đội nền tảng thanh toán")
        assert found is not None and found.id == entity.id
        # The registered alias resolves too.
        by_alias = await uow.canonical.resolve(group_id=group, name="nhóm nền tảng")
        assert by_alias is not None and by_alias.id == entity.id
        # Accents carry meaning: an accent-stripped variant is a different name and must miss.
        stripped = await uow.canonical.resolve(group_id=group, name="doi nen tang thanh toan")
        assert stripped is None
