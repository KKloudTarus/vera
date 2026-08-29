"""The persisted ontology registry matches the code registry (no startup drift)."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.repositories.ontology import SqlAlchemyOntologyRepository
from vera.domain.ontology import current_descriptor, detect_drift

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_persisted_ontology_has_no_drift_from_code(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    code = current_descriptor()
    async with sessionmaker() as session:
        repo = SqlAlchemyOntologyRepository(session)
        await repo.ensure_current(code)
        await session.commit()
        persisted = await repo.get_active()

    assert persisted is not None
    assert persisted.id is not None  # the active row carries a real version id
    assert persisted.version == code.version
    # The migration froze v2's policies to the code's; startup would fail on any divergence.
    assert detect_drift(code, persisted) == []
    assert {p.predicate for p in persisted.predicate_policies}  # policies are persisted, not empty
