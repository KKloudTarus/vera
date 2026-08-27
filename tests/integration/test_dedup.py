"""Semantic dedup against the live database: backfill embeddings and link synonyms.

Proves the two persistence methods the backfill and resolver rely on (list entities
lacking an embedding, store one) and that the resolver merges a synonym into a known
entity using embeddings read back from Postgres.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation.entity_resolver import SemanticEntityResolver
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _FakeEmbedder:
    def __init__(self, vectors: dict[str, list[float]]) -> None:
        self._vectors = vectors

    async def embed(self, text: str) -> list[float]:
        return self._vectors[text]


async def test_backfill_lists_then_stores_embeddings(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        entity = await uow.canonical.create(
            group_id=group, entity_type="Service", canonical_name="paymentapi", aliases=[]
        )
        await uow.commit()

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        pending = await uow.canonical.without_embeddings(group_id=group)
        assert [e.id for e in pending] == [entity.id]
        await uow.canonical.set_embedding(entity_id=entity.id, embedding=[1.0, 0.0, 0.0])
        await uow.commit()

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        assert await uow.canonical.without_embeddings(group_id=group) == []
        cands = await uow.canonical.candidates_with_embeddings(
            group_id=group, entity_type="Service"
        )
    assert [(e.id, v) for e, v in cands] == [(entity.id, [1.0, 0.0, 0.0])]


async def test_resolver_links_synonym_via_stored_embedding(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    resolver = SemanticEntityResolver(
        _FakeEmbedder({"paymentapi": [1.0, 0.0, 0.0], "payment service": [0.95, 0.31, 0.0]}),
        threshold=0.86,
        enabled=True,
    )
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        first = await resolver.resolve_or_create(
            uow.canonical, group_id=group, name="paymentapi", entity_type="Service"
        )
        second = await resolver.resolve_or_create(
            uow.canonical, group_id=group, name="payment service", entity_type="Service"
        )
        await uow.commit()
    assert second.id == first.id  # the synonym linked to the same canonical entity

    # And the alias now resolves exactly, without the embedding step.
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        resolved = await uow.canonical.resolve(group_id=group, name="payment service")
    assert resolved is not None and resolved.id == first.id
