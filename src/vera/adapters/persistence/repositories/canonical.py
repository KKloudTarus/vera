"""Canonical entity repository: create entities with aliases, and resolve a surface
form to a canonical entity. Group-scoped tables, so the caller sets the RLS tenant
via ``UnitOfWork.use_tenant`` first.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.canonical import CanonicalEntityRow, EntityAliasRow
from vera.domain.knowledge.models import CanonicalEntity
from vera.shared.text import normalize_name


def _to_entity(row: CanonicalEntityRow) -> CanonicalEntity:
    return CanonicalEntity(
        id=row.id,
        group_id=row.group_id,
        entity_type=row.entity_type,
        canonical_name=row.canonical_name,
    )


class SqlAlchemyCanonicalEntityRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        group_id: str,
        entity_type: str,
        canonical_name: str,
        aliases: list[str],
        embedding: list[float] | None = None,
    ) -> CanonicalEntity:
        entity = CanonicalEntityRow(
            group_id=group_id,
            entity_type=entity_type,
            canonical_name=canonical_name,
            name_embedding=embedding,
        )
        self._session.add(entity)
        await self._session.flush()
        # Dedup by normalized form: the canonical name and an alias can collapse to
        # the same alias_norm, which the (group_id, alias_norm) unique index rejects.
        seen: set[str] = set()
        for alias in (canonical_name, *aliases):
            norm = normalize_name(alias)
            if norm in seen:
                continue
            seen.add(norm)
            self._session.add(
                EntityAliasRow(canonical_entity_id=entity.id, group_id=group_id, alias=alias)
            )
        await self._session.flush()
        return _to_entity(entity)

    async def resolve(self, *, group_id: str, name: str) -> CanonicalEntity | None:
        norm = normalize_name(name)
        exact = (
            select(CanonicalEntityRow)
            .join(EntityAliasRow, EntityAliasRow.canonical_entity_id == CanonicalEntityRow.id)
            .where(EntityAliasRow.group_id == group_id, EntityAliasRow.alias_norm == norm)
            .limit(1)
        )
        row = (await self._session.execute(exact)).scalars().first()
        return _to_entity(row) if row is not None else None

    async def add_alias(self, *, entity_id: UUID, group_id: str, alias: str) -> None:
        norm = normalize_name(alias)
        exists = await self._session.scalar(
            select(EntityAliasRow.id).where(
                EntityAliasRow.group_id == group_id, EntityAliasRow.alias_norm == norm
            )
        )
        if exists is not None:
            return
        self._session.add(
            EntityAliasRow(canonical_entity_id=entity_id, group_id=group_id, alias=alias)
        )
        await self._session.flush()

    async def candidates_with_embeddings(
        self, *, group_id: str, entity_type: str
    ) -> list[tuple[CanonicalEntity, list[float]]]:
        rows = (
            await self._session.execute(
                select(CanonicalEntityRow).where(
                    CanonicalEntityRow.group_id == group_id,
                    CanonicalEntityRow.entity_type == entity_type,
                    CanonicalEntityRow.name_embedding.is_not(None),
                )
            )
        ).scalars()
        return [(_to_entity(row), [float(x) for x in (row.name_embedding or [])]) for row in rows]

    async def without_embeddings(self, *, group_id: str) -> list[CanonicalEntity]:
        rows = (
            await self._session.execute(
                select(CanonicalEntityRow).where(
                    CanonicalEntityRow.group_id == group_id,
                    CanonicalEntityRow.name_embedding.is_(None),
                )
            )
        ).scalars()
        return [_to_entity(row) for row in rows]

    async def set_embedding(self, *, entity_id: UUID, embedding: list[float]) -> None:
        await self._session.execute(
            update(CanonicalEntityRow)
            .where(CanonicalEntityRow.id == entity_id)
            .values(name_embedding=embedding)
        )
        await self._session.flush()
