"""Canonical entity repository: create entities with aliases, and resolve a surface
form to a canonical entity. Group-scoped tables, so the caller sets the RLS tenant
via ``UnitOfWork.use_tenant`` first.
"""

from __future__ import annotations

from sqlalchemy import func, select
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
        self, *, group_id: str, entity_type: str, canonical_name: str, aliases: list[str]
    ) -> CanonicalEntity:
        entity = CanonicalEntityRow(
            group_id=group_id, entity_type=entity_type, canonical_name=canonical_name
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
        if row is not None:
            return _to_entity(row)

        # Fuzzy fallback for near misses only (pg_trgm similarity).
        fuzzy = (
            select(CanonicalEntityRow)
            .join(EntityAliasRow, EntityAliasRow.canonical_entity_id == CanonicalEntityRow.id)
            .where(
                EntityAliasRow.group_id == group_id,
                EntityAliasRow.alias_norm.op("%")(norm),
            )
            .order_by(func.similarity(EntityAliasRow.alias_norm, norm).desc())
            .limit(1)
        )
        row = (await self._session.execute(fuzzy)).scalars().first()
        return _to_entity(row) if row is not None else None
