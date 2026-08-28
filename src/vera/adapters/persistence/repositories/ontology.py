"""Ontology repository: reads and seeds ``ontology_versions``.

The active ontology is the highest version number; published episodes reference its id
so a later reprocess knows which ontology produced each episode. The row also carries the
per-predicate governance policies so reconciliation reads the rules the version shipped with.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.models.ops import OntologyVersionRow
from vera.domain.ontology import OntologyDescriptor, descriptor_from_row


class SqlAlchemyOntologyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_active_id(self) -> UUID | None:
        return await self._session.scalar(
            select(OntologyVersionRow.id).order_by(OntologyVersionRow.version.desc()).limit(1)
        )

    async def get_active(self) -> OntologyDescriptor | None:
        row = await self._session.scalar(
            select(OntologyVersionRow).order_by(OntologyVersionRow.version.desc()).limit(1)
        )
        if row is None:
            return None
        return descriptor_from_row(
            id=row.id,
            version=row.version,
            name=row.name,
            entity_types=list(row.entity_types.get("types", [])),
            edge_types=list(row.edge_types.get("types", [])),
            predicate_policies=row.predicate_policies,
        )

    async def ensure(
        self, *, version: int, name: str, entity_types: list[str], edge_types: list[str]
    ) -> UUID:
        await self._session.execute(
            pg_insert(OntologyVersionRow)
            .values(
                version=version,
                name=name,
                entity_types={"types": entity_types},
                edge_types={"types": edge_types},
            )
            .on_conflict_do_nothing(index_elements=[OntologyVersionRow.version])
        )
        existing = await self._session.scalar(
            select(OntologyVersionRow.id).where(OntologyVersionRow.version == version)
        )
        if existing is None:  # pragma: no cover - insert above guarantees a row
            raise RuntimeError(f"ontology version {version} could not be ensured")
        return existing

    async def ensure_current(self, descriptor: OntologyDescriptor) -> UUID:
        await self._session.execute(
            pg_insert(OntologyVersionRow)
            .values(
                version=descriptor.version,
                name=descriptor.name,
                entity_types={"types": list(descriptor.entity_types)},
                edge_types={"types": list(descriptor.edge_types)},
                predicate_policies=descriptor.policies_as_json(),
            )
            .on_conflict_do_nothing(index_elements=[OntologyVersionRow.version])
        )
        existing = await self._session.scalar(
            select(OntologyVersionRow.id).where(OntologyVersionRow.version == descriptor.version)
        )
        if existing is None:  # pragma: no cover - insert above guarantees a row
            raise RuntimeError(f"ontology version {descriptor.version} could not be ensured")
        return existing
