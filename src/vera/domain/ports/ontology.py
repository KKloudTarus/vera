"""Ontology repository port: the active ontology version stamped onto episodes."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID


class OntologyRepository(Protocol):
    async def get_active_id(self) -> UUID | None:
        """The id of the highest-numbered ontology version, or None if none exist."""
        ...

    async def ensure(
        self, *, version: int, name: str, entity_types: list[str], edge_types: list[str]
    ) -> UUID:
        """Insert the ontology version if absent (by version number); return its id."""
        ...
