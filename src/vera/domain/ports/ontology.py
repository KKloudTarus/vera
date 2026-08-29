"""Ontology repository port: the active ontology version stamped onto episodes."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from vera.domain.ontology import OntologyDescriptor


class OntologyRepository(Protocol):
    async def get_active_id(self) -> UUID | None:
        """The id of the highest-numbered ontology version, or None if none exist."""
        ...

    async def get_active(self) -> OntologyDescriptor | None:
        """The full descriptor of the highest-numbered version, or None if none exist."""
        ...

    async def get_version(self, version: int) -> OntologyDescriptor | None:
        """A specific immutable ontology descriptor, or None when absent."""
        ...

    async def ensure(
        self, *, version: int, name: str, entity_types: list[str], edge_types: list[str]
    ) -> UUID:
        """Insert the ontology version if absent (by version number); return its id."""
        ...

    async def ensure_current(self, descriptor: OntologyDescriptor) -> UUID:
        """Insert the descriptor's version if absent, including its predicate policies, and
        return its id. Existing rows are never mutated here: a divergence between code and a
        stored row is a drift to report, not to silently overwrite.
        """
        ...
