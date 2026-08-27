"""Backfill canonical-name embeddings so semantic dedup can compare against entities
that were created before it was enabled.

Semantic linking merges a new surface form with a known entity only when that entity
already carries a name embedding. Entities created before the feature have none, so this
embeds each one's canonical name once. Run it per group after turning the feature on;
it is idempotent, since an entity that already has an embedding is skipped.
"""

from __future__ import annotations

import asyncio
import sys

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.bootstrap import Container, build_container, dispose_container
from vera.config.settings import get_settings
from vera.observability import configure_logging, get_logger

log = get_logger(__name__)


async def backfill_group(container: Container, group_id: str) -> int:
    """Embed the canonical name of every entity in the group that lacks one. Returns the
    number of entities updated.
    """
    embedder = container.embedder
    if embedder is None:
        raise SystemExit(
            "no embedder configured: set memory.semantic_dedup_enabled and an embedder provider"
        )
    updated = 0
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group_id)
        pending = await uow.canonical.without_embeddings(group_id=group_id)
        for entity in pending:
            embedding = await embedder.embed(entity.canonical_name)
            await uow.canonical.set_embedding(entity_id=entity.id, embedding=embedding)
            updated += 1
        await uow.session.commit()
    log.info("backfill.done", group_id=group_id, updated=updated)
    return updated


async def _run(group_id: str) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    try:
        await backfill_group(container, group_id)
    finally:
        await dispose_container(container)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m vera.entrypoints.backfill_embeddings <group_id>")
    asyncio.run(_run(sys.argv[1]))


if __name__ == "__main__":
    main()
