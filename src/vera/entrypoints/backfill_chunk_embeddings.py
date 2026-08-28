"""Backfill dense embeddings for chunks so pgvector passage search has vectors to rank.

Requires the pgvector chunk embedding column (migration e2b3c4d5f6a7 on the pgvector image)
and a configured embedder. Embeds each chunk in the group that lacks an embedding, in batches,
and is idempotent: a chunk that already has one is skipped, so a partial run resumes. Run it
per group after enabling vector search; new ingests can be embedded by re-running it.

    python -m vera.entrypoints.backfill_chunk_embeddings <group_id>
"""

from __future__ import annotations

import asyncio
import sys

from sqlalchemy import text

from vera.adapters.persistence.repositories.pgvector_index import vector_literal
from vera.bootstrap import Container, build_container, dispose_container
from vera.config.settings import get_settings
from vera.observability import configure_logging, get_logger

log = get_logger(__name__)

_BATCH = 200
_PENDING = text(
    "SELECT id::text AS id, text FROM chunks "
    "WHERE group_id = :g AND embedding IS NULL ORDER BY created_at LIMIT :lim"
)
_SET = text("UPDATE chunks SET embedding = CAST(:v AS vector) WHERE id = CAST(:id AS uuid)")


async def backfill_group(container: Container, group_id: str) -> int:
    embedder = container.embedder
    if embedder is None:
        raise SystemExit("no embedder configured: set memory.embedder and its provider")
    updated = 0
    while True:
        async with container.workers() as session, session.begin():
            rows = (
                (await session.execute(_PENDING, {"g": group_id, "lim": _BATCH})).mappings().all()
            )
            for row in rows:
                vector = await embedder.embed(row["text"])
                await session.execute(_SET, {"v": vector_literal(vector), "id": row["id"]})
            updated += len(rows)
        if len(rows) < _BATCH:
            break
    log.info("chunk_embeddings.backfill.done", group_id=group_id, updated=updated)
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
        raise SystemExit("usage: python -m vera.entrypoints.backfill_chunk_embeddings <group_id>")
    asyncio.run(_run(sys.argv[1]))


if __name__ == "__main__":
    main()
