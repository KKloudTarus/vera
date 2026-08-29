"""Backfill one model version into the multi-model chunk embedding table.

Existing embeddings from other models remain available. A partial run resumes by selecting only
chunks without the configured provider, model, and model version.

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
    "SELECT c.id::text AS id, c.text, c.content_hash FROM chunks c "
    "LEFT JOIN chunk_embeddings ce ON ce.chunk_id = c.id "
    "AND ce.provider = :provider AND ce.model = :model AND ce.model_version = :version "
    "WHERE c.group_id = :g AND ce.id IS NULL ORDER BY c.created_at LIMIT :lim"
)
_SET = text(
    "INSERT INTO chunk_embeddings ("
    "group_id, chunk_id, provider, model, model_version, dimension, embedding, content_hash, active"
    ") VALUES ("
    ":g, CAST(:id AS uuid), :provider, :model, :version, :dimension, "
    "CAST(:embedding AS vector), :content_hash, true"
    ") ON CONFLICT (chunk_id, provider, model, model_version) DO NOTHING"
)


async def backfill_group(container: Container, group_id: str) -> int:
    embedder = container.embedder
    if embedder is None:
        raise SystemExit("no embedder configured: set memory.embedder and its provider")
    memory = container.settings.memory
    params: dict[str, object] = {
        "g": group_id,
        "provider": memory.embedder,
        "model": memory.embedding_model,
        "version": memory.embedding_model_version,
        "dimension": memory.embedding_dim,
    }
    updated = 0
    while True:
        async with container.workers() as session, session.begin():
            await session.execute(
                text("SELECT set_config('vera.group_id', :g, true)"), {"g": group_id}
            )
            rows = (await session.execute(_PENDING, params | {"lim": _BATCH})).mappings().all()
            for row in rows:
                vector = await embedder.embed(row["text"])
                if len(vector) != memory.embedding_dim:
                    raise ValueError(
                        "embedding dimension mismatch: "
                        f"expected {memory.embedding_dim}, got {len(vector)}"
                    )
                await session.execute(
                    _SET,
                    params
                    | {
                        "embedding": vector_literal(vector),
                        "id": row["id"],
                        "content_hash": row["content_hash"],
                    },
                )
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
