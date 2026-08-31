"""Backfill one model version into the multi-model fact embedding table.

python -m vera.entrypoints.backfill_fact_embeddings <group_id>
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from typing import cast

from sqlalchemy import text

from vera.adapters.persistence.repositories.pgvector_index import vector_literal
from vera.bootstrap import Container, build_container, dispose_container
from vera.config.settings import active_embedding, get_settings
from vera.domain.knowledge.fabric import fact_semantic_text
from vera.observability import configure_logging, get_logger

log = get_logger(__name__)

_BATCH = 200
_PENDING = text(
    "SELECT f.id::text AS id, cs.canonical_name AS subject_name, f.predicate, "
    "COALESCE(co.canonical_name, f.object_scalar, '') AS object_name, "
    "f.object_type, f.qualifiers, fe.content_hash, fe.active "
    "FROM facts f "
    "JOIN canonical_entities cs ON cs.id = f.subject_entity_id AND cs.group_id = f.group_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id AND co.group_id = f.group_id "
    "LEFT JOIN fact_embeddings fe ON fe.fact_id = f.id "
    "AND fe.provider = :provider AND fe.model = :model AND fe.model_version = :version "
    "AND fe.dimension = :dimension "
    "WHERE f.group_id = :g AND f.lifecycle_state IN ('active', 'disputed') "
    "ORDER BY f.created_at, f.id LIMIT :lim OFFSET :offset"
)
_SET = text(
    "INSERT INTO fact_embeddings ("
    "group_id, fact_id, provider, model, model_version, dimension, embedding, content_hash, active"
    ") VALUES ("
    ":g, CAST(:id AS uuid), :provider, :model, :version, :dimension, "
    "CAST(:embedding AS vector), :content_hash, true"
    ") ON CONFLICT (fact_id, provider, model, model_version, dimension) DO UPDATE SET "
    "embedding = EXCLUDED.embedding, content_hash = EXCLUDED.content_hash, "
    "created_at = now(), active = true"
)


async def backfill_group(container: Container, group_id: str) -> int:
    embedder = container.embedder
    if embedder is None:
        raise SystemExit("no embedder configured: set memory.embedder and its provider")
    memory = container.settings.memory
    model, dimension = active_embedding(container.settings)
    params: dict[str, object] = {
        "g": group_id,
        "provider": memory.embedder,
        "model": model,
        "version": memory.embedding_model_version,
        "dimension": dimension,
    }
    updated = 0
    offset = 0
    while True:
        async with container.workers() as session:
            await session.execute(
                text("SELECT set_config('vera.group_id', :g, true)"), {"g": group_id}
            )
            rows = (
                (await session.execute(_PENDING, params | {"lim": _BATCH, "offset": offset}))
                .mappings()
                .all()
            )
        pending: list[dict[str, object]] = []
        for row in rows:
            fact_text = fact_semantic_text(
                subject_name=str(row["subject_name"]),
                predicate=str(row["predicate"]),
                object_name=str(row["object_name"]),
                object_type=str(row["object_type"]),
                qualifiers=cast("dict[str, object]", row["qualifiers"] or {}),
            )
            content_hash = hashlib.sha256(fact_text.encode()).hexdigest()
            if row["content_hash"] == content_hash and bool(row["active"]):
                continue
            vector = await embedder.embed(fact_text)
            if len(vector) != dimension:
                raise ValueError(
                    f"embedding dimension mismatch: expected {dimension}, got {len(vector)}"
                )
            pending.append(
                {
                    "embedding": vector_literal(vector),
                    "id": row["id"],
                    "content_hash": content_hash,
                }
            )
        if pending:
            async with container.workers() as session, session.begin():
                await session.execute(
                    text("SELECT set_config('vera.group_id', :g, true)"), {"g": group_id}
                )
                for values in pending:
                    await session.execute(
                        _SET,
                        params | values,
                    )
            updated += len(pending)
        offset += len(rows)
        if len(rows) < _BATCH:
            break
    log.info("fact_embeddings.backfill.done", group_id=group_id, updated=updated)
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
        raise SystemExit("usage: python -m vera.entrypoints.backfill_fact_embeddings <group_id>")
    asyncio.run(_run(sys.argv[1]))


if __name__ == "__main__":
    main()
