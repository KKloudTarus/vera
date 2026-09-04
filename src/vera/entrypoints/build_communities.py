"""Build graph communities and their summaries for a group (or all groups).

Communities are an LLM-derived, rebuildable projection over the graph, so this is an
operator-run step, not part of live ingestion: it costs LLM calls and is safe to re-run
(a rebuild reconstructs them). Requires a real graph engine and a configured LLM; with the
null engine it reports zero and does nothing.

    python -m vera.entrypoints.build_communities <group_id> | --all
"""

from __future__ import annotations

import asyncio
import sys

from sqlalchemy import text

from vera.adapters.persistence.repositories.community import (
    SqlAlchemyCommunityLineageRepository,
)
from vera.adapters.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from vera.adapters.persistence.repositories.projection import SqlAlchemyProjectionSource
from vera.application.community import CommunityLineageService
from vera.application.projection import FactProjectionService
from vera.bootstrap import Container, build_container, dispose_container
from vera.config.settings import get_settings
from vera.observability import configure_logging, get_logger
from vera.observability.metrics import record_community_build
from vera.shared.ids import uuid7

log = get_logger(__name__)
_GROUP_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended(:g, 0))")
_BUILD_PENDING = text(
    "SELECT 1 FROM ingestion_jobs WHERE group_id = :g "
    "AND status IN ('pending','inflight') "
    "AND payload->>'job_kind' = 'build_communities' LIMIT 1"
)


async def _group_ids(container: Container) -> list[str]:
    async with container.reads() as session:
        rows = await session.scalars(text("SELECT DISTINCT group_id FROM facts"))
    return list(rows)


async def build_group(container: Container, group_id: str) -> int:
    if container.fact_projection is None:
        log.info("communities.built", group_id=group_id, communities=0)
        return 0
    async with container.workers() as lock_session, lock_session.begin():
        await lock_session.execute(_GROUP_LOCK, {"g": group_id})
        await FactProjectionService(
            source=SqlAlchemyProjectionSource(container.reads),
            projection=container.fact_projection,
        ).project_group(group_id)
        report = await CommunityLineageService(
            memory=container.memory,
            lineage=SqlAlchemyCommunityLineageRepository(container.workers),
        ).build(group_id=group_id)
    record_community_build()
    log.info(
        "communities.built",
        group_id=group_id,
        communities=report.communities,
        lineage_rows=report.lineage_rows,
        derivation_run_id=str(report.derivation_run_id),
        projection_checkpoint=report.projection_checkpoint,
    )
    return report.communities


async def build_all(container: Container, *, group_id: str | None = None) -> int:
    groups = [group_id] if group_id else await _group_ids(container)
    return sum([await build_group(container, value) for value in groups])


async def schedule_builds(container: Container, *, group_id: str | None = None) -> int:
    groups = [group_id] if group_id else await _group_ids(container)
    scheduled = 0
    for value in groups:
        async with container.workers() as session, session.begin():
            if await session.scalar(_BUILD_PENDING, {"g": value}):
                continue
            await SqlAlchemyOutboxRepository(session).add(
                group_id=value,
                source_id=f"community:{value}",
                dedup_uuid=uuid7(),
                payload={"job_kind": "build_communities", "group_id": value},
            )
            scheduled += 1
    return scheduled


async def _run(target: str) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    try:
        await build_all(container, group_id=None if target == "--all" else target)
    finally:
        await dispose_container(container)


def main() -> None:
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        raise SystemExit("usage: python -m vera.entrypoints.build_communities <group_id|--all>")
    asyncio.run(_run(args[0]))


if __name__ == "__main__":
    main()
