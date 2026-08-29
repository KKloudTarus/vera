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
from vera.adapters.persistence.repositories.projection import SqlAlchemyProjectionSource
from vera.application.community import CommunityLineageService
from vera.application.projection import FactProjectionService
from vera.bootstrap import Container, build_container, dispose_container
from vera.config.settings import get_settings
from vera.observability import configure_logging, get_logger

log = get_logger(__name__)


async def _group_ids(container: Container) -> list[str]:
    async with container.reads() as session:
        rows = await session.scalars(text("SELECT DISTINCT group_id FROM facts"))
    return list(rows)


async def build_group(container: Container, group_id: str) -> int:
    if container.fact_projection is None:
        log.info("communities.built", group_id=group_id, communities=0)
        return 0
    # Rebuild first so clustering and summaries consume only PostgreSQL's active fact set.
    await FactProjectionService(
        source=SqlAlchemyProjectionSource(container.reads),
        projection=container.fact_projection,
    ).rebuild_group(group_id)
    report = await CommunityLineageService(
        memory=container.memory,
        lineage=SqlAlchemyCommunityLineageRepository(container.workers),
    ).build(group_id=group_id)
    log.info(
        "communities.built",
        group_id=group_id,
        communities=report.communities,
        lineage_rows=report.lineage_rows,
        derivation_run_id=str(report.derivation_run_id),
        projection_checkpoint=report.projection_checkpoint,
    )
    return report.communities


async def _run(target: str) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    try:
        groups = await _group_ids(container) if target == "--all" else [target]
        for group_id in groups:
            await build_group(container, group_id)
    finally:
        await dispose_container(container)


def main() -> None:
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        raise SystemExit("usage: python -m vera.entrypoints.build_communities <group_id|--all>")
    asyncio.run(_run(args[0]))


if __name__ == "__main__":
    main()
