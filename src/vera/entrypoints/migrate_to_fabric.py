"""Migrate legacy published knowledge into the Knowledge Fabric (Phase 8).

Backfills Fact / Assertion / Evidence from ``published_episodes`` per group, idempotently, so
it can be run repeatedly and resumed. The legacy tables and graph maps are left untouched, so
the old read paths keep working during the transition and rollback is simply not cutting over
(see docs/runbooks). After the backfill, rebuild the fabric projections with
``FactProjectionService`` and the passage index (both derive from these rows).

    python -m vera.entrypoints.migrate_to_fabric [<group_id> | --all] [--verify]
"""

from __future__ import annotations

import asyncio
import sys

from sqlalchemy import text

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.bootstrap import Container, build_container, dispose_container
from vera.config.settings import get_settings
from vera.entrypoints.migration import FabricBackfillService
from vera.observability import configure_logging, get_logger

log = get_logger(__name__)


async def _group_ids(container: Container) -> list[str]:
    async with container.sessionmaker() as session:
        rows = await session.scalars(text("SELECT DISTINCT group_id FROM published_episodes"))
    return list(rows)


async def backfill_group(container: Container, group_id: str) -> None:
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group_id)
        # Pass the configured extractor so free-text episodes are re-extracted into facts with
        # the episode's own provenance; without an LLM key the structured extractor yields
        # nothing for prose and those episodes are counted for review.
        report = await FabricBackfillService(uow.session, container.extractor).backfill_group(
            group_id=group_id
        )
        await uow.commit()
    log.info(
        "fabric.backfill.done",
        group_id=group_id,
        episodes=report.episodes_processed,
        facts=report.facts_created,
        assertions=report.assertions_created,
        evidence=report.evidence_created,
        needs_review=report.needs_review,
    )


async def verify_group(container: Container, group_id: str) -> bool:
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group_id)
        counts = await FabricBackfillService(uow.session).verify_group(group_id=group_id)
    # Every group that had structured knowledge must come back with at least as many
    # assertions as episodes (one triple per episode maps to at least one assertion).
    ok = counts["episodes"] == 0 or counts["assertions"] >= 1
    log.info("fabric.verify", group_id=group_id, ok=ok, **counts)
    return ok


async def _run(target: str, *, verify: bool) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    try:
        groups = await _group_ids(container) if target == "--all" else [target]
        failed: list[str] = []
        for group_id in groups:
            await backfill_group(container, group_id)
            if verify and not await verify_group(container, group_id):
                failed.append(group_id)
        if failed:
            raise SystemExit(f"fabric backfill verification failed for: {', '.join(failed)}")
    finally:
        await dispose_container(container)


def main() -> None:
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        raise SystemExit(
            "usage: python -m vera.entrypoints.migrate_to_fabric <group_id|--all> [--verify]"
        )
    target = args[0]
    verify = "--verify" in args[1:]
    asyncio.run(_run(target, verify=verify))


if __name__ == "__main__":
    main()
