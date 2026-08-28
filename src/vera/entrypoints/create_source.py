"""Create a knowledge source, the row a connector ingests into.

A connector spec references a ``source_id``: the id of a ``knowledge_sources`` row that
carries the source's kind and trust tier. There is no HTTP endpoint for this on purpose
(creating a source is an operator action, not an agent action), so this command creates one
and prints its id for use in ``VERA_CONNECTORS__SPECS``.

    python -m vera.entrypoints.create_source \
        --group p:<project> --workspace <workspace-uuid> \
        --kind filesystem --name "Team docs" --tier 1

The workspace id and the project's group_id come from the identity API (create a workspace
and a project first). Trust tier drives publishing: 1-2 auto-publish, 3 needs review, 4 is
proposal-only. See docs/loading-knowledge.md.
"""

from __future__ import annotations

import argparse
import asyncio
from uuid import UUID

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.bootstrap import build_container, dispose_container
from vera.config.settings import get_settings
from vera.domain.knowledge.models import SourceKind
from vera.observability import configure_logging, get_logger

log = get_logger(__name__)


async def _run(args: argparse.Namespace) -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    try:
        async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
            await uow.use_tenant(args.group)
            source_id = await uow.sources.create(
                workspace_id=UUID(args.workspace),
                project_id=UUID(args.project) if args.project else None,
                kind=SourceKind(args.kind).value,
                name=args.name,
                trust_tier=args.tier,
            )
            await uow.commit()
    finally:
        await dispose_container(container)
    log.info("source.created", source_id=str(source_id), kind=args.kind, tier=args.tier)
    print(source_id)  # the id to put in VERA_CONNECTORS__SPECS


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a VERA knowledge source.")
    parser.add_argument("--group", required=True, help="project group_id, e.g. p:<uuid>")
    parser.add_argument("--workspace", required=True, help="workspace id (uuid)")
    parser.add_argument("--project", default=None, help="project id (uuid), optional")
    parser.add_argument("--kind", required=True, choices=[k.value for k in SourceKind])
    parser.add_argument("--name", required=True, help="human-readable source name")
    parser.add_argument(
        "--tier", type=int, default=2, choices=[1, 2, 3, 4], help="trust tier (default 2)"
    )
    asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    main()
