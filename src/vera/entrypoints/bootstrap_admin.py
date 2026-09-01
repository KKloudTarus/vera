"""Seed the initial admin for a deployment with self-service signup closed.

A closed deployment (``VERA_API__REGISTRATION_OPEN=false``) has no way to mint the first
credential over HTTP, so this operator command creates one out of band: an admin principal
that owns a root workspace and holds a preassigned API key. That admin then provisions
everyone else through the admin endpoints (``POST /identity/users``).

    python -m vera.entrypoints.bootstrap_admin

It reads ``VERA_BOOTSTRAP__*`` from the environment and is a no-op unless
``VERA_BOOTSTRAP__ENABLED`` is set and ``VERA_BOOTSTRAP__ADMIN_API_KEY`` is provided (the
full ``vera_<prefix>.<secret>``, minted out of band and kept in the deployment secret; only
its hash is stored). The seed is idempotent, so it is safe to run on every deploy as a
post-install/post-upgrade Job.
"""

from __future__ import annotations

import asyncio

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.identity import IdentityService
from vera.bootstrap import build_container, dispose_container
from vera.config.settings import get_settings
from vera.observability import configure_logging, get_logger
from vera.shared.errors import Err

log = get_logger(__name__)


async def _run() -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    bs = settings.bootstrap

    if not bs.enabled:
        log.info("bootstrap.skipped", reason="disabled")
        return
    key = bs.admin_api_key.get_secret_value() if bs.admin_api_key is not None else ""
    if not key:
        log.warning("bootstrap.skipped", reason="no_admin_api_key")
        return

    container = build_container(settings)
    try:
        async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
            result = await IdentityService(uow).ensure_admin(
                admin_api_key=key,
                admin_email=bs.admin_email,
                admin_display_name=bs.admin_display_name,
                org_slug=bs.org_slug,
                org_name=bs.org_name,
                workspace_slug=bs.workspace_slug,
                workspace_name=bs.workspace_name,
            )
            if isinstance(result, Err):
                log.error("bootstrap.failed", error=result.error.message)
                raise SystemExit(1)
            await uow.commit()
            admin = result.value
    finally:
        await dispose_container(container)

    log.info(
        "bootstrap.admin.ready",
        principal_id=str(admin.principal_id),
        workspace_id=str(admin.workspace_id),
        created=admin.created,
    )
    # The workspace id is what an operator passes to POST /identity/users to provision more.
    print(
        f"principal_id={admin.principal_id} "
        f"workspace_id={admin.workspace_id} created={admin.created}"
    )


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
