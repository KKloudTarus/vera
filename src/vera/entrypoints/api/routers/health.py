"""Health & readiness probes.

``/health/live`` never touches dependencies (liveness). ``/health/ready`` checks
the source-of-truth DB and the memory engine (readiness); it returns 503 if a
critical dependency is down so orchestrators route traffic away.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response, status
from sqlalchemy import text

from vera import __version__
from vera.entrypoints.api.deps import ContainerDep

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", summary="Liveness probe")
async def live() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@router.get("/ready", summary="Readiness probe")
async def ready(container: ContainerDep, response: Response) -> dict[str, Any]:
    checks: dict[str, str] = {}

    try:
        async with container.sessionmaker() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception:
        checks["database"] = "down"

    try:
        checks["memory"] = "ok" if await container.memory.health() else "down"
    except Exception:
        checks["memory"] = "down"

    ok = all(v == "ok" for v in checks.values())
    if not ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if ok else "degraded", "checks": checks}
