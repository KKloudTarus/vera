"""Health & readiness probes.

``/health/live`` never touches dependencies (liveness). ``/health/ready`` checks
the source-of-truth DB and the memory engine (readiness); it returns 503 if a
critical dependency is down so orchestrators route traffic away.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Response, status
from sqlalchemy import text

from vera import __version__
from vera.build_metadata import BuildMetadataError, load_build_metadata
from vera.entrypoints.api.deps import ContainerDep

router = APIRouter(prefix="/health", tags=["health"])
BUILD_METADATA_PATH = Path("/app/build-metadata.json")


@router.get("/live", summary="Liveness probe")
async def live() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@router.get("/build", summary="Build provenance")
def build(response: Response) -> dict[str, str | bool]:
    try:
        metadata = load_build_metadata(BUILD_METADATA_PATH)
    except BuildMetadataError:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "unavailable",
            "service_version": __version__,
            "git_sha": "unknown",
            "git_dirty": True,
        }
    return {"status": "ok", "service_version": __version__, **metadata.as_dict()}


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
