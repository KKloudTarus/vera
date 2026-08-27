"""App lifespan: build the container on startup, dispose it on shutdown."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator

from fastapi import FastAPI

from vera.bootstrap import (
    Container,
    build_container,
    dispose_container,
    refresh_rerank_weights,
)
from vera.config.settings import get_settings
from vera.observability import (
    configure_logging,
    configure_tracing,
    get_logger,
    instrument_fastapi,
)

log = get_logger(__name__)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    configure_tracing(settings)
    container: Container = build_container(settings)
    # Adopt feedback-calibrated rerank weights, if calibration has persisted any.
    with contextlib.suppress(Exception):
        await refresh_rerank_weights(container)
    app.state.container = container
    instrument_fastapi(app, container.engine)
    log.info("api.startup", environment=settings.environment, service=settings.service_name)
    try:
        yield
    finally:
        await dispose_container(container)
        log.info("api.shutdown")
