"""FastAPI application factory.

Wires: lifespan (container), CORS, a correlation-id + structlog-binding middleware,
RFC 9457 problem+json error handling, and the routers.
"""

from __future__ import annotations

import os
import uuid

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from vera import __version__
from vera.config.settings import get_settings
from vera.entrypoints.api.lifespan import lifespan
from vera.entrypoints.api.routers import health, identity, knowledge, memory
from vera.entrypoints.evaluation_budget import EvaluationBudgetMiddleware
from vera.observability import bind_log_context, clear_log_context, get_logger
from vera.observability.metrics import record_write_failure
from vera.shared.errors import VeraError

log = get_logger(__name__)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Attach a correlation id to every request and bind it to the log context."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        correlation_id = request.headers.get("X-Correlation-ID") or uuid.uuid4().hex
        clear_log_context()
        bind_log_context(correlation_id=correlation_id, path=request.url.path)
        try:
            response = await call_next(request)
        finally:
            clear_log_context()
        response.headers["X-Correlation-ID"] = correlation_id
        return response


class WriteFailureMiddleware(BaseHTTPMiddleware):
    """Count failed requests on the API's mutating identity and deletion surfaces."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        mutating = request.method in {"POST", "PUT", "PATCH", "DELETE"} and (
            request.url.path.startswith("/identity")
            or (request.method == "DELETE" and request.url.path.startswith("/memory/sources/"))
        )
        try:
            response = await call_next(request)
        except Exception:
            if mutating:
                record_write_failure()
            raise
        if mutating and response.status_code >= 500:
            record_write_failure()
        return response


def _problem(status_code: int, title: str, detail: str) -> JSONResponse:
    """RFC 9457 problem+json body."""
    return JSONResponse(
        status_code=status_code,
        media_type="application/problem+json",
        content={"type": "about:blank", "title": title, "status": status_code, "detail": detail},
    )


async def _on_infra_error(request: Request, exc: Exception) -> JSONResponse:
    log.error("infrastructure_error", error=str(exc), error_type=type(exc).__name__)
    return _problem(503, "Service dependency unavailable", str(exc))


async def _on_unhandled_error(_request: Request, exc: Exception) -> JSONResponse:
    log.error("unhandled_error", error_type=type(exc).__name__)
    return _problem(500, "Internal server error", "The request could not be completed")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="VERA",
        version=__version__,
        summary="Verified Episodic Recall for Agents",
        lifespan=lifespan,
    )

    if settings.api.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.api.cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    app.add_middleware(WriteFailureMiddleware)
    app.add_middleware(CorrelationIdMiddleware)
    evaluation_scope = os.environ.get("VERA_EVAL_SCOPE_ID")
    if evaluation_scope and settings.environment != "prod":
        app.add_middleware(EvaluationBudgetMiddleware, scope_id=evaluation_scope)
    app.add_exception_handler(VeraError, _on_infra_error)
    app.add_exception_handler(Exception, _on_unhandled_error)

    if settings.observability.metrics_enabled:
        _mount_metrics(app)

    app.include_router(health.router)
    app.include_router(identity.router)
    app.include_router(memory.router)
    app.include_router(knowledge.router)
    return app


def _mount_metrics(app: FastAPI) -> None:
    """Add RED request metrics and a /metrics endpoint over the default registry."""
    from prometheus_fastapi_instrumentator import Instrumentator

    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)


app = create_app()
