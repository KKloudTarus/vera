"""Distributed tracing with OpenTelemetry.

Auto-instruments the request path (FastAPI, SQLAlchemy, asyncpg, httpx) and offers a
``span`` helper for the manual spans that matter: the add_episode ingestion stages and
the rerank. Spans export only when an OTLP endpoint is configured; otherwise they run
against a real provider with no exporter, so the code path is exercised in tests and
locally at negligible cost. Instrumentation is global and installed once per process.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from vera.config.settings import Settings

_configured = False
_instrumented = False


def configure_tracing(settings: Settings) -> None:
    """Install the tracer provider once. Safe to call from every process start."""
    global _configured
    if _configured or not settings.observability.tracing_enabled:
        return
    resource = Resource.create(
        {
            "service.name": settings.service_name,
            "deployment.environment": settings.environment,
        }
    )
    provider = TracerProvider(resource=resource)
    endpoint = settings.observability.otlp_endpoint
    if endpoint:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    _configured = True


def instrument_fastapi(app: Any, engine: Any) -> None:
    """Auto-instrument the API process. Instrumentors are global, so guard re-entry."""
    global _instrumented
    if _instrumented:
        return
    from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    FastAPIInstrumentor.instrument_app(app)
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
    AsyncPGInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()
    _instrumented = True


def instrument_worker() -> None:
    """Auto-instrument the worker process (DB and outbound HTTP)."""
    global _instrumented
    if _instrumented:
        return
    from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    AsyncPGInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()
    _instrumented = True


@contextmanager
def span(name: str, **attributes: Any) -> Generator[trace.Span, None, None]:
    """Open a manual span. A no-op provider makes this cheap when tracing is off."""
    tracer = trace.get_tracer("vera")
    with tracer.start_as_current_span(name) as current:
        for key, value in attributes.items():
            if value is not None:
                current.set_attribute(key, value)
        yield current
