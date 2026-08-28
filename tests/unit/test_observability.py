"""Cost math, metric recording, and manual tracing spans."""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from vera.observability import span
from vera.observability.cost import (
    UsageContext,
    build_usage_event,
    cost_usd,
    estimate_tokens,
    reset_usage_context,
    set_usage_context,
)
from vera.observability.metrics import (
    record_ingestion,
    record_llm_usage,
    record_search,
    render_latest,
)

# Install an in-memory span exporter once so `span()` has somewhere to record.
_span_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_span_exporter))
trace.set_tracer_provider(_provider)


def test_cost_uses_the_price_table() -> None:
    # gpt-4.1-mini: $0.40 / MTok prompt, $1.60 / MTok completion.
    assert cost_usd("gpt-4.1-mini", 1_000_000, 0) == 0.40
    assert cost_usd("gpt-4.1-mini", 0, 1_000_000) == 1.60
    assert cost_usd("text-embedding-3-small", 1_000_000, 0) == 0.02


def test_cost_is_priced_across_providers() -> None:
    # Cost tracking is provider-neutral: Voyage models are priced too, not just OpenAI.
    assert cost_usd("voyage-3.5", 1_000_000, 0) == 0.06
    assert cost_usd("rerank-2.5", 1_000_000, 0) == 0.05


def test_unknown_model_is_metered_but_not_priced() -> None:
    assert cost_usd("some-future-model", 5000, 5000) == 0.0


def test_estimate_tokens_is_roughly_chars_over_four() -> None:
    assert estimate_tokens("") == 1
    assert estimate_tokens("abcd") == 1
    assert estimate_tokens("a" * 400) == 100


def test_usage_event_picks_up_the_current_context() -> None:
    token = set_usage_context(UsageContext(request_kind="ingest", group_id="p:1", ref="src:1"))
    try:
        event = build_usage_event(
            model="text-embedding-3-small",
            operation="embedding",
            prompt_tokens=100,
            completion_tokens=0,
        )
    finally:
        reset_usage_context(token)
    assert event.request_kind == "ingest"
    assert event.group_id == "p:1"
    assert event.ref == "src:1"
    assert event.cost_usd > 0


def test_usage_event_without_context_is_unknown() -> None:
    event = build_usage_event(
        model="gpt-4.1-mini", operation="llm", prompt_tokens=10, completion_tokens=5
    )
    assert event.request_kind == "unknown"
    assert event.group_id is None


def test_metrics_are_recorded_in_the_registry() -> None:
    record_ingestion(result="done", duration_s=0.5)
    record_search(duration_s=0.2, hits=3)
    record_llm_usage(
        model="text-embedding-3-small",
        operation="embedding",
        prompt_tokens=100,
        completion_tokens=0,
        cost_usd=0.000002,
    )
    body, content_type = render_latest()
    text = body.decode()
    assert "text/plain" in content_type
    assert "vera_ingestion_jobs_total" in text
    assert "vera_search_duration_seconds" in text
    assert "vera_llm_tokens_total" in text


def test_span_is_recorded_with_attributes() -> None:
    _span_exporter.clear()
    with span("memory.rerank", candidates=3) as current:
        assert current is not None
    names = [s.name for s in _span_exporter.get_finished_spans()]
    assert "memory.rerank" in names
