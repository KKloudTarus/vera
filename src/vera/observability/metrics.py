"""Prometheus metrics for VERA.

RED for the request path and USE for the queue, plus LLM call and token counters, all
with bounded label sets so cardinality stays flat: results are closed enums, models
come from the fixed configured set, and no group_id, source_id, or free text is ever a
label. Metric objects are module-level singletons in the default registry; each process
exposes them (the API through its /metrics route, the worker through its own server).
"""

from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

_LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)

ingestion_jobs = Counter(
    "vera_ingestion_jobs_total",
    "Ingestion jobs processed, by outcome.",
    labelnames=("result",),
)
ingestion_duration = Histogram(
    "vera_ingestion_duration_seconds",
    "Wall-clock time to ingest one job into the graph.",
    buckets=_LATENCY_BUCKETS,
)
queue_depth = Gauge(
    "vera_queue_depth",
    "Ingestion jobs currently in each state.",
    labelnames=("status",),
)
search_duration = Histogram(
    "vera_search_duration_seconds",
    "Wall-clock time for one memory search (retrieve plus rerank).",
    buckets=_LATENCY_BUCKETS,
)
search_hits = Histogram(
    "vera_search_hits",
    "Number of ranked hits returned by a search.",
    buckets=(0, 1, 2, 5, 10, 20, 50, 100),
)
llm_calls = Counter(
    "vera_llm_calls_total",
    "Calls to an LLM or embedding provider, by model and operation.",
    labelnames=("model", "operation"),
)
llm_tokens = Counter(
    "vera_llm_tokens_total",
    "Tokens consumed at a provider, by model, operation, and token type.",
    labelnames=("model", "operation", "token_type"),
)
llm_cost_usd = Counter(
    "vera_llm_cost_usd_total",
    "Estimated provider cost in USD, by model and operation.",
    labelnames=("model", "operation"),
)
queue_backpressure = Counter(
    "vera_queue_backpressure_events_total",
    "Times the pending backlog crossed the configured alert threshold.",
)


def record_ingestion(*, result: str, duration_s: float) -> None:
    ingestion_jobs.labels(result=result).inc()
    if result == "done":
        ingestion_duration.observe(duration_s)


def set_queue_depth(depths: dict[str, int]) -> None:
    for status in ("pending", "inflight", "dead"):
        queue_depth.labels(status=status).set(depths.get(status, 0))


def note_backpressure(depths: dict[str, int], threshold: int) -> bool:
    """Return whether the pending backlog is over the threshold, counting each crossing so
    an alert can fire. A threshold of zero disables the check.
    """
    if threshold <= 0:
        return False
    over = depths.get("pending", 0) > threshold
    if over:
        queue_backpressure.inc()
    return over


def record_search(*, duration_s: float, hits: int) -> None:
    search_duration.observe(duration_s)
    search_hits.observe(hits)


def record_llm_usage(
    *,
    model: str,
    operation: str,
    prompt_tokens: int,
    completion_tokens: int,
    cost_usd: float,
) -> None:
    llm_calls.labels(model=model, operation=operation).inc()
    if prompt_tokens:
        llm_tokens.labels(
            model=model,
            operation=operation,
            token_type="prompt",  # noqa: S106  metric label, not a secret
        ).inc(prompt_tokens)
    if completion_tokens:
        llm_tokens.labels(
            model=model,
            operation=operation,
            token_type="completion",  # noqa: S106  metric label, not a secret
        ).inc(completion_tokens)
    if cost_usd:
        llm_cost_usd.labels(model=model, operation=operation).inc(cost_usd)


def render_latest() -> tuple[bytes, str]:
    """Serialize the default registry for a /metrics response."""
    return generate_latest(), CONTENT_TYPE_LATEST


def start_metrics_server(port: int) -> None:
    """Expose /metrics on its own HTTP server (used by the worker process)."""
    from prometheus_client import start_http_server

    start_http_server(port)
