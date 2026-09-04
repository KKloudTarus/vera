"""Cost math, metric recording, and manual tracing spans."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from mcp.server.mcpserver import MCPServer
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from starlette.types import Receive, Scope, Send

from vera.adapters.resilience.quota import InProcessQuota
from vera.config.settings import get_settings
from vera.entrypoints.evaluation_budget import EvaluationBudgetMiddleware
from vera.entrypoints.mcp import main as mcp_main
from vera.entrypoints.mcp.guard import Guard
from vera.entrypoints.mcp.policy import ToolClass
from vera.observability import span
from vera.observability.cost import (
    ProviderBudgetContext,
    ProviderBudgetExceededError,
    UsageContext,
    UsageEvent,
    UsagePersistenceError,
    build_usage_event,
    cost_usd,
    current_provider_budget_context,
    estimate_tokens,
    guard_provider_call,
    model_price_known,
    provider_budget_trace_context,
    provider_reported_cost,
    reset_provider_budget_context,
    reset_usage_context,
    set_provider_budget_context,
    set_usage_context,
    settle_provider_call,
)
from vera.observability.metrics import (
    record_ingestion,
    record_llm_usage,
    record_search,
    record_time_to_searchable,
    render_latest,
)

# Install an in-memory span exporter once so `span()` has somewhere to record.
_span_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_span_exporter))
trace.set_tracer_provider(_provider)
ROOT = Path(__file__).parents[2]


def test_cost_uses_the_price_table() -> None:
    # gpt-4.1-mini: $0.40 / MTok prompt, $1.60 / MTok completion.
    assert cost_usd("gpt-4.1-mini", 1_000_000, 0) == 0.40
    assert cost_usd("gpt-4.1-mini", 0, 1_000_000) == 1.60
    assert cost_usd("text-embedding-3-small", 1_000_000, 0) == 0.02


def test_cost_is_priced_across_providers() -> None:
    # Cost tracking is provider-neutral: Voyage models are priced too, not just OpenAI.
    assert cost_usd("voyage-3.5", 1_000_000, 0) == 0.06
    assert cost_usd("rerank-2.5", 1_000_000, 0) == 0.05


def test_routed_evaluation_models_have_conservative_cost_ceilings() -> None:
    assert cost_usd("codex/gpt-5.6-sol", 1_000_000, 1_000_000) == 200.0
    assert cost_usd("codex/gpt-5.6-terra", 1_000_000, 1_000_000) == 200.0
    assert model_price_known("codex/gpt-5.6-sol") is True
    assert model_price_known("codex/gpt-5.6-terra") is True


@pytest.mark.parametrize(
    "config_name",
    [
        "run.local.json",
        "run.nightly.local.json",
        "run.weekly.local.json",
        "run.release.local.json",
    ],
)
def test_configured_evaluation_provider_models_have_cost_ceilings(config_name: str) -> None:
    config = json.loads((ROOT / "evals" / config_name).read_text(encoding="utf-8"))

    for role in ("candidate", "extractor", "contradiction_judge", "entity_judge"):
        assert model_price_known(config["models"][role]), role


def test_unknown_model_is_metered_but_not_priced() -> None:
    assert cost_usd("some-future-model", 5000, 5000) == 0.0
    assert model_price_known("some-future-model") is False
    assert model_price_known("gpt-4.1-mini") is True


def test_exact_provider_cost_overrides_the_local_price_table() -> None:
    exact = provider_reported_cost({"usage": {}, "cost_usd": 0.125})
    event = build_usage_event(
        model="some-future-model",
        operation="llm",
        prompt_tokens=5000,
        completion_tokens=5000,
        exact_cost_usd=exact,
    )

    assert exact == 0.125
    assert event.cost_usd == 0.125
    assert event.cost_complete is True


def test_exact_provider_cost_can_come_from_usage() -> None:
    assert provider_reported_cost({"usage": {"cost_usd": 0.25}}) == 0.25


def test_ambiguous_provider_cost_unit_is_not_accepted_as_usd() -> None:
    assert provider_reported_cost({"usage": {"cost": 0.25}}) is None


def test_unknown_model_without_exact_provider_cost_is_incomplete() -> None:
    event = build_usage_event(
        model="some-future-model",
        operation="llm",
        prompt_tokens=5000,
        completion_tokens=5000,
    )

    assert event.cost_usd == 0.0
    assert event.cost_complete is False


def test_known_model_requires_explicitly_complete_provider_usage() -> None:
    incomplete = build_usage_event(
        model="voyage-4-lite", operation="embedding", prompt_tokens=5000, completion_tokens=0
    )
    complete = build_usage_event(
        model="voyage-4-lite",
        operation="embedding",
        prompt_tokens=5000,
        completion_tokens=0,
        usage_complete=True,
    )

    assert incomplete.cost_complete is False
    assert complete.cost_complete is True


@pytest.mark.parametrize(
    ("prompt_tokens", "completion_tokens"),
    [(-1, 0), (0, -1), (True, 0), (1.5, 0)],
)
def test_usage_event_rejects_invalid_token_counts(
    prompt_tokens: int, completion_tokens: int
) -> None:
    with pytest.raises(ValueError, match="token counts"):
        build_usage_event(
            model="gpt-4.1-mini",
            operation="llm",
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )


@pytest.mark.asyncio
async def test_provider_failure_persists_an_incomplete_usage_marker() -> None:
    events: list[UsageEvent] = []

    class Sink:
        async def record(self, event: UsageEvent) -> None:
            events.append(event)

    async def fail() -> None:
        raise TimeoutError("ambiguous provider result")

    with pytest.raises(TimeoutError, match="ambiguous"):
        await guard_provider_call(fail, Sink(), model="future-model", operation="llm")

    assert len(events) == 1
    assert events[0].cost_complete is False


@pytest.mark.asyncio
async def test_failed_ingest_fence_prevents_the_provider_call() -> None:
    provider_calls = 0
    settlements: list[tuple[str, float, float]] = []

    class Sink:
        async def reserve_provider_budget(self, _action_key: str, _max_cost_usd: float) -> bool:
            return True

        async def settle_provider_budget(
            self, action_key: str, reserved_cost_usd: float, actual_cost_usd: float
        ) -> bool:
            settlements.append((action_key, reserved_cost_usd, actual_cost_usd))
            return True

        async def fence_provider_attempt(self, _job_id: str, _claim_token: str) -> None:
            raise RuntimeError("database unavailable")

        async def record(self, _event: UsageEvent) -> None:
            raise AssertionError("no usage event is expected before the provider call")

    async def provider() -> None:
        nonlocal provider_calls
        provider_calls += 1

    token = set_usage_context(
        UsageContext(
            request_kind="ingest",
            group_id="p:1",
            ref="src:1",
            job_id="job-1",
            claim_token="claim-1",  # noqa: S106  queue lease token, not a credential
        )
    )
    budget_token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        with pytest.raises(UsagePersistenceError, match="attempt fence"):
            await guard_provider_call(
                provider,
                Sink(),  # type: ignore[arg-type]
                model="gpt-4.1-mini",
                operation="llm",
                prompt_token_limit=1000,
                completion_token_limit=1000,
            )
    finally:
        reset_provider_budget_context(budget_token)
        reset_usage_context(token)

    assert provider_calls == 0
    assert len(settlements) == 1
    assert settlements[0][0] == "run:case:step"
    assert settlements[0][2] == 0.0


@pytest.mark.asyncio
async def test_cancelled_ingest_fence_releases_the_pre_dispatch_reservation() -> None:
    fence_started = asyncio.Event()
    settlements: list[tuple[str, float, float]] = []

    class Sink:
        async def reserve_provider_budget(self, _action_key: str, _max_cost_usd: float) -> bool:
            return True

        async def settle_provider_budget(
            self, action_key: str, reserved_cost_usd: float, actual_cost_usd: float
        ) -> bool:
            settlements.append((action_key, reserved_cost_usd, actual_cost_usd))
            return True

        async def fence_provider_attempt(self, _job_id: str, _claim_token: str) -> None:
            fence_started.set()
            await asyncio.Event().wait()

    async def provider() -> None:
        raise AssertionError("provider must not be called before the fence commits")

    usage_token = set_usage_context(
        UsageContext(
            request_kind="ingest",
            job_id="job-1",
            claim_token="claim-1",  # noqa: S106  queue lease token, not a credential
        )
    )
    budget_token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        guarded = asyncio.create_task(
            guard_provider_call(
                provider,
                Sink(),  # type: ignore[arg-type]
                model="gpt-4.1-mini",
                operation="llm",
                prompt_token_limit=1000,
                completion_token_limit=1000,
            )
        )
        await fence_started.wait()
        guarded.cancel()
        with pytest.raises(asyncio.CancelledError):
            await guarded
    finally:
        reset_provider_budget_context(budget_token)
        reset_usage_context(usage_token)

    assert len(settlements) == 1
    assert settlements[0][0] == "run:case:step"
    assert settlements[0][2] == 0.0


@pytest.mark.asyncio
async def test_fence_error_finishes_refund_when_cancellation_arrives_during_settlement() -> None:
    settlement_started = asyncio.Event()
    release_settlement = asyncio.Event()
    settled = False

    class Sink:
        async def reserve_provider_budget(self, _action_key: str, _max_cost_usd: float) -> bool:
            return True

        async def settle_provider_budget(
            self, _action_key: str, _reserved_cost_usd: float, actual_cost_usd: float
        ) -> bool:
            nonlocal settled
            assert actual_cost_usd == 0.0
            settlement_started.set()
            await release_settlement.wait()
            settled = True
            return True

        async def fence_provider_attempt(self, _job_id: str, _claim_token: str) -> None:
            raise RuntimeError("database unavailable")

    async def provider() -> None:
        raise AssertionError("provider must not be called before the fence commits")

    usage_token = set_usage_context(
        UsageContext(
            request_kind="ingest",
            job_id="job-1",
            claim_token="claim-1",  # noqa: S106  queue lease token, not a credential
        )
    )
    budget_token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        guarded = asyncio.create_task(
            guard_provider_call(
                provider,
                Sink(),  # type: ignore[arg-type]
                model="gpt-4.1-mini",
                operation="llm",
                prompt_token_limit=1000,
                completion_token_limit=1000,
            )
        )
        await settlement_started.wait()
        guarded.cancel()
        await asyncio.sleep(0)
        assert not guarded.done()
        release_settlement.set()
        with pytest.raises(asyncio.CancelledError):
            await guarded
    finally:
        release_settlement.set()
        reset_provider_budget_context(budget_token)
        reset_usage_context(usage_token)

    assert settled is True


@pytest.mark.asyncio
async def test_evaluation_budget_middleware_requires_the_matching_scope() -> None:
    observed: list[str | None] = []

    async def asgi_app(_scope: Scope, _receive: Receive, send: Send) -> None:
        budget = current_provider_budget_context()
        observed.append(budget.action_key if budget is not None else None)
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = EvaluationBudgetMiddleware(asgi_app, scope_id="owned-scope")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware), base_url="http://test"
    ) as client:
        accepted = await client.get(
            "/", headers={"X-Vera-Eval-Scope": "owned-scope", "X-Vera-Provider-Budget": "key"}
        )
        rejected = await client.get(
            "/", headers={"X-Vera-Eval-Scope": "wrong-scope", "X-Vera-Provider-Budget": "key"}
        )

    assert accepted.status_code == 204
    assert rejected.status_code == 204
    assert observed == ["key", None]
    assert current_provider_budget_context() is None


@pytest.mark.asyncio
async def test_provider_budget_rejection_prevents_dispatch() -> None:
    provider_calls = 0

    class Sink:
        async def reserve_provider_budget(self, action_key: str, max_cost_usd: float) -> bool:
            assert action_key == "run:case:step"
            assert max_cost_usd > 0
            return False

    async def provider() -> None:
        nonlocal provider_calls
        provider_calls += 1

    token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        with pytest.raises(ProviderBudgetExceededError, match="cannot fit action budget"):
            await guard_provider_call(
                provider,
                Sink(),  # type: ignore[arg-type]
                model="gpt-4.1-mini",
                operation="llm",
                prompt_token_limit=1000,
                completion_token_limit=1000,
            )
    finally:
        reset_provider_budget_context(token)

    assert provider_calls == 0


@pytest.mark.asyncio
async def test_provider_budget_requires_a_priced_model_and_token_ceiling() -> None:
    class Sink:
        async def reserve_provider_budget(self, _action_key: str, _max_cost_usd: float) -> bool:
            raise AssertionError("invalid calls must fail before the durable reservation")

    async def provider() -> None:
        raise AssertionError("invalid calls must fail before provider dispatch")

    token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        with pytest.raises(ProviderBudgetExceededError, match="omitted its token ceiling"):
            await guard_provider_call(
                provider,
                Sink(),
                model="gpt-4.1-mini",
                operation="llm",  # type: ignore[arg-type]
            )
        with pytest.raises(ProviderBudgetExceededError, match="has no price ceiling"):
            await guard_provider_call(
                provider,
                Sink(),  # type: ignore[arg-type]
                model="unpriced-model",
                operation="llm",
                prompt_token_limit=1000,
                completion_token_limit=1000,
            )
    finally:
        reset_provider_budget_context(token)


@pytest.mark.asyncio
async def test_provider_budget_settlement_replaces_the_maximum_with_actual_cost() -> None:
    settlements: list[tuple[str, float, float]] = []

    class Sink:
        async def settle_provider_budget(
            self, action_key: str, reserved_cost_usd: float, actual_cost_usd: float
        ) -> bool:
            settlements.append((action_key, reserved_cost_usd, actual_cost_usd))
            return True

    token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        await settle_provider_call(
            Sink(),  # type: ignore[arg-type]
            reserved_cost_usd=0.25,
            actual_cost_usd=0.05,
        )
    finally:
        reset_provider_budget_context(token)

    assert settlements == [("run:case:step", 0.25, 0.05)]


def test_provider_budget_trace_rejects_an_untrusted_explicit_key() -> None:
    assert provider_budget_trace_context(
        {"correlation_id": "corr-1", "_provider_budget_key": "spoofed"}
    ) == {"correlation_id": "corr-1"}


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
    record_time_to_searchable(1200)
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
    assert "vera_time_to_searchable_seconds" in text
    assert "vera_llm_tokens_total" in text


def test_span_is_recorded_with_attributes() -> None:
    _span_exporter.clear()
    with span("memory.rerank", candidates=3) as current:
        assert current is not None
    names = [s.name for s in _span_exporter.get_finished_spans()]
    assert "memory.rerank" in names


@pytest.mark.asyncio
async def test_mcp_telemetry_excludes_raw_tool_names_request_ids_and_arguments(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sentinel = "PRIVATE_TELEMETRY_VALUE_123"
    _span_exporter.clear()
    settings = get_settings()
    server: MCPServer = MCPServer("probe")
    mcp_main._harden_sdk_middleware(server)
    guard = Guard(server, settings, InProcessQuota())

    @guard.tool(ToolClass.READ)
    async def probe(query: str) -> dict[str, str]:
        return {"query": query}

    app = server.streamable_http_app(
        json_response=True,
        stateless_http=True,
        transport_security=mcp_main._transport_security(settings),
        host="127.0.0.1",
    )
    headers = {"Accept": "application/json, text/event-stream"}
    caplog.set_level("INFO")
    caplog.clear()
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
        ) as client,
    ):
        unknown = await client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": sentinel,
                "method": "tools/call",
                "params": {"name": sentinel, "arguments": {}},
            },
            headers=headers,
        )
        known = await client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "probe", "arguments": {"query": sentinel}},
            },
            headers=headers,
        )

    spans = _span_exporter.get_finished_spans()
    exported = json.dumps(
        [{"name": item.name, "attributes": dict(item.attributes)} for item in spans],
        default=str,
    )
    assert unknown.status_code == 200
    assert unknown.json()["error"]["data"] == {"code": "invalid_input", "field": "name"}
    assert known.status_code == 200
    assert sentinel not in exported
    assert sentinel not in caplog.text
    assert [item.name for item in spans] == ["vera.mcp.tool"]
