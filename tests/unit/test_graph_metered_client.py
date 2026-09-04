from __future__ import annotations

from types import SimpleNamespace

import pytest
from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.llm_client.openai_client import OpenAIClient
from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient
from graphiti_core.prompts.models import Message

from vera.adapters.graph.metered import MeteredEmbedder, build_metered_llm_client
from vera.adapters.graph.resilient import ResilientEmbedder
from vera.adapters.resilience.breaker import CircuitBreaker
from vera.adapters.resilience.limiter import InProcessRateLimiter
from vera.adapters.resilience.policy import ResiliencePolicy
from vera.observability.cost import (
    ProviderBudgetContext,
    UsageEvent,
    UsagePersistenceError,
    reset_provider_budget_context,
    set_provider_budget_context,
)


class _Sink:
    def __init__(self) -> None:
        self.events: list[UsageEvent] = []

    async def record(self, event: UsageEvent) -> None:
        self.events.append(event)


class _RetryingEmbedder:
    def __init__(self) -> None:
        self.calls = 0

    async def create(self, _input_data: object) -> list[float]:
        self.calls += 1
        if self.calls == 1:
            raise TimeoutError("ambiguous provider result")
        return [0.1]

    async def create_batch(self, _input_data_list: list[str]) -> list[list[float]]:
        raise NotImplementedError


def _policy() -> ResiliencePolicy:
    return ResiliencePolicy(
        limiter=InProcessRateLimiter(requests_per_minute=1000, tokens_per_minute=1_000_000),
        breaker=CircuitBreaker(name="test", failure_threshold=5, reset_timeout_s=1.0),
        retry_attempts=2,
        initial_backoff_s=0.0,
        max_backoff_s=0.0,
        per_call_timeout_s=5.0,
    )


def test_official_openai_uses_responses_client() -> None:
    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="test"), llm_model="test", sink=None
    )

    assert isinstance(client, OpenAIClient)
    assert client.client.max_retries == 0


def test_custom_base_url_uses_compatible_chat_client() -> None:
    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="test", base_url="http://llm.test/v1"),
        llm_model="test",
        sink=None,
    )

    assert isinstance(client, OpenAIGenericClient)
    assert client.structured_output_mode == "json_object"
    assert client.client.max_retries == 0


@pytest.mark.asyncio
async def test_compatible_chat_client_records_provider_usage() -> None:
    sink = _Sink()
    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="test", base_url="http://llm.test/v1"),
        llm_model="test",
        sink=sink,
    )

    async def create(**_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(
            model="test",
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"value":"ok"}'))],
            usage=SimpleNamespace(prompt_tokens=11, completion_tokens=3),
            cost_usd=0.125,
        )

    client.client = SimpleNamespace(  # type: ignore[attr-defined]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )
    response = await client._generate_response(  # type: ignore[attr-defined]
        [Message(role="user", content="hello")]
    )

    assert response == {"value": "ok"}
    assert len(sink.events) == 1
    assert sink.events[0].prompt_tokens == 11
    assert sink.events[0].completion_tokens == 3
    assert sink.events[0].cost_usd == 0.125
    assert sink.events[0].cost_complete is True


@pytest.mark.asyncio
async def test_compatible_chat_timeout_is_incomplete_and_not_retried() -> None:
    sink = _Sink()
    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="test", base_url="http://llm.test/v1"),
        llm_model="test",
        sink=sink,
        policy=_policy(),
    )
    calls = 0

    async def create(**_kwargs: object) -> SimpleNamespace:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("ambiguous provider result")
        return SimpleNamespace(
            model="test",
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"value":"ok"}'))],
            usage=SimpleNamespace(prompt_tokens=11, completion_tokens=3),
            cost_usd=0.125,
        )

    client.client = SimpleNamespace(  # type: ignore[attr-defined]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(TimeoutError, match="ambiguous"):
        await client._generate_response(  # type: ignore[attr-defined]
            [Message(role="user", content="hello")]
        )

    assert calls == 1
    assert [event.cost_complete for event in sink.events] == [False]


@pytest.mark.asyncio
async def test_compatible_chat_sink_failure_does_not_repeat_provider_call() -> None:
    calls = 0

    class FailingSink:
        async def record(self, _event: UsageEvent) -> None:
            raise RuntimeError("database unavailable")

    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="test", base_url="http://llm.test/v1"),
        llm_model="test",
        sink=FailingSink(),
        policy=_policy(),
    )

    async def create(**_kwargs: object) -> SimpleNamespace:
        nonlocal calls
        calls += 1
        return SimpleNamespace(
            model="test",
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"value":"ok"}'))],
            usage=SimpleNamespace(prompt_tokens=11, completion_tokens=3),
            cost_usd=0.125,
        )

    client.client = SimpleNamespace(  # type: ignore[attr-defined]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(UsagePersistenceError, match="durable provider usage marker failed"):
        await client._generate_response([Message(role="user", content="hello")])  # type: ignore[attr-defined]

    assert calls == 1


@pytest.mark.asyncio
async def test_compatible_chat_rejects_provider_model_substitution() -> None:
    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="test", base_url="http://llm.test/v1"),
        llm_model="test",
        sink=_Sink(),
    )

    async def create(**_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(
            model="substituted-model",
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"value":"ok"}'))],
            usage=SimpleNamespace(prompt_tokens=11, completion_tokens=3),
            cost_usd=0.125,
        )

    client.client = SimpleNamespace(  # type: ignore[attr-defined]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(RuntimeError, match="differs from reserved model"):
        await client._generate_response([Message(role="user", content="hello")])  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_embedding_timeout_is_incomplete_and_not_retried() -> None:
    sink = _Sink()
    inner = _RetryingEmbedder()
    embedder = ResilientEmbedder(
        MeteredEmbedder(inner, model="text-embedding-3-small", sink=sink),  # type: ignore[arg-type]
        _policy(),
    )

    with pytest.raises(TimeoutError, match="ambiguous"):
        await embedder.create("hello")
    assert inner.calls == 1
    assert [event.cost_complete for event in sink.events] == [False]


@pytest.mark.asyncio
async def test_official_openai_response_identity_and_reported_cost_are_settled() -> None:
    class BudgetSink(_Sink):
        def __init__(self) -> None:
            super().__init__()
            self.reserved: list[tuple[str, float]] = []
            self.settled: list[tuple[str, float, float]] = []

        async def reserve_provider_budget(self, action_key: str, max_cost_usd: float) -> bool:
            self.reserved.append((action_key, max_cost_usd))
            return True

        async def settle_provider_budget(
            self, action_key: str, reserved_cost_usd: float, actual_cost_usd: float
        ) -> bool:
            self.settled.append((action_key, reserved_cost_usd, actual_cost_usd))
            return True

    sink = BudgetSink()
    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="gpt-4.1-mini"),
        llm_model="gpt-4.1-mini",
        sink=sink,
    )

    async def create(**_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(
            model="gpt-4.1-mini",
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"value":"ok"}'))],
            usage=SimpleNamespace(prompt_tokens=11, completion_tokens=3),
            cost_usd=0.00001,
        )

    client.client = SimpleNamespace(  # type: ignore[attr-defined]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )
    token = set_provider_budget_context(ProviderBudgetContext("run:case:step"))
    try:
        response = await client._generate_response(  # type: ignore[attr-defined]
            [Message(role="user", content="hello")], max_tokens=100
        )
    finally:
        reset_provider_budget_context(token)

    assert response[0] == {"value": "ok"}
    assert sink.events[0].cost_complete is True
    assert sink.reserved == [("run:case:step", sink.settled[0][1])]
    assert sink.settled[0][2] == sink.events[0].cost_usd


@pytest.mark.asyncio
async def test_official_openai_rejects_provider_model_substitution() -> None:
    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="gpt-4.1-mini"),
        llm_model="gpt-4.1-mini",
        sink=_Sink(),
    )

    async def create(**_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(
            model="substituted-model",
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"value":"ok"}'))],
            usage=SimpleNamespace(prompt_tokens=11, completion_tokens=3),
        )

    client.client = SimpleNamespace(  # type: ignore[attr-defined]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(RuntimeError, match="differs from reserved model"):
        await client._generate_response([Message(role="user", content="hello")])  # type: ignore[attr-defined]
