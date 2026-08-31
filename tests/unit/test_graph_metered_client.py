from __future__ import annotations

from types import SimpleNamespace

import pytest
from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.llm_client.openai_client import OpenAIClient
from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient
from graphiti_core.prompts.models import Message

from vera.adapters.graph.metered import build_metered_llm_client
from vera.observability.cost import UsageEvent


class _Sink:
    def __init__(self) -> None:
        self.events: list[UsageEvent] = []

    async def record(self, event: UsageEvent) -> None:
        self.events.append(event)


def test_official_openai_uses_responses_client() -> None:
    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="test"), llm_model="test", sink=None
    )

    assert isinstance(client, OpenAIClient)


def test_custom_base_url_uses_compatible_chat_client() -> None:
    client = build_metered_llm_client(
        LLMConfig(api_key="test", model="test", base_url="http://llm.test/v1"),
        llm_model="test",
        sink=None,
    )

    assert isinstance(client, OpenAIGenericClient)
    assert client.structured_output_mode == "json_object"


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
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"value":"ok"}'))],
            usage=SimpleNamespace(prompt_tokens=11, completion_tokens=3),
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
