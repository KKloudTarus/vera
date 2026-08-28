from __future__ import annotations

from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.llm_client.openai_client import OpenAIClient
from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient

from vera.adapters.graph.metered import build_metered_llm_client


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
