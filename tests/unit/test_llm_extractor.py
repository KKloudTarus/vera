"""The LLM claim extractor: structured passthrough plus text extraction (fake client)."""

from __future__ import annotations

from typing import Any

import pytest

from vera.adapters.curation.llm_extractor import LlmClaimExtractor


class _Message:
    def __init__(self, content: str) -> None:
        self.content = content


class _Choice:
    def __init__(self, content: str) -> None:
        self.message = _Message(content)


class _Response:
    def __init__(self, content: str) -> None:
        self.choices = [_Choice(content)]


class _FakeCompletions:
    def __init__(self, content: str) -> None:
        self._content = content
        self.calls = 0

    async def create(self, **_kwargs: Any) -> _Response:
        self.calls += 1
        return _Response(self._content)


class _FakeClient:
    def __init__(self, content: str) -> None:
        self.chat = type("Chat", (), {"completions": _FakeCompletions(content)})()


def _extractor(content: str) -> tuple[LlmClaimExtractor, _FakeClient]:
    client = _FakeClient(content)
    return LlmClaimExtractor(model="gpt-4.1-nano", client=client), client


@pytest.mark.asyncio
async def test_structured_metadata_skips_the_llm() -> None:
    extractor, client = _extractor('{"facts": []}')
    claims = await extractor.extract(
        body="ignored",
        knowledge_type="fact_triple",
        metadata={"triples": [{"subject": "a", "predicate": "RUNS_ON", "object": "b"}]},
    )
    assert len(claims) == 1
    assert claims[0].subject == "a"
    assert client.chat.completions.calls == 0  # metadata path never calls the LLM


@pytest.mark.asyncio
async def test_free_text_is_extracted_into_triples() -> None:
    content = (
        '{"facts": [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "prod-eks"},'
        '{"subject": "platform team", "predicate": "OWNS", "object": "cacheapi"}]}'
    )
    extractor, client = _extractor(content)
    claims = await extractor.extract(body="some prose", knowledge_type="text", metadata={})
    assert client.chat.completions.calls == 1
    assert [(c.subject, c.predicate, c.object) for c in claims] == [
        ("paymentapi", "RUNS_ON", "prod-eks"),
        ("platform team", "OWNS", "cacheapi"),
    ]
    assert claims[0].statement == "paymentapi RUNS_ON prod-eks"


@pytest.mark.asyncio
async def test_empty_body_returns_nothing_without_calling_the_llm() -> None:
    extractor, client = _extractor('{"facts": []}')
    assert await extractor.extract(body="   ", knowledge_type="text", metadata={}) == []
    assert client.chat.completions.calls == 0


@pytest.mark.asyncio
async def test_incomplete_triples_are_dropped() -> None:
    extractor, _ = _extractor('{"facts": [{"subject": "a", "predicate": "", "object": "b"}]}')
    assert await extractor.extract(body="prose", knowledge_type="text", metadata={}) == []


@pytest.mark.asyncio
async def test_oversized_triples_are_dropped_without_losing_valid_facts() -> None:
    content = (
        '{"facts": [{"subject": "a", "predicate": "REL", "object": "'
        + ("x" * 513)
        + '"}, {"subject": "b", "predicate": "REL", "object": "c"}]}'
    )
    extractor, _ = _extractor(content)

    claims = await extractor.extract(body="prose", knowledge_type="text", metadata={})

    assert [(claim.subject, claim.predicate, claim.object) for claim in claims] == [
        ("b", "REL", "c")
    ]


@pytest.mark.asyncio
async def test_bad_json_is_handled() -> None:
    extractor, _ = _extractor("not json")
    assert await extractor.extract(body="prose", knowledge_type="text", metadata={}) == []
