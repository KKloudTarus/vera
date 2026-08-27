"""The LLM entity-resolution judge parses the model's verdict (fake client, no network)."""

from __future__ import annotations

from typing import Any

import pytest

from vera.adapters.curation.entity_judge import LlmEntityResolutionJudge


class _Resp:
    def __init__(self, content: str) -> None:
        self.choices = [type("C", (), {"message": type("M", (), {"content": content})()})()]


class _Completions:
    def __init__(self, content: str) -> None:
        self._content = content

    async def create(self, **_kwargs: Any) -> _Resp:
        return _Resp(self._content)


class _Client:
    def __init__(self, content: str) -> None:
        self.chat = type("Chat", (), {"completions": _Completions(content)})()


def _judge(content: str) -> LlmEntityResolutionJudge:
    return LlmEntityResolutionJudge(model="gpt-4.1-nano", client=_Client(content))


@pytest.mark.asyncio
async def test_returns_the_matched_candidate() -> None:
    judge = _judge('{"match": "paymentapi"}')
    result = await judge.same_entity(
        name="payment service", entity_type="Service", candidates=["paymentapi", "billing"]
    )
    assert result == "paymentapi"


@pytest.mark.asyncio
async def test_null_match_returns_none() -> None:
    judge = _judge('{"match": null}')
    result = await judge.same_entity(
        name="payment service", entity_type="Service", candidates=["billing"]
    )
    assert result is None


@pytest.mark.asyncio
async def test_no_candidates_skips_the_model() -> None:
    judge = _judge('{"match": "x"}')
    assert await judge.same_entity(name="s", entity_type="Service", candidates=[]) is None


@pytest.mark.asyncio
async def test_hallucinated_match_is_filtered_out() -> None:
    judge = _judge('{"match": "not-a-candidate"}')
    result = await judge.same_entity(name="s", entity_type="Service", candidates=["billing"])
    assert result is None


@pytest.mark.asyncio
async def test_bad_json_returns_none() -> None:
    judge = _judge("nonsense")
    result = await judge.same_entity(name="s", entity_type="Service", candidates=["billing"])
    assert result is None
