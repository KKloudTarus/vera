"""The LLM contradiction judge parses the model's verdict (fake client, no network)."""

from __future__ import annotations

from typing import Any

import pytest

from vera.adapters.curation.judge import LlmContradictionJudge


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


def _judge(content: str) -> LlmContradictionJudge:
    return LlmContradictionJudge(model="gpt-4.1-nano", client=_Client(content))


@pytest.mark.asyncio
async def test_returns_contradicted_subset() -> None:
    judge = _judge('{"contradicted": ["postgres"]}')
    result = await judge.contradictions(
        subject="paymentapi",
        predicate="DEPENDS_ON",
        new_object="valkey",
        existing_objects=["postgres", "redis"],
    )
    assert result == {"postgres"}


@pytest.mark.asyncio
async def test_no_existing_objects_skips_the_model() -> None:
    judge = _judge('{"contradicted": ["x"]}')
    assert (
        await judge.contradictions(
            subject="s", predicate="OWNS", new_object="o", existing_objects=[]
        )
        == set()
    )


@pytest.mark.asyncio
async def test_hallucinated_values_are_filtered_out() -> None:
    judge = _judge('{"contradicted": ["not-a-candidate"]}')
    result = await judge.contradictions(
        subject="s", predicate="DEPENDS_ON", new_object="o", existing_objects=["postgres"]
    )
    assert result == set()


@pytest.mark.asyncio
async def test_bad_json_returns_empty() -> None:
    judge = _judge("nonsense")
    result = await judge.contradictions(
        subject="s", predicate="DEPENDS_ON", new_object="o", existing_objects=["postgres"]
    )
    assert result == set()
