"""Semantic-dedup on real embeddings and the real LLM judge (text-embedding-3-small,
gpt-4.1-nano).

The benchmark shows why dedup needs the judge: cosine over bare canonical names scores a
sibling service ("payment service" vs "billing service") higher than true synonyms and a
translation far lower, so no single threshold separates them. The judge, over the blocked
candidates, resolves synonyms and cross-lingual names correctly. Marked ``llm`` (real
OpenAI) and excluded from the default gate.
"""

from __future__ import annotations

import pytest

from vera.application.curation.entity_resolver import cosine
from vera.config.settings import get_settings
from vera.domain.ports.curation import EntityResolutionJudge
from vera.domain.ports.embedder import Embedder

pytestmark = [pytest.mark.integration, pytest.mark.llm, pytest.mark.asyncio]

# (left, right, same real-world entity?)
_LABELED = [
    ("payment service", "paymentapi", True),
    ("payment service", "payment api", True),
    ("payment service", "dich vu thanh toan", True),  # Vietnamese for "payment service"
    ("payment service", "billing service", False),
    ("payment service", "notification service", False),
]


def _settings_or_skip() -> object:
    from dotenv import load_dotenv

    load_dotenv(override=True)
    settings = get_settings()
    if settings.memory.embedder != "openai" or settings.memory.openai_api_key is None:
        pytest.skip("real embedder not configured")
    return settings


@pytest.fixture
def embedder() -> Embedder:
    settings = _settings_or_skip()
    from vera.adapters.graph import build_embedder

    return build_embedder(settings)  # type: ignore[return-value]


@pytest.fixture
def judge() -> EntityResolutionJudge:
    settings = _settings_or_skip()
    from vera.adapters.curation.entity_judge import LlmEntityResolutionJudge

    key = settings.memory.openai_api_key.get_secret_value()  # type: ignore[union-attr]
    # Matches production wiring: entity resolution uses the larger model, which is stable
    # on sibling-vs-same where the small model is not.
    return LlmEntityResolutionJudge(
        api_key=key,
        base_url=settings.memory.openai_base_url,
        model=settings.memory.llm_model,
    )


async def test_cosine_over_bare_names_cannot_separate_the_set(embedder: Embedder) -> None:
    vectors = {}
    for left, right, _ in _LABELED:
        for name in (left, right):
            vectors.setdefault(name, await embedder.embed(name))
    same = [cosine(vectors[a], vectors[b]) for a, b, s in _LABELED if s]
    diff = [cosine(vectors[a], vectors[b]) for a, b, s in _LABELED if not s]
    # The finding that motivates the judge: a different entity outscores a true match,
    # so cosine alone has no separating threshold. It is a candidate generator, not a decider.
    assert max(diff) > min(same)


async def test_judge_resolves_synonyms_and_cross_lingual(judge: EntityResolutionJudge) -> None:
    # An abbreviation is matched to its full name.
    assert (
        await judge.same_entity(
            name="paymentapi",
            entity_type="Service",
            candidates=["payment service", "notification service"],
        )
        == "payment service"
    )
    # A cross-lingual name, which cosine ranks near zero, is matched: this is the case only
    # the judge can catch, since no embedding threshold would surface it.
    assert (
        await judge.same_entity(
            name="payment service",
            entity_type="Service",
            candidates=["dich vu thanh toan", "notification service"],
        )
        == "dich vu thanh toan"
    )
    # Sibling services that only share a domain word are not merged (no false positive).
    assert (
        await judge.same_entity(
            name="payment service",
            entity_type="Service",
            candidates=["notification service", "billing service"],
        )
        is None
    )
