"""SupersedePolicy is the single contradiction authority shared by all publish paths."""

from __future__ import annotations

from uuid import uuid4

import pytest

from vera.application.curation.supersede import SupersedePolicy
from vera.domain.curation.models import ClaimRecord
from vera.domain.knowledge.models import ClaimType, VerificationStatus


def _claim(obj: str) -> ClaimRecord:
    return ClaimRecord(
        id=uuid4(),
        group_id="p:demo",
        artifact_version_id=uuid4(),
        claim_type=ClaimType.FACT,
        subject="paymentapi",
        predicate="DEPENDS_ON",
        object=obj,
        statement=f"paymentapi DEPENDS_ON {obj}",
        status=VerificationStatus.VERIFIED,
        version_id=1,
    )


class _Judge:
    def __init__(self, contradicted: set[str]) -> None:
        self._contradicted = contradicted

    async def contradictions(
        self, *, subject: str, predicate: str, new_object: str, existing_objects: list[str]
    ) -> set[str]:
        return self._contradicted & set(existing_objects)


@pytest.mark.asyncio
async def test_functional_predicate_supersedes_every_prior_value() -> None:
    policy = SupersedePolicy(judge=_Judge(set()))
    conflicts = [_claim("prod-eks"), _claim("stage-eks")]
    result = await policy.contradicted(
        subject="svc", predicate="RUNS_ON", new_object="gke", conflicts=conflicts
    )
    assert result == conflicts  # RUNS_ON is single-valued: all are replaced


@pytest.mark.asyncio
async def test_multivalued_uses_the_judge() -> None:
    policy = SupersedePolicy(judge=_Judge({"postgres"}))
    conflicts = [_claim("postgres"), _claim("redis")]
    result = await policy.contradicted(
        subject="paymentapi", predicate="DEPENDS_ON", new_object="valkey", conflicts=conflicts
    )
    assert [c.object for c in result] == ["postgres"]  # only the judged one


@pytest.mark.asyncio
async def test_multivalued_without_judge_keeps_both() -> None:
    policy = SupersedePolicy(judge=None)
    conflicts = [_claim("postgres"), _claim("redis")]
    result = await policy.contradicted(
        subject="paymentapi", predicate="DEPENDS_ON", new_object="valkey", conflicts=conflicts
    )
    assert result == []


@pytest.mark.asyncio
async def test_no_conflicts_is_empty() -> None:
    policy = SupersedePolicy(judge=_Judge({"x"}))
    assert (
        await policy.contradicted(subject="s", predicate="DEPENDS_ON", new_object="o", conflicts=[])
        == []
    )
