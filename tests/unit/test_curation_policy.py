"""Curation domain policies: trust tiers, state machine, contamination guard."""

from __future__ import annotations

import pytest

from vera.domain.curation.policy import may_publish_to
from vera.domain.curation.state import can_transition
from vera.domain.curation.trust import TrustAction, action_for_tier
from vera.domain.knowledge.models import VerificationStatus as V


@pytest.mark.parametrize(
    ("tier", "action"),
    [
        (1, TrustAction.AUTO_PUBLISH),
        (2, TrustAction.AUTO_PUBLISH),
        (3, TrustAction.REVIEW_REQUIRED),
        (4, TrustAction.PROPOSAL_ONLY),
    ],
)
def test_action_for_tier(tier: int, action: TrustAction) -> None:
    assert action_for_tier(tier) == action


def test_state_transitions() -> None:
    assert can_transition(V.UNVERIFIED, V.VERIFIED) is True
    assert can_transition(V.PENDING, V.VERIFIED) is True
    assert can_transition(V.VERIFIED, V.DISPUTED) is True
    assert can_transition(V.DISPUTED, V.VERIFIED) is True
    assert can_transition(V.VERIFIED, V.PENDING) is False
    assert can_transition(V.VERIFIED, V.UNVERIFIED) is False


def test_contamination_guard() -> None:
    # Shared scopes take only verified knowledge.
    assert may_publish_to("p:landing", V.VERIFIED) is True
    assert may_publish_to("p:landing", V.UNVERIFIED) is False
    assert may_publish_to("w:platform", V.PENDING) is False
    # Personal scope accepts anything.
    assert may_publish_to("u:alice", V.UNVERIFIED) is True
