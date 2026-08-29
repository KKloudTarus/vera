"""Characterization of current governance semantics that the Knowledge Fabric evolution
must preserve or deliberately supersede.

These pin the behavior Phase 2 reconciliation and Phase 6/7 governance build on: trust ->
action/authority mapping, and single- vs multi-valued predicate classification. If a later
phase changes any of these, that change must be intentional and update this file, so the
shift is visible in review rather than silent. The broader end-to-end behavior (ingest,
search, RLS, retraction, rebuild) is characterized by the existing integration suite.
"""

from __future__ import annotations

from vera.domain.curation.trust import (
    TrustAction,
    TrustTier,
    action_for_tier,
    authority_for_tier,
)
from vera.domain.ontology.registry import ONTOLOGY_VERSION, is_single_valued


def test_current_ontology_version_is_two() -> None:
    assert ONTOLOGY_VERSION == 2


def test_trust_tier_to_publish_action_is_stable() -> None:
    assert action_for_tier(TrustTier.AUTHORITATIVE) is TrustAction.AUTO_PUBLISH
    assert action_for_tier(TrustTier.CURATED) is TrustAction.AUTO_PUBLISH
    assert action_for_tier(TrustTier.INFORMATIONAL) is TrustAction.REVIEW_REQUIRED
    assert action_for_tier(TrustTier.UNVERIFIED) is TrustAction.PROPOSAL_ONLY


def test_trust_tier_to_authority_is_stable() -> None:
    # Authority ordering must remain strictly descending: lower tiers never outrank higher.
    authorities = [authority_for_tier(t) for t in (1, 2, 3, 4)]
    assert authorities == sorted(authorities, reverse=True)
    assert authorities[0] == 1.0  # Tier 1 authoritative


def test_single_valued_predicate_classification_is_stable() -> None:
    # slot_key-based supersession in Phase 2 relies on this classification.
    assert is_single_valued("RUNS_ON") is True
    assert is_single_valued("runs_on") is True  # case-insensitive
    assert is_single_valued("DEPENDS_ON") is False  # multi-valued, values coexist
