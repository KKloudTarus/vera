"""Source-trust tiers and the publish action each implies.

Tier 1 (authoritative, e.g. CMDB/Terraform) and tier 2 (curated, e.g. approved ADRs)
publish automatically. Tier 3 (informational, e.g. Confluence) needs review. Tier 4
(unverified, e.g. Slack, agent observations) can only propose.
"""

from __future__ import annotations

from enum import IntEnum, StrEnum


class TrustTier(IntEnum):
    AUTHORITATIVE = 1
    CURATED = 2
    INFORMATIONAL = 3
    UNVERIFIED = 4


class TrustAction(StrEnum):
    AUTO_PUBLISH = "auto_publish"
    REVIEW_REQUIRED = "review_required"
    PROPOSAL_ONLY = "proposal_only"


def action_for_tier(tier: int) -> TrustAction:
    if tier <= TrustTier.CURATED:
        return TrustAction.AUTO_PUBLISH
    if tier == TrustTier.INFORMATIONAL:
        return TrustAction.REVIEW_REQUIRED
    return TrustAction.PROPOSAL_ONLY


# Authority weight per tier, used by the retrieval rerank. Higher-trust sources
# outrank lower-trust ones at equal relevance.
_AUTHORITY = {
    TrustTier.AUTHORITATIVE: 1.0,
    TrustTier.CURATED: 0.85,
    TrustTier.INFORMATIONAL: 0.7,
    TrustTier.UNVERIFIED: 0.4,
}


def authority_for_tier(tier: int) -> float:
    try:
        return _AUTHORITY[TrustTier(tier)]
    except ValueError:
        return 0.5
