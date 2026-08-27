"""The verification state machine for candidate claims.

Only these transitions are legal. Guarding them (in the domain and again in the SQL
UPDATE) prevents a claim from skipping review or moving backwards by accident.
"""

from __future__ import annotations

from vera.domain.knowledge.models import VerificationStatus

_ALLOWED: dict[VerificationStatus, set[VerificationStatus]] = {
    VerificationStatus.UNVERIFIED: {
        VerificationStatus.PENDING,
        VerificationStatus.VERIFIED,
        VerificationStatus.DISPUTED,
    },
    VerificationStatus.PENDING: {
        VerificationStatus.VERIFIED,
        VerificationStatus.DISPUTED,
        VerificationStatus.UNVERIFIED,
    },
    VerificationStatus.VERIFIED: {VerificationStatus.DISPUTED},
    VerificationStatus.DISPUTED: {VerificationStatus.VERIFIED},
}


def can_transition(source: VerificationStatus, target: VerificationStatus) -> bool:
    if source == target:
        return True
    return target in _ALLOWED.get(source, set())
