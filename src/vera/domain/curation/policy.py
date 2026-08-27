"""Publish policy, including the contamination guard.

Shared scopes (org/workspace/project) hold verified, shared memory. Only verified
knowledge may publish there, so unverified or proposal-stage claims never pollute a
shared entity summary. Personal scope accepts anything the owner puts in it.
"""

from __future__ import annotations

from vera.domain.identity.scopes import is_shared_scope
from vera.domain.knowledge.models import VerificationStatus


def may_publish_to(group_id: str, status: VerificationStatus) -> bool:
    if is_shared_scope(group_id):
        return status == VerificationStatus.VERIFIED
    return True
