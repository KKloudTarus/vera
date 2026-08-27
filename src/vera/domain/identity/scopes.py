"""Scope helpers for group_ids.

VERA group_ids are prefixed by scope: ``o:`` org, ``w:`` workspace, ``p:`` project,
``u:`` personal. Shared scopes (org/workspace/project) hold verified, shared memory;
personal scope is isolated. The contamination guard uses this so unverified or
personal content never lands in a shared group_id (enforced at publish, in curation).
"""

from __future__ import annotations

from enum import StrEnum


class ScopeKind(StrEnum):
    ORG = "org"
    WORKSPACE = "workspace"
    PROJECT = "project"
    PERSONAL = "personal"
    UNKNOWN = "unknown"


_PREFIX_TO_KIND = {
    "o": ScopeKind.ORG,
    "w": ScopeKind.WORKSPACE,
    "p": ScopeKind.PROJECT,
    "u": ScopeKind.PERSONAL,
}


def scope_kind(group_id: str) -> ScopeKind:
    prefix, sep, _ = group_id.partition(":")
    if not sep:
        return ScopeKind.UNKNOWN
    return _PREFIX_TO_KIND.get(prefix, ScopeKind.UNKNOWN)


def is_shared_scope(group_id: str) -> bool:
    return scope_kind(group_id) in {ScopeKind.ORG, ScopeKind.WORKSPACE, ScopeKind.PROJECT}
