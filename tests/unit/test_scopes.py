"""Scope classification for group_ids."""

from __future__ import annotations

import pytest

from vera.domain.identity.scopes import ScopeKind, is_shared_scope, scope_kind


@pytest.mark.parametrize(
    ("group_id", "kind"),
    [
        ("o:acme", ScopeKind.ORG),
        ("w:platform", ScopeKind.WORKSPACE),
        ("p:landing", ScopeKind.PROJECT),
        ("u:alice", ScopeKind.PERSONAL),
        ("weird", ScopeKind.UNKNOWN),
        ("x:other", ScopeKind.UNKNOWN),
    ],
)
def test_scope_kind(group_id: str, kind: ScopeKind) -> None:
    assert scope_kind(group_id) == kind


def test_is_shared_scope() -> None:
    assert is_shared_scope("p:landing") is True
    assert is_shared_scope("o:acme") is True
    assert is_shared_scope("u:alice") is False
    assert is_shared_scope("weird") is False
