"""Role ordering and group_id scope classification."""

from __future__ import annotations

from vera.domain.identity.models import Role, role_at_least
from vera.domain.identity.scopes import ScopeKind, is_shared_scope, scope_kind


def test_role_rank_is_a_total_order() -> None:
    assert role_at_least(Role.OWNER, Role.ADMIN)
    assert role_at_least(Role.ADMIN, Role.MEMBER)
    assert role_at_least(Role.MEMBER, Role.VIEWER)
    assert role_at_least(Role.ADMIN, Role.ADMIN)


def test_lower_role_does_not_meet_higher_minimum() -> None:
    assert not role_at_least(Role.MEMBER, Role.ADMIN)
    assert not role_at_least(Role.VIEWER, Role.OWNER)


def test_scope_kind_reads_the_prefix() -> None:
    assert scope_kind("o:123") is ScopeKind.ORG
    assert scope_kind("w:123") is ScopeKind.WORKSPACE
    assert scope_kind("p:123") is ScopeKind.PROJECT
    assert scope_kind("u:123") is ScopeKind.PERSONAL
    assert scope_kind("nope") is ScopeKind.UNKNOWN


def test_only_org_workspace_project_are_shared() -> None:
    assert is_shared_scope("o:1")
    assert is_shared_scope("w:1")
    assert is_shared_scope("p:1")
    assert not is_shared_scope("u:1")
