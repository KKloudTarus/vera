"""Per-tool policy: annotations, scope classes, input bounds, and quota buckets."""

from __future__ import annotations

import pytest
from mcp.shared.exceptions import MCPError

from vera.config.settings import McpSettings
from vera.entrypoints.mcp import policy
from vera.entrypoints.mcp.policy import ToolClass


def _code(exc: MCPError) -> str:
    assert isinstance(exc.data, dict)
    return str(exc.data["code"])


def test_read_annotations_are_read_only_and_idempotent() -> None:
    ann = policy.annotations_for(ToolClass.READ)
    assert ann.read_only_hint is True
    assert ann.idempotent_hint is True
    assert ann.destructive_hint is False
    assert ann.open_world_hint is True


def test_write_annotations_are_not_read_only() -> None:
    for cls in (ToolClass.PROPOSE, ToolClass.FEEDBACK, ToolClass.SNAPSHOT):
        ann = policy.annotations_for(cls)
        assert ann.read_only_hint is False
        assert ann.idempotent_hint is False


def test_get_context_overrides_read_only_because_it_persists_a_pack() -> None:
    ann = policy.annotations_for(ToolClass.READ, read_only=False, idempotent=False)
    assert ann.read_only_hint is False
    assert ann.idempotent_hint is False
    assert ann.open_world_hint is True


def test_self_retract_can_be_marked_destructive() -> None:
    ann = policy.annotations_for(ToolClass.PROPOSE, idempotent=True, destructive=True)
    assert ann.destructive_hint is True
    assert ann.idempotent_hint is True


def test_scope_for_maps_each_class() -> None:
    mcp = McpSettings()
    assert policy.scope_for(ToolClass.READ, mcp) == "memory:read"
    assert policy.scope_for(ToolClass.PROPOSE, mcp) == "memory:propose"
    assert policy.scope_for(ToolClass.FEEDBACK, mcp) == "memory:feedback"
    assert policy.scope_for(ToolClass.SNAPSHOT, mcp) == "memory:snapshot"


def test_bounds_accept_values_in_range() -> None:
    policy.validate_bounds("memory_search", {"query": "hello", "limit": 10, "as_of": None})
    policy.validate_bounds("knowledge_get_context", {"token_budget": 2000, "min_authority": 0.5})
    policy.validate_bounds(
        "knowledge_propose",
        {
            "runtime": "opencode",
            "session_ref": "session-1",
            "task_ref": "task-1",
            "repository_ref": "github.com/acme/vera",
        },
    )


def test_none_and_unknown_arguments_are_skipped() -> None:
    policy.validate_bounds("memory_search", {"query": None, "unknown_param": 999})


@pytest.mark.parametrize(
    ("tool", "kwargs", "field"),
    [
        ("memory_search", {"limit": 51}, "limit"),
        ("memory_search", {"limit": 0}, "limit"),
        ("memory_search", {"query": ""}, "query"),
        ("memory_search", {"query": "x" * 9000}, "query"),
        ("memory_explore", {"depth": 6}, "depth"),
        ("knowledge_get_context", {"token_budget": 99}, "token_budget"),
        ("knowledge_get_context", {"token_budget": 40000}, "token_budget"),
        ("knowledge_get_context", {"min_authority": 1.5}, "min_authority"),
        ("knowledge_get_context", {"max_trust_tier": 9}, "max_trust_tier"),
        ("knowledge_propose", {"evidence_text": "e" * 9000}, "evidence_text"),
        ("knowledge_propose", {"subject": "s" * 513}, "subject"),
        ("knowledge_propose", {"task_ref": "t" * 257}, "task_ref"),
        ("knowledge_feedback", {"context_pack_id": "p" * 513}, "context_pack_id"),
        ("memory_feedback", {"signal": "maybe"}, "signal"),
    ],
)
def test_bounds_reject_out_of_range(tool: str, kwargs: dict[str, object], field: str) -> None:
    with pytest.raises(MCPError) as exc:
        policy.validate_bounds(tool, kwargs)
    assert _code(exc.value) == "invalid_input"
    assert exc.value.data["field"] == field


def test_limit_ceiling_is_per_tool() -> None:
    # Feed tools mirror the REST le=200 ceiling; search tools stay at 50.
    policy.validate_bounds("knowledge_get_changes", {"limit": 200})
    policy.validate_bounds("knowledge_proposal_report", {"limit": 100})
    with pytest.raises(MCPError):
        policy.validate_bounds("knowledge_get_changes", {"limit": 201})
    with pytest.raises(MCPError):
        policy.validate_bounds("knowledge_proposal_report", {"limit": 101})
    with pytest.raises(MCPError):
        policy.validate_bounds("knowledge_search", {"limit": 200})


def test_predicate_list_is_bounded() -> None:
    policy.validate_bounds("knowledge_get_context", {"include_predicates": ["p"] * 64})
    with pytest.raises(MCPError) as exc:
        policy.validate_bounds("knowledge_get_context", {"include_predicates": ["p"] * 65})
    assert _code(exc.value) == "invalid_input"


def test_signal_accepts_up_and_down() -> None:
    policy.validate_bounds("memory_feedback", {"signal": "up"})
    policy.validate_bounds("memory_feedback", {"signal": "down"})


def test_subject_accepts_database_maximum() -> None:
    policy.validate_bounds("knowledge_propose", {"subject": "s" * 512})


def test_quota_bucket_separates_context_and_snapshot_from_reads() -> None:
    mcp = McpSettings()
    read = policy.quota_for("memory_search", ToolClass.READ, mcp)
    context = policy.quota_for("knowledge_get_context", ToolClass.READ, mcp)
    snapshot = policy.quota_for("knowledge_create_snapshot", ToolClass.SNAPSHOT, mcp)
    assert read is not None and read.bucket == "read" and read.window_seconds == 60
    assert context is not None and context.bucket == "context"
    assert (
        snapshot is not None and snapshot.bucket == "snapshot" and snapshot.window_seconds == 3600
    )


def test_quota_disabled_returns_none() -> None:
    mcp = McpSettings(quota_enabled=False)
    assert policy.quota_for("memory_search", ToolClass.READ, mcp) is None
