"""Per-tool policy: authorization class, behavioral annotations, input bounds, quotas.

One place decides, for each MCP tool, which scope it needs, what hints it advertises,
how large its inputs may be, and which abuse bucket it draws from. Keeping this out of
``main.py`` lets the tool registrations stay declarative and lets the rules be unit
tested without a server. Bounds mirror the REST boundary in ``routers/knowledge.py``;
graph ``depth`` and a maximum ``query`` length are added here because REST bounds
neither.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import cast

from mcp_types import ToolAnnotations

from vera.config.settings import McpSettings
from vera.entrypoints.mcp import errors


class ToolClass(Enum):
    """The authorization class of a tool. Determines the extra scope a caller needs."""

    READ = "read"
    PROPOSE = "propose"
    FEEDBACK = "feedback"
    SNAPSHOT = "snapshot"


def scope_for(tool_class: ToolClass, mcp: McpSettings) -> str:
    return {
        ToolClass.READ: mcp.scope_read,
        ToolClass.PROPOSE: mcp.scope_propose,
        ToolClass.FEEDBACK: mcp.scope_feedback,
        ToolClass.SNAPSHOT: mcp.scope_snapshot,
    }[tool_class]


def annotations_for(
    tool_class: ToolClass,
    *,
    read_only: bool | None = None,
    idempotent: bool | None = None,
    destructive: bool = False,
) -> ToolAnnotations:
    """Behavioral hints for a tool. Reads default to read-only and idempotent; writes to
    neither. ``knowledge_get_context`` overrides ``read_only`` because it can persist a pack.
    Every tool queries an open knowledge base, so open-world is always true. Self-retraction
    overrides the destructive hint because it permanently retracts a proposal.
    """
    ro = tool_class is ToolClass.READ if read_only is None else read_only
    idem = tool_class is ToolClass.READ if idempotent is None else idempotent
    return ToolAnnotations(
        read_only_hint=ro,
        destructive_hint=destructive,
        idempotent_hint=idem,
        open_world_hint=True,
    )


@dataclass(frozen=True)
class _Int:
    lo: int
    hi: int


@dataclass(frozen=True)
class _Float:
    lo: float
    hi: float


@dataclass(frozen=True)
class _Str:
    min_len: int
    max_len: int


# Bound per parameter name. Names are consistent across tools, so one table covers all;
# the ``limit`` ceiling varies by tool and is overridden in ``_LIMIT_MAX``.
_BOUNDS: dict[str, _Int | _Float | _Str] = {
    "query": _Str(1, 8192),
    "entity": _Str(1, 1024),
    "entity_id": _Str(1, 512),
    "subject": _Str(1, 512),
    "predicate": _Str(1, 2048),
    "object": _Str(1, 2048),
    "evidence_text": _Str(0, 8000),
    "project": _Str(1, 512),
    "repository": _Str(1, 1024),
    "repository_ref": _Str(1, 256),
    "branch": _Str(1, 512),
    "code_path": _Str(1, 1024),
    "document_type": _Str(1, 256),
    "source_type": _Str(1, 256),
    "fact_key": _Str(1, 512),
    "source_id": _Str(1, 512),
    "snapshot_id": _Str(1, 512),
    "pack_id": _Str(1, 512),
    "context_pack_id": _Str(1, 512),
    "community_id": _Str(1, 512),
    "derivation_run_id": _Str(1, 512),
    "cursor": _Str(1, 1024),
    "result_ref": _Str(1, 512),
    "usage_ref": _Str(1, 512),
    "runtime": _Str(1, 256),
    "session_ref": _Str(1, 256),
    "task_ref": _Str(1, 256),
    "as_of": _Str(1, 64),
    "known_as_of": _Str(1, 64),
    "limit": _Int(1, 50),
    "depth": _Int(1, 5),
    "token_budget": _Int(100, 32000),
    "min_authority": _Float(0.0, 1.0),
    "max_trust_tier": _Int(0, 4),
}

# Tools whose list size may exceed the default ceiling of 50, matching the REST bounds.
_LIMIT_MAX: dict[str, int] = {
    "memory_explore": 200,
    "memory_recent_changes": 200,
    "knowledge_explore": 200,
    "knowledge_get_entity": 500,
    "knowledge_proposal_report": 100,
    "knowledge_search_communities": 100,
    "knowledge_get_community_lineage": 200,
    "knowledge_get_changes": 200,
    "knowledge_get_conflicts": 200,
}

_MAX_PREDICATES = 64
_MAX_PREDICATE_LEN = 256


def validate_bounds(tool: str, kwargs: dict[str, object]) -> None:
    """Raise ``invalid_input`` for the first out-of-range argument. A supplied value that
    is ``None`` (an omitted optional) is skipped; the SDK has already checked types.
    """
    for name, value in kwargs.items():
        if value is not None:
            _validate_one(tool, name, value)


def _validate_one(tool: str, name: str, value: object) -> None:
    if name == "signal":
        if value not in ("up", "down"):
            raise errors.invalid_input(name, "must be 'up' or 'down'")
        return
    if name in ("include_predicates", "exclude_predicates"):
        _validate_predicates(name, value)
        return
    bound = _BOUNDS.get(name)
    if isinstance(bound, _Str):
        _check_str(name, value, bound)
    elif isinstance(bound, _Int):
        _check_int(tool, name, value, bound)
    elif isinstance(bound, _Float):
        _check_float(name, value, bound)


def _check_str(name: str, value: object, bound: _Str) -> None:
    if isinstance(value, str) and not bound.min_len <= len(value) <= bound.max_len:
        raise errors.invalid_input(name, f"length must be {bound.min_len}..{bound.max_len}")


def _check_int(tool: str, name: str, value: object, bound: _Int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        return
    hi = _LIMIT_MAX.get(tool, bound.hi) if name == "limit" else bound.hi
    if not bound.lo <= value <= hi:
        raise errors.invalid_input(name, f"must be {bound.lo}..{hi}")


def _check_float(name: str, value: object, bound: _Float) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return
    if not bound.lo <= value <= bound.hi:
        raise errors.invalid_input(name, f"must be {bound.lo}..{bound.hi}")


def _validate_predicates(name: str, value: object) -> None:
    if not isinstance(value, list):
        return
    items = cast("list[object]", value)
    if len(items) > _MAX_PREDICATES:
        raise errors.invalid_input(name, f"at most {_MAX_PREDICATES} entries")
    for item in items:
        if isinstance(item, str) and len(item) > _MAX_PREDICATE_LEN:
            raise errors.invalid_input(name, f"each entry at most {_MAX_PREDICATE_LEN} chars")


@dataclass(frozen=True)
class QuotaRule:
    bucket: str
    limit: int
    window_seconds: int


def quota_for(tool: str, tool_class: ToolClass, mcp: McpSettings) -> QuotaRule | None:
    """The abuse bucket a tool draws from, or ``None`` when quotas are disabled. Context
    assembly (which can persist) and snapshots are budgeted apart from plain reads.
    """
    if not mcp.quota_enabled:
        return None
    if tool == "knowledge_get_context":
        return QuotaRule("context", mcp.quota_context_per_minute, 60)
    match tool_class:
        case ToolClass.READ:
            return QuotaRule("read", mcp.quota_reads_per_minute, 60)
        case ToolClass.PROPOSE:
            return QuotaRule("propose", mcp.quota_proposals_per_minute, 60)
        case ToolClass.FEEDBACK:
            return QuotaRule("feedback", mcp.quota_feedback_per_minute, 60)
        case ToolClass.SNAPSHOT:
            return QuotaRule("snapshot", mcp.quota_snapshots_per_hour, 3600)
