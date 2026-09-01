"""Shared, allowlisted rollout state for the disposable production gate."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

SERVICES = ("api", "worker", "mcp")

FABRIC_WRITE_MODE = "VERA_MEMORY__FABRIC_WRITE_MODE"
FABRIC_ENABLED = "VERA_MEMORY__FABRIC_ENABLED"
ROLE_ENFORCEMENT = "VERA_DB__ROLE_ENFORCEMENT"
VECTOR_SEARCH_ENABLED = "VERA_MEMORY__VECTOR_SEARCH_ENABLED"
COMMUNITY_BUILD_ENABLED = "VERA_WORKER__COMMUNITY_BUILD_ENABLED"
COMMUNITY_BUILD_GROUP_ID = "VERA_WORKER__COMMUNITY_BUILD_GROUP_ID"

CONTROL_DEFAULTS = {
    FABRIC_WRITE_MODE: "legacy",
    FABRIC_ENABLED: "false",
    ROLE_ENFORCEMENT: "false",
    VECTOR_SEARCH_ENABLED: "false",
    COMMUNITY_BUILD_ENABLED: "false",
    COMMUNITY_BUILD_GROUP_ID: "",
}
CONTROL_VALUES = {
    FABRIC_WRITE_MODE: frozenset(("legacy", "dual", "fabric")),
    FABRIC_ENABLED: frozenset(("false",)),
    ROLE_ENFORCEMENT: frozenset(("false", "true")),
    VECTOR_SEARCH_ENABLED: frozenset(("false", "true")),
    COMMUNITY_BUILD_ENABLED: frozenset(("false", "true")),
}


@dataclass(frozen=True, slots=True)
class RolloutTransition:
    environment_key: str
    services: tuple[str, ...]
    baseline: str
    rollout: str


TRANSITIONS = {
    "legacy_to_dual": RolloutTransition(FABRIC_WRITE_MODE, ("worker",), "legacy", "dual"),
    "dual_to_fabric": RolloutTransition(FABRIC_WRITE_MODE, ("worker",), "dual", "fabric"),
    "role_enforcement_off_to_on": RolloutTransition(ROLE_ENFORCEMENT, SERVICES, "false", "true"),
    "vector_retrieval_off_to_on": RolloutTransition(
        VECTOR_SEARCH_ENABLED, SERVICES, "false", "true"
    ),
    "community_build_off_to_on": RolloutTransition(
        COMMUNITY_BUILD_ENABLED, ("worker",), "false", "true"
    ),
}


def normalize_control_environment(values: Mapping[str, object]) -> dict[str, str]:
    normalized = dict(CONTROL_DEFAULTS)
    for key in CONTROL_DEFAULTS:
        if key in values:
            normalized[key] = str(values[key]).lower()
    for key, value in normalized.items():
        if key == COMMUNITY_BUILD_GROUP_ID:
            if value and (not value.startswith(("p:", "o:", "w:")) or len(value) > 200):
                raise ValueError("unsupported community-build group id")
            continue
        if value not in CONTROL_VALUES[key]:
            raise ValueError(f"unsupported rollout value for {key}: {value}")
    return normalized


def process_control_environment() -> dict[str, str]:
    return normalize_control_environment(os.environ)


def configuration_sha256(environments: Mapping[str, Mapping[str, object]]) -> str:
    canonical = {
        service: normalize_control_environment(environment)
        for service, environment in sorted(environments.items())
    }
    return hashlib.sha256(
        json.dumps(canonical, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def read_document(path: Path) -> dict[str, Any]:
    value: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"rollout document {path.name} is not an object")
    return cast(dict[str, Any], value)


def write_document(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
