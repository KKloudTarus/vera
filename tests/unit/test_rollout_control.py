from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from vera.entrypoints.rollout_control import (
    COMMUNITY_BUILD_GROUP_ID,
    FABRIC_ENABLED,
    FABRIC_WRITE_MODE,
    TRANSITIONS,
    configuration_sha256,
    normalize_control_environment,
    read_document,
    write_document,
)
from vera.entrypoints.rollout_supervisor import _child_environment


def test_rollout_registry_is_fixed_and_secret_free() -> None:
    assert list(TRANSITIONS) == [
        "legacy_to_dual",
        "dual_to_fabric",
        "role_enforcement_off_to_on",
        "vector_retrieval_off_to_on",
        "community_build_off_to_on",
    ]
    assert all("PASSWORD" not in transition.environment_key for transition in TRANSITIONS.values())
    assert all("SECRET" not in transition.environment_key for transition in TRANSITIONS.values())


def test_control_environment_validation_and_hashing() -> None:
    first = normalize_control_environment(
        {FABRIC_WRITE_MODE: "DUAL", COMMUNITY_BUILD_GROUP_ID: "p:canary"}
    )
    second = normalize_control_environment(dict(reversed(list(first.items()))))

    assert first[FABRIC_WRITE_MODE] == "dual"
    assert configuration_sha256({"worker": first}) == configuration_sha256({"worker": second})
    with pytest.raises(ValueError, match="unsupported rollout value"):
        normalize_control_environment({FABRIC_WRITE_MODE: "unknown"})
    with pytest.raises(ValueError, match="unsupported rollout value"):
        normalize_control_environment({FABRIC_ENABLED: "true"})
    with pytest.raises(ValueError, match="group id"):
        normalize_control_environment({COMMUNITY_BUILD_GROUP_ID: "../../host"})


def test_supervisor_excludes_control_credentials_from_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERA_DB__DSN", "postgresql://runtime")
    monkeypatch.setenv("VERA_ROLLOUT_LEGACY_DB_DSN", "postgresql://legacy")
    monkeypatch.setenv("VERA_ROLLOUT_CONTROLLER_TOKEN", "controller-secret")
    monkeypatch.setenv("VERA_EVAL_ADMIN_DSN", "postgresql://owner")

    enforced = normalize_control_environment({"VERA_DB__ROLE_ENFORCEMENT": "true"})
    legacy = normalize_control_environment({"VERA_DB__ROLE_ENFORCEMENT": "false"})

    enforced_child = _child_environment(enforced)
    legacy_child = _child_environment(legacy)
    assert enforced_child["VERA_DB__DSN"] == "postgresql://runtime"
    assert legacy_child["VERA_DB__DSN"] == "postgresql://legacy"
    assert "VERA_ROLLOUT_LEGACY_DB_DSN" not in legacy_child
    assert "VERA_ROLLOUT_CONTROLLER_TOKEN" not in legacy_child
    assert "VERA_EVAL_ADMIN_DSN" not in legacy_child
    assert enforced_child["HOME"] == "/app"
    assert enforced_child["USER"] == "vera"
    assert enforced_child["LOGNAME"] == "vera"


def _wait_for_status(
    path: Path, predicate: Callable[[dict[str, Any]], bool], *, timeout_s: float = 10.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            status = read_document(path)
            if predicate(status):
                return status
        time.sleep(0.05)
    raise AssertionError("supervisor status did not reach the expected state")


def test_supervisor_restarts_child_for_new_revision(tmp_path: Path) -> None:
    desired_root = tmp_path / "desired"
    status_root = tmp_path / "status"
    desired_root.mkdir()
    status_root.mkdir()
    environment = os.environ.copy()
    environment["VERA_ROLLOUT_DESIRED_ROOT"] = str(desired_root)
    environment["VERA_ROLLOUT_STATUS_ROOT"] = str(status_root)
    supervisor = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "vera.entrypoints.rollout_supervisor",
            "api",
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
        ],
        env=environment,
    )
    status_path = status_root / "api.status.json"
    try:
        initial = _wait_for_status(status_path, lambda value: value.get("running") is True)
        desired = normalize_control_environment(
            {**initial["environment"], FABRIC_WRITE_MODE: "dual"}  # type: ignore[arg-type]
        )
        write_document(
            desired_root / "api.desired.json",
            {"service": "api", "revision": 1, "environment": desired},
        )
        restarted = _wait_for_status(status_path, lambda value: value.get("revision") == 1)

        assert restarted["process_id"] != initial["process_id"]
        assert restarted["environment"] == desired
        assert restarted["restart_count"] == 1
    finally:
        supervisor.terminate()
        supervisor.wait(timeout=10)
