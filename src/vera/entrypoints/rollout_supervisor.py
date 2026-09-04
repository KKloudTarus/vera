"""Restart one eval application child when the allowlisted rollout revision changes."""

from __future__ import annotations

import argparse
import os
import pwd
import signal
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import cast

from vera.entrypoints.rollout_control import (
    ROLE_ENFORCEMENT,
    SERVICES,
    configuration_sha256,
    normalize_control_environment,
    process_control_environment,
    read_document,
    write_document,
)


def _timestamp() -> str:
    return datetime.now(UTC).isoformat()


def _stop_child(child: subprocess.Popen[bytes]) -> None:
    if child.poll() is not None:
        return
    os.killpg(child.pid, signal.SIGTERM)
    try:
        child.wait(timeout=20)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=5)


def _desired(path: Path, service: str) -> tuple[int, dict[str, str]] | None:
    if not path.exists():
        return None
    document = read_document(path)
    if document.get("service") != service or not isinstance(document.get("revision"), int):
        raise ValueError("rollout desired state has invalid service or revision")
    environment = document.get("environment")
    if not isinstance(environment, dict):
        raise ValueError("rollout desired state has no environment object")
    return int(document["revision"]), normalize_control_environment(
        cast(dict[str, object], environment)
    )


def _child_environment(environment: dict[str, str]) -> dict[str, str]:
    child_environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("VERA_ROLLOUT_", "VERA_EVAL_"))
    }
    child_environment.update({"HOME": "/app", "USER": "vera", "LOGNAME": "vera"})
    child_environment.update(environment)
    if environment[ROLE_ENFORCEMENT] == "false":
        legacy_dsn = os.environ.get("VERA_ROLLOUT_LEGACY_DB_DSN")
        if legacy_dsn:
            child_environment["VERA_DB__DSN"] = legacy_dsn
    return child_environment


def _drop_child_privileges() -> None:
    if os.geteuid() != 0:
        return
    account = pwd.getpwnam("vera")
    os.setgroups([])
    os.setgid(account.pw_gid)
    os.setuid(account.pw_uid)


def _start_child(command: list[str], environment: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(  # noqa: S603  fixed by the Compose service definition
        command,
        env=_child_environment(environment),
        start_new_session=True,
        preexec_fn=_drop_child_privileges,
    )


def _write_status(
    path: Path,
    *,
    service: str,
    revision: int,
    environment: dict[str, str],
    child: subprocess.Popen[bytes],
    restart_count: int,
    running: bool,
) -> None:
    write_document(
        path,
        {
            "service": service,
            "revision": revision,
            "environment": environment,
            "configuration_sha256": configuration_sha256({service: environment}),
            "process_id": str(child.pid),
            "supervisor_process_id": str(os.getpid()),
            "restart_count": restart_count,
            "running": running,
            "observed_at": _timestamp(),
        },
    )


def supervise(service: str, command: list[str], desired_root: Path, status_root: Path) -> None:
    desired_path = desired_root / f"{service}.desired.json"
    status_path = status_root / f"{service}.status.json"
    initial = _desired(desired_path, service)
    revision, environment = initial or (0, process_control_environment())
    child = _start_child(command, environment)
    restart_count = 0
    stopping = False

    def stop(_signum: int, _frame: FrameType | None) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    _write_status(
        status_path,
        service=service,
        revision=revision,
        environment=environment,
        child=child,
        restart_count=restart_count,
        running=True,
    )
    try:
        while not stopping:
            requested = _desired(desired_path, service)
            if requested is not None and requested[0] > revision:
                _stop_child(child)
                revision, environment = requested
                child = _start_child(command, environment)
                restart_count += 1
                _write_status(
                    status_path,
                    service=service,
                    revision=revision,
                    environment=environment,
                    child=child,
                    restart_count=restart_count,
                    running=True,
                )
            elif child.poll() is not None:
                child = _start_child(command, environment)
                restart_count += 1
                _write_status(
                    status_path,
                    service=service,
                    revision=revision,
                    environment=environment,
                    child=child,
                    restart_count=restart_count,
                    running=True,
                )
            time.sleep(0.2)
    finally:
        _stop_child(child)
        _write_status(
            status_path,
            service=service,
            revision=revision,
            environment=environment,
            child=child,
            restart_count=restart_count,
            running=False,
        )


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("service", choices=SERVICES)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        raise SystemExit("rollout supervisor requires a child command")
    desired_root = Path(os.environ.get("VERA_ROLLOUT_DESIRED_ROOT", "/rollout/desired"))
    status_root = Path(os.environ.get("VERA_ROLLOUT_STATUS_ROOT", "/rollout/status"))
    status_root.mkdir(parents=True, exist_ok=True)
    os.umask(0o027)
    supervise(str(args.service), command, desired_root, status_root)


if __name__ == "__main__":
    main()
