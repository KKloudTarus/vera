"""Production adapter protocol for VERA evaluation actions."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import secrets
import signal
import subprocess
from dataclasses import dataclass, field
from typing import Any, Protocol

STATUSES = frozenset({"PASS", "FAIL", "BLOCKED", "NOT_APPLICABLE"})


class AdapterProtocolError(RuntimeError):
    """The configured adapter violated the JSON action protocol."""


class _OutputLimitExceeded(RuntimeError):
    pass


async def _read_limited(stream: asyncio.StreamReader, limit: int, stream_name: str) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = await stream.read(min(64 * 1024, limit - size + 1))
        if not chunk:
            return b"".join(chunks)
        size += len(chunk)
        if size > limit:
            raise _OutputLimitExceeded(f"adapter {stream_name} exceeded the byte limit")
        chunks.append(chunk)


async def _terminate_process_tree(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    if os.name == "nt":
        killer = await asyncio.create_subprocess_exec(
            "taskkill",
            "/PID",
            str(process.pid),
            "/T",
            "/F",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await killer.wait()
    else:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=2.0)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(process.wait(), timeout=5.0)


async def _communicate_limited(
    process: asyncio.subprocess.Process, payload: bytes, limit: int
) -> tuple[bytes, bytes]:
    if process.stdin is None or process.stdout is None or process.stderr is None:
        raise RuntimeError("adapter subprocess pipes were not created")
    process.stdin.write(payload)
    with contextlib.suppress(BrokenPipeError, ConnectionResetError):
        await process.stdin.drain()
    process.stdin.close()
    tasks = [
        asyncio.create_task(_read_limited(process.stdout, limit, "stdout")),
        asyncio.create_task(_read_limited(process.stderr, limit, "stderr")),
        asyncio.create_task(process.wait()),
    ]
    try:
        stdout, stderr, _returncode = await asyncio.gather(*tasks)
        return stdout, stderr
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


@dataclass(frozen=True, slots=True)
class ActionRequest:
    run_id: str
    case_id: str
    step_id: str
    action: str
    isolation: str
    effect: str
    inputs: dict[str, Any]
    observe: tuple[str, ...]
    run_context: dict[str, Any]
    evidence_labels: tuple[str, ...] = ()
    request_nonce: str = field(default_factory=lambda: secrets.token_hex(32))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "run_id": self.run_id,
            "case_id": self.case_id,
            "step_id": self.step_id,
            "action": self.action,
            "isolation": self.isolation,
            "effect": self.effect,
            "inputs": self.inputs,
            "observe": list(self.observe),
            "run_context": self.run_context,
            "evidence_labels": list(self.evidence_labels),
            "request_nonce": self.request_nonce,
        }


@dataclass(frozen=True, slots=True)
class ActionResponse:
    status: str
    observations: dict[str, Any] = field(default_factory=dict)
    message: str = ""
    metrics: tuple[dict[str, Any], ...] = ()
    evidence: tuple[dict[str, Any], ...] = ()
    created_resources: tuple[str, ...] = ()
    removed_resources: tuple[str, ...] = ()

    @classmethod
    def from_dict(
        cls,
        value: dict[str, Any],
        *,
        expected_request_nonce: str | None = None,
    ) -> ActionResponse:
        allowed = {
            "schema_version",
            "request_nonce",
            "status",
            "observations",
            "message",
            "metrics",
            "evidence",
            "created_resources",
            "removed_resources",
        }
        unknown = set(value) - allowed
        if unknown:
            raise AdapterProtocolError(f"adapter response has unknown fields: {sorted(unknown)}")
        if value.get("schema_version") != "1.0":
            raise AdapterProtocolError("adapter response schema_version must be 1.0")
        if (
            expected_request_nonce is not None
            and value.get("request_nonce") != expected_request_nonce
        ):
            raise AdapterProtocolError("adapter response request_nonce does not match the request")
        status = value.get("status")
        if status not in STATUSES:
            raise AdapterProtocolError(f"adapter response has invalid status: {status!r}")
        observations = value.get("observations", {})
        metrics = value.get("metrics", [])
        evidence = value.get("evidence", [])
        created = value.get("created_resources", [])
        removed = value.get("removed_resources", [])
        message = value.get("message", "")
        if not isinstance(observations, dict):
            raise AdapterProtocolError("adapter observations must be an object")
        if not isinstance(metrics, list) or not all(isinstance(item, dict) for item in metrics):
            raise AdapterProtocolError("adapter metrics must be an array of objects")
        if not isinstance(evidence, list) or not all(isinstance(item, dict) for item in evidence):
            raise AdapterProtocolError("adapter evidence must be an array of objects")
        for descriptor in evidence:
            label = descriptor.get("label")
            if not isinstance(label, str) or not label:
                raise AdapterProtocolError("adapter evidence requires one non-empty label")
            if descriptor.get("kind", "file") not in {
                "api",
                "mcp",
                "database",
                "graph",
                "log",
                "trace",
                "metric",
                "file",
                "human_label",
            }:
                raise AdapterProtocolError("adapter evidence kind is invalid")
        if not isinstance(created, list) or not all(isinstance(item, str) for item in created):
            raise AdapterProtocolError("created_resources must be an array of strings")
        if not isinstance(removed, list) or not all(isinstance(item, str) for item in removed):
            raise AdapterProtocolError("removed_resources must be an array of strings")
        if not isinstance(message, str):
            raise AdapterProtocolError("adapter message must be a string")
        return cls(
            status=status,
            observations=observations,
            message=message,
            metrics=tuple(metrics),
            evidence=tuple(evidence),
            created_resources=tuple(created),
            removed_resources=tuple(removed),
        )


class ActionDriver(Protocol):
    """Execute allowlisted actions against one configured target environment."""

    def supports(self, action: str) -> bool: ...

    async def execute(self, request: ActionRequest) -> ActionResponse: ...


@dataclass(frozen=True, slots=True)
class UnavailableActionDriver:
    reason: str = "no action adapter is configured"

    def supports(self, action: str) -> bool:
        return False

    async def execute(self, request: ActionRequest) -> ActionResponse:
        return ActionResponse(status="BLOCKED", message=self.reason)


@dataclass(frozen=True, slots=True)
class SubprocessActionDriver:
    """Invoke a trusted JSON-over-stdio adapter without a command shell."""

    command: tuple[str, ...]
    capabilities: frozenset[str]
    timeout_s: float = 120.0
    max_response_bytes: int = 10 * 1024 * 1024

    def __post_init__(self) -> None:
        if not self.command:
            raise ValueError("adapter command cannot be empty")
        if not self.capabilities or not all(
            isinstance(action, str) and action for action in self.capabilities
        ):
            raise ValueError("adapter capabilities must be an explicit non-empty set")
        if self.timeout_s <= 0:
            raise ValueError("adapter timeout must be positive")
        if self.max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")

    def supports(self, action: str) -> bool:
        return action in self.capabilities

    async def execute(self, request: ActionRequest) -> ActionResponse:
        process_options: dict[str, Any]
        if os.name == "nt":
            process_options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        else:
            process_options = {"start_new_session": True}
        process = await asyncio.create_subprocess_exec(
            *self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **process_options,
        )
        payload = json.dumps(
            request.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode()
        try:
            stdout, _stderr = await asyncio.wait_for(
                _communicate_limited(process, payload, self.max_response_bytes),
                timeout=self.timeout_s,
            )
        except TimeoutError:
            await asyncio.shield(_terminate_process_tree(process))
            return ActionResponse(
                status="BLOCKED",
                message=f"adapter timed out after {self.timeout_s:g} seconds",
            )
        except _OutputLimitExceeded as exc:
            await asyncio.shield(_terminate_process_tree(process))
            return ActionResponse(status="BLOCKED", message=str(exc))
        except asyncio.CancelledError:
            await asyncio.shield(_terminate_process_tree(process))
            raise
        except BaseException:
            await asyncio.shield(_terminate_process_tree(process))
            raise
        if process.returncode != 0:
            return ActionResponse(
                status="BLOCKED",
                observations={"adapter": {"exit_code": process.returncode}},
                message=f"adapter exited with code {process.returncode}",
            )
        try:
            parsed = json.loads(stdout)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AdapterProtocolError("adapter stdout is not one JSON object") from exc
        if not isinstance(parsed, dict):
            raise AdapterProtocolError("adapter stdout must be a JSON object")
        return ActionResponse.from_dict(parsed, expected_request_nonce=request.request_nonce)
