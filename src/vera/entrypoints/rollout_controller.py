"""Allowlisted rollout controller for supervised eval application processes."""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import time
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import httpx
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, field_validator

from vera.entrypoints.rollout_control import (
    COMMUNITY_BUILD_GROUP_ID,
    SERVICES,
    TRANSITIONS,
    configuration_sha256,
    normalize_control_environment,
    read_document,
    write_document,
)

app = FastAPI(title="VERA rollout controller", docs_url=None, redoc_url=None)
_lock = asyncio.Lock()
_bearer = HTTPBearer(auto_error=False)


class TransitionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transition: Literal[
        "legacy_to_dual",
        "dual_to_fabric",
        "role_enforcement_off_to_on",
        "vector_retrieval_off_to_on",
        "community_build_off_to_on",
    ]
    direction: Literal["rollout", "rollback"]
    group_id: str

    @field_validator("group_id")
    @classmethod
    def valid_group_id(cls, value: str) -> str:
        if not value.startswith(("p:", "o:", "w:")) or len(value) > 200:
            raise ValueError("group_id is not a supported VERA scope")
        return value


class ResetRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str

    @field_validator("group_id")
    @classmethod
    def valid_group_id(cls, value: str) -> str:
        return TransitionRequest.valid_group_id(value)


class PrepareRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transition: Literal[
        "legacy_to_dual",
        "dual_to_fabric",
        "role_enforcement_off_to_on",
        "vector_retrieval_off_to_on",
        "community_build_off_to_on",
    ]
    group_id: str

    @field_validator("group_id")
    @classmethod
    def valid_group_id(cls, value: str) -> str:
        return TransitionRequest.valid_group_id(value)


def _authorize(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> None:
    expected = os.environ.get("VERA_ROLLOUT_CONTROLLER_TOKEN")
    if not expected:
        raise HTTPException(status_code=503, detail="rollout controller token unavailable")
    if credentials is None or not hmac.compare_digest(credentials.credentials, expected):
        raise HTTPException(
            status_code=401,
            detail="invalid rollout controller credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def _desired_root() -> Path:
    return Path(os.environ.get("VERA_ROLLOUT_DESIRED_ROOT", "/rollout/desired"))


def _status_root() -> Path:
    return Path(os.environ.get("VERA_ROLLOUT_STATUS_ROOT", "/rollout/status"))


def _state_path() -> Path:
    root = Path(os.environ.get("VERA_ROLLOUT_CONTROLLER_STATE_ROOT", "/rollout/state"))
    return root / "controller.state.json"


def _status(service: str) -> dict[str, Any]:
    try:
        status = read_document(_status_root() / f"{service}.status.json")
    except (OSError, ValueError) as exc:
        raise HTTPException(
            status_code=503, detail=f"{service} supervisor status unavailable"
        ) from exc
    environment = status.get("environment")
    if (
        status.get("service") != service
        or status.get("running") is not True
        or not isinstance(status.get("revision"), int)
        or not isinstance(status.get("process_id"), str)
        or not isinstance(environment, dict)
    ):
        raise HTTPException(status_code=503, detail=f"{service} supervisor status invalid")
    status["environment"] = normalize_control_environment(cast(dict[str, object], environment))
    return status


def _statuses(services: tuple[str, ...] = SERVICES) -> dict[str, dict[str, Any]]:
    return {service: _status(service) for service in services}


def _configured_release_baseline() -> dict[str, dict[str, str]]:
    raw = os.environ.get("VERA_ROLLOUT_RELEASE_BASELINE_JSON")
    if not raw:
        raise HTTPException(status_code=503, detail="release baseline is unavailable")
    try:
        value: object = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError
        document = cast(dict[str, object], value)
        if set(document) != set(SERVICES):
            raise ValueError
        if any(not isinstance(document[service], dict) for service in SERVICES):
            raise ValueError
        return {
            service: normalize_control_environment(cast(dict[str, object], document[service]))
            for service in SERVICES
        }
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=503, detail="release baseline is invalid") from exc


def _controller_state() -> dict[str, Any]:
    path = _state_path()
    baseline = _configured_release_baseline()
    if path.exists():
        try:
            state = read_document(path)
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=503, detail="rollout controller state invalid") from exc
        if state.get("release_baseline") != baseline:
            raise HTTPException(
                status_code=503, detail="stored rollout state differs from release baseline"
            )
        return state
    statuses = _statuses()
    state: dict[str, Any] = {
        "revision": max(int(status["revision"]) for status in statuses.values()),
        "active_transition": None,
        "release_baseline": baseline,
        "release_baseline_sha256": configuration_sha256(baseline),
    }
    write_document(path, state)
    return state


async def _wait_for_revision(
    services: tuple[str, ...], revision: int, expected: dict[str, dict[str, str]]
) -> dict[str, dict[str, Any]]:
    deadline = time.monotonic() + float(os.environ.get("VERA_ROLLOUT_TIMEOUT_S", "60"))
    while time.monotonic() < deadline:
        try:
            statuses = _statuses(services)
        except HTTPException:
            statuses = {}
        if statuses and all(
            int(statuses[service]["revision"]) == revision
            and statuses[service]["environment"] == expected[service]
            for service in services
        ):
            return statuses
        await asyncio.sleep(0.2)
    raise HTTPException(
        status_code=503, detail="supervised processes did not apply rollout revision"
    )


async def _health_checks() -> dict[str, bool]:
    checks = {"api": False, "worker": False, "mcp": False}
    deadline = time.monotonic() + float(os.environ.get("VERA_ROLLOUT_TIMEOUT_S", "60"))
    api_url = os.environ.get("VERA_ROLLOUT_API_URL", "http://api:8000").rstrip("/")
    worker_url = os.environ.get("VERA_ROLLOUT_WORKER_METRICS_URL", "http://worker:9100/metrics")
    mcp_host = os.environ.get("VERA_ROLLOUT_MCP_HOST", "mcp")
    mcp_port = int(os.environ.get("VERA_ROLLOUT_MCP_PORT", "8080"))
    while time.monotonic() < deadline and not all(checks.values()):
        async with httpx.AsyncClient(timeout=2.0) as client:
            try:
                checks["api"] = (await client.get(f"{api_url}/health/ready")).status_code == 200
            except httpx.HTTPError:
                checks["api"] = False
            try:
                checks["worker"] = (await client.get(worker_url)).status_code == 200
            except httpx.HTTPError:
                checks["worker"] = False
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(mcp_host, mcp_port), timeout=2.0
            )
            writer.close()
            await writer.wait_closed()
            checks["mcp"] = True
        except (OSError, TimeoutError):
            checks["mcp"] = False
        if not all(checks.values()):
            await asyncio.sleep(0.25)
    return checks


async def _apply_once(
    state: dict[str, Any], environments: dict[str, dict[str, str]]
) -> dict[str, Any]:
    services = tuple(environments)
    before = _statuses(services)
    revision = int(state.get("revision", 0)) + 1
    normalized = {
        service: normalize_control_environment(environment)
        for service, environment in environments.items()
    }
    state["revision"] = revision
    write_document(_state_path(), state)
    for service, environment in normalized.items():
        write_document(
            _desired_root() / f"{service}.desired.json",
            {"service": service, "revision": revision, "environment": environment},
        )
    after = await _wait_for_revision(services, revision, normalized)
    checks = await _health_checks()
    return {
        "before_revision": max(int(status["revision"]) for status in before.values()),
        "after_revision": revision,
        "process_ids": {
            "before": {service: status["process_id"] for service, status in before.items()},
            "after": {service: status["process_id"] for service, status in after.items()},
        },
        "service_environments": {
            service: cast(dict[str, str], status["environment"])
            for service, status in after.items()
        },
        "configuration_sha256": configuration_sha256(
            {
                service: cast(dict[str, str], status["environment"])
                for service, status in after.items()
            }
        ),
        "configuration_applied": all(
            after[service]["environment"] == normalized[service] for service in services
        ),
        "process_restarted": all(
            before[service]["process_id"] != after[service]["process_id"] for service in services
        ),
        "state_changed": any(
            before[service]["environment"] != after[service]["environment"] for service in services
        ),
        "invariants_preserved": all(checks.values()),
        "health_checks": checks,
    }


async def _apply_environments(
    state: dict[str, Any], environments: dict[str, dict[str, str]]
) -> dict[str, Any]:
    before = _statuses(tuple(environments))
    previous = {
        service: cast(dict[str, str], status["environment"]) for service, status in before.items()
    }
    try:
        evidence = await _apply_once(state, environments)
        if evidence["invariants_preserved"] is not True:
            raise HTTPException(status_code=503, detail="rollout health checks failed")
        return evidence
    except (HTTPException, OSError, ValueError) as exc:
        try:
            rollback = await _apply_once(state, previous)
        except (HTTPException, OSError, ValueError) as rollback_exc:
            raise HTTPException(
                status_code=503, detail="rollout failed and automatic rollback failed"
            ) from rollback_exc
        if rollback["invariants_preserved"] is not True:
            raise HTTPException(
                status_code=503, detail="rollout failed and automatic rollback was unhealthy"
            ) from exc
        raise HTTPException(
            status_code=503, detail="rollout failed and the prior state was restored"
        ) from exc


async def _apply_value(
    state: dict[str, Any],
    *,
    transition_name: str,
    value: str,
    extra: dict[str, str] | None = None,
) -> dict[str, Any]:
    transition = TRANSITIONS[transition_name]
    statuses = _statuses(transition.services)
    environments = {
        service: {
            **cast(dict[str, str], status["environment"]),
            transition.environment_key: value,
            **(extra or {}),
        }
        for service, status in statuses.items()
    }
    return await _apply_environments(state, environments)


@app.get("/health")
async def health() -> dict[str, object]:
    statuses = _statuses()
    return {"status": "ready", "services": sorted(statuses)}


@app.post("/v1/prepare")
async def prepare(
    request: PrepareRequest, _authorization: Annotated[None, Depends(_authorize)]
) -> dict[str, Any]:
    async with _lock:
        state = _controller_state()
        if state.get("active_transition") is not None:
            raise HTTPException(status_code=409, detail="another transition is active")
        definition = TRANSITIONS[request.transition]
        evidence = await _apply_value(
            state,
            transition_name=request.transition,
            value=definition.baseline,
            extra=(
                {COMMUNITY_BUILD_GROUP_ID: ""}
                if request.transition == "community_build_off_to_on"
                else None
            ),
        )
        write_document(_state_path(), state)
        return {
            **evidence,
            "transition": request.transition,
            "operation": "prepare",
            "group_id": request.group_id,
            "effective_state": {definition.environment_key: definition.baseline},
        }


@app.post("/v1/transitions")
async def transition(
    request: TransitionRequest, _authorization: Annotated[None, Depends(_authorize)]
) -> dict[str, Any]:
    async with _lock:
        state = _controller_state()
        definition = TRANSITIONS[request.transition]
        active = state.get("active_transition")
        if request.direction == "rollout":
            if active is not None:
                raise HTTPException(status_code=409, detail="another transition is active")
            statuses = _statuses(definition.services)
            if any(
                status["environment"][definition.environment_key] != definition.baseline
                for status in statuses.values()
            ):
                raise HTTPException(
                    status_code=409,
                    detail="transition baseline is not active; prepare it explicitly",
                )
            evidence = await _apply_value(
                state,
                transition_name=request.transition,
                value=definition.rollout,
                extra=(
                    {COMMUNITY_BUILD_GROUP_ID: request.group_id}
                    if request.transition == "community_build_off_to_on"
                    else None
                ),
            )
            state["active_transition"] = request.transition
        else:
            if active != request.transition:
                raise HTTPException(
                    status_code=409, detail="rollback does not match active transition"
                )
            evidence = await _apply_value(
                state,
                transition_name=request.transition,
                value=definition.baseline,
                extra=(
                    {COMMUNITY_BUILD_GROUP_ID: ""}
                    if request.transition == "community_build_off_to_on"
                    else None
                ),
            )
            state["active_transition"] = None
        write_document(_state_path(), state)
        return {
            **evidence,
            "transition": request.transition,
            "direction": request.direction,
            "group_id": request.group_id,
            "effective_state": {
                definition.environment_key: (
                    definition.rollout if request.direction == "rollout" else definition.baseline
                )
            },
            "baseline_restored": request.direction == "rollback",
        }


@app.post("/v1/reset")
async def reset(
    request: ResetRequest, _authorization: Annotated[None, Depends(_authorize)]
) -> dict[str, Any]:
    async with _lock:
        state = _controller_state()
        baseline = state.get("release_baseline")
        if not isinstance(baseline, dict):
            raise HTTPException(status_code=503, detail="release baseline is unavailable")
        baseline_document = cast(dict[str, object], baseline)
        if set(baseline_document) != set(SERVICES):
            raise HTTPException(status_code=503, detail="release baseline is unavailable")
        environments = {
            service: normalize_control_environment(
                cast(dict[str, object], baseline_document[service])
            )
            for service in SERVICES
        }
        evidence = await _apply_environments(state, environments)
        state["active_transition"] = None
        write_document(_state_path(), state)
        return {
            **evidence,
            "group_id": request.group_id,
            "release_baseline_restored": True,
        }
