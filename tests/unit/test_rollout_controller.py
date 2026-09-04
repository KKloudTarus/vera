from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from vera.entrypoints import rollout_controller
from vera.entrypoints.rollout_control import normalize_control_environment


def test_controller_authentication_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERA_ROLLOUT_CONTROLLER_TOKEN", "expected-token")

    with pytest.raises(HTTPException) as missing:
        rollout_controller._authorize(None)
    with pytest.raises(HTTPException) as incorrect:
        rollout_controller._authorize(
            HTTPAuthorizationCredentials(scheme="Bearer", credentials="wrong-token")
        )

    assert missing.value.status_code == 401
    assert incorrect.value.status_code == 401
    assert (
        rollout_controller._authorize(
            HTTPAuthorizationCredentials(scheme="Bearer", credentials="expected-token")
        )
        is None
    )


def test_release_baseline_must_be_explicit_and_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VERA_ROLLOUT_RELEASE_BASELINE_JSON", raising=False)
    with pytest.raises(HTTPException, match="release baseline is unavailable"):
        rollout_controller._configured_release_baseline()

    monkeypatch.setenv("VERA_ROLLOUT_RELEASE_BASELINE_JSON", '{"api":{}}')
    with pytest.raises(HTTPException, match="release baseline is invalid"):
        rollout_controller._configured_release_baseline()


@pytest.mark.asyncio
async def test_failed_apply_restores_previous_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous = normalize_control_environment({"VERA_MEMORY__FABRIC_WRITE_MODE": "legacy"})
    requested = normalize_control_environment({"VERA_MEMORY__FABRIC_WRITE_MODE": "dual"})
    calls: list[dict[str, dict[str, str]]] = []

    monkeypatch.setattr(
        rollout_controller,
        "_statuses",
        lambda _services: {
            "worker": {
                "environment": previous,
                "revision": 1,
                "process_id": "100",
            }
        },
    )

    async def apply_once(
        _state: dict[str, Any], environments: dict[str, dict[str, str]]
    ) -> dict[str, Any]:
        calls.append(environments)
        return {"invariants_preserved": len(calls) > 1}

    monkeypatch.setattr(rollout_controller, "_apply_once", apply_once)

    with pytest.raises(HTTPException, match="prior state was restored"):
        await rollout_controller._apply_environments({}, {"worker": requested})

    assert calls == [{"worker": requested}, {"worker": previous}]
