from __future__ import annotations

import asyncio
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from evals import adapters
from evals.adapters import (
    ActionRequest,
    ActionResponse,
    AdapterProtocolError,
    SubprocessActionDriver,
)


def _request(action: str = "search.http") -> ActionRequest:
    return ActionRequest(
        run_id="run-test",
        case_id="CASE-001",
        step_id="S1",
        action=action,
        isolation="none",
        effect="read",
        inputs={"query": "synthetic"},
        observe=("search",),
        run_context={"environment": "test"},
    )


def test_action_response_rejects_unknown_fields() -> None:
    with pytest.raises(AdapterProtocolError, match="unknown fields"):
        ActionResponse.from_dict({"schema_version": "1.0", "status": "PASS", "unexpected": True})


def test_action_response_requires_one_evidence_label() -> None:
    with pytest.raises(AdapterProtocolError, match="requires one non-empty label"):
        ActionResponse.from_dict(
            {
                "schema_version": "1.0",
                "status": "PASS",
                "evidence": [{"kind": "file"}],
            }
        )


def test_action_response_validates_reported_cost() -> None:
    response = ActionResponse.from_dict(
        {"schema_version": "1.0", "status": "PASS", "cost_usd": 0.125}
    )

    assert response.cost_usd == 0.125
    with pytest.raises(AdapterProtocolError, match="cost_usd"):
        ActionResponse.from_dict(
            {"schema_version": "1.0", "status": "PASS", "cost_usd": float("nan")}
        )


def test_action_request_serializes_evidence_labels() -> None:
    request = replace(_request(), evidence_labels=("query trace", "result IDs"))

    assert request.to_dict()["evidence_labels"] == ["query trace", "result IDs"]


def test_subprocess_driver_round_trip(tmp_path: Path) -> None:
    script = tmp_path / "adapter.py"
    script.write_text(
        "import json, sys\n"
        "request = json.load(sys.stdin)\n"
        "json.dump({'schema_version': '1.0', 'status': 'PASS', "
        "'request_nonce': request['request_nonce'], "
        "'observations': {'search': {'action': request['action']}}}, sys.stdout)\n",
        encoding="utf-8",
    )
    driver = SubprocessActionDriver(
        command=(sys.executable, str(script)), capabilities=frozenset({"search.http"})
    )

    response = asyncio.run(driver.execute(_request()))

    assert response.status == "PASS"
    assert response.observations == {"search": {"action": "search.http"}}


def test_subprocess_driver_rejects_a_response_for_another_request(tmp_path: Path) -> None:
    script = tmp_path / "adapter.py"
    script.write_text(
        "import json, sys\n"
        "json.load(sys.stdin)\n"
        "json.dump({'schema_version': '1.0', 'request_nonce': 'stale', "
        "'status': 'PASS'}, sys.stdout)\n",
        encoding="utf-8",
    )
    driver = SubprocessActionDriver(
        command=(sys.executable, str(script)), capabilities=frozenset({"search.http"})
    )

    with pytest.raises(AdapterProtocolError, match="request_nonce"):
        asyncio.run(driver.execute(_request()))


def test_subprocess_driver_fails_closed_on_nonzero_exit(tmp_path: Path) -> None:
    script = tmp_path / "adapter.py"
    script.write_text("raise SystemExit(7)\n", encoding="utf-8")
    driver = SubprocessActionDriver(
        command=(sys.executable, str(script)), capabilities=frozenset({"search.http"})
    )

    response = asyncio.run(driver.execute(_request()))

    assert response.status == "BLOCKED"
    assert response.observations["adapter"]["exit_code"] == 7


def test_subprocess_driver_rejects_non_json_stdout(tmp_path: Path) -> None:
    script = tmp_path / "adapter.py"
    script.write_text("print('not json')\n", encoding="utf-8")
    driver = SubprocessActionDriver(
        command=(sys.executable, str(script)), capabilities=frozenset({"search.http"})
    )

    with pytest.raises(AdapterProtocolError, match="not one JSON object"):
        asyncio.run(driver.execute(_request()))


def test_subprocess_driver_capabilities_are_explicit() -> None:
    driver = SubprocessActionDriver(
        command=(sys.executable, "adapter.py"),
        capabilities=frozenset({"search.http"}),
    )

    assert driver.supports("search.http")
    assert not driver.supports("record.ingest")

    with pytest.raises(ValueError, match="explicit non-empty set"):
        SubprocessActionDriver(command=(sys.executable, "adapter.py"), capabilities=frozenset())


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_subprocess_driver_bounds_output_while_reading(tmp_path: Path, stream: str) -> None:
    script = tmp_path / "adapter.py"
    script.write_text(
        f"import sys\nsys.{stream}.write('x' * 4096)\nsys.{stream}.flush()\n",
        encoding="utf-8",
    )
    driver = SubprocessActionDriver(
        command=(sys.executable, str(script)),
        capabilities=frozenset({"search.http"}),
        max_response_bytes=128,
    )

    response = asyncio.run(driver.execute(_request()))

    assert response.status == "BLOCKED"
    assert stream in response.message


def test_subprocess_cancellation_terminates_descendants(tmp_path: Path) -> None:
    script = tmp_path / "adapter.py"
    ready = tmp_path / "ready"
    child_marker = tmp_path / "child-finished"
    child_code = (
        "import time; from pathlib import Path; time.sleep(0.8); "
        f"Path({str(child_marker)!r}).write_text('alive')"
    )
    script.write_text(
        "import subprocess, sys, time\n"
        f"subprocess.Popen([sys.executable, '-c', {child_code!r}])\n"
        f"open({str(ready)!r}, 'w').close()\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    driver = SubprocessActionDriver(
        command=(sys.executable, str(script)),
        capabilities=frozenset({"search.http"}),
        timeout_s=30,
    )

    async def cancel_after_start() -> None:
        task = asyncio.create_task(driver.execute(_request()))
        for _ in range(200):
            if ready.exists():
                break
            await asyncio.sleep(0.01)
        assert ready.exists()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(1.0)

    asyncio.run(cancel_after_start())

    assert not child_marker.exists()


def test_repeated_cancellation_waits_for_finalizer() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    finalized = False

    async def finalizer() -> None:
        nonlocal finalized
        started.set()
        await release.wait()
        finalized = True

    async def cancel_twice() -> None:
        finalizer_task = asyncio.create_task(finalizer())
        task = asyncio.create_task(adapters.finish_finalizer(finalizer_task))
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_twice())
    assert finalized is True
