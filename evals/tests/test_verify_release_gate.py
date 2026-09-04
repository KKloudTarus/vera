from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

import evals.validate as validate_module
from evals.judging.aggregate import aggregate
from evals.judging.finalize import finalize
from evals.runner import EvaluationRunner
from evals.tests.test_runner import (
    SyntheticDriver,
    _config,
    _contract_copy,
    _external_panel_inputs,
)
from evals.validate import load_json
from evals.verify_release_gate import verify_release_gate

GIT_SHA = "0123456789abcdef0123456789abcdef01234567"
RUN_ID = "20260904T120000Z-01234567-release"


def _write_json(path: Path, value: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _evidence(root: Path) -> tuple[Path, Path, Path, str]:
    run_root = root / "evals" / "runs" / RUN_ID
    report_path = run_root / "report.json"
    report_sha = _write_json(
        report_path,
        {
            "run_id": RUN_ID,
            "profile": "release",
            "status": "PASS",
            "quality_status": "PENDING_JUDGMENT",
            "manifest": {
                "git_sha": GIT_SHA,
                "git_dirty": False,
                "app_image_digest": "sha256:" + "a" * 64,
            },
        },
    )
    panel_path = run_root / "panel" / "results" / "REAL-001.json"
    panel = {
        "packet_id": f"{RUN_ID}-REAL-001",
        "status": "PASS",
        "quality_status": "PASS",
        "overall_score": 0.9,
        "judge_count": 4,
    }
    panel_sha = _write_json(panel_path, panel)
    gate_path = run_root / "quality-gate.json"
    _write_json(
        gate_path,
        {
            "schema_version": "1.0",
            "run_id": RUN_ID,
            "profile": "release",
            "generated_at": "2026-09-04T12:00:00Z",
            "execution_report": {
                "ref": f"/output/{RUN_ID}/report.json",
                "sha256": report_sha,
                "status": "PASS",
                "quality_status": "PENDING_JUDGMENT",
            },
            "panel_results": [
                {
                    "packet_id": panel["packet_id"],
                    "ref": f"/output/{RUN_ID}/panel/results/REAL-001.json",
                    "sha256": panel_sha,
                    "status": "PASS",
                    "quality_status": "PASS",
                    "overall_score": 0.9,
                    "judge_count": 4,
                }
            ],
            "quality_status": "PASS",
            "status": "PASS",
            "reason": "execution gates and every bound panel result pass",
        },
    )
    return gate_path, report_path, panel_path, panel_sha


def _valid_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, set[str]]:
    contract_root = _contract_copy(tmp_path, monkeypatch, fill_production=True)
    cases = validate_module.load_cases()
    outcome = asyncio.run(
        EvaluationRunner(
            _config(contract_root, profile="release", run_id=RUN_ID),
            SyntheticDriver(cases),
            root=contract_root,
        ).run()
    )
    report = load_json(outcome.report_path)
    run_root = outcome.report_path.parent
    panel_inputs_by_case: dict[str, tuple[dict[str, Any], Path]] = {}
    for packet_binding in report["judge_packets"]:
        packet_path = Path(packet_binding["ref"])
        panel_inputs = run_root / "panel" / "inputs" / packet_binding["case_id"]
        panel_inputs.mkdir(parents=True)
        assignment_path, judgments = _external_panel_inputs(panel_inputs, packet_path)
        panel_result, errors = aggregate(
            packet_path,
            outcome.report_path,
            judgments,
            assignment_path,
        )
        assert errors == []
        assert panel_result is not None
        panel_path = run_root / "panel" / "results" / f"{packet_binding['case_id']}.json"
        panel_inputs_by_case[packet_binding["case_id"]] = (panel_result, panel_path)

    def archived_ref(path: Path) -> str:
        return f"/output/{RUN_ID}/{path.relative_to(run_root).as_posix()}"

    for binding in report["evidence"]:
        binding["ref"] = archived_ref(Path(binding["ref"]))
    for binding in report["judge_packets"]:
        binding["ref"] = archived_ref(Path(binding["ref"]))
    report_sha = _write_json(outcome.report_path, report)

    panel_paths: list[Path] = []
    trusted: set[str] = set()
    for panel_result, panel_path in panel_inputs_by_case.values():
        panel_result["inputs"]["report_sha256"] = report_sha
        panel_result["inputs"]["assignment_ref"] = archived_ref(
            Path(panel_result["inputs"]["assignment_ref"])
        )
        for binding in panel_result["inputs"]["judgments"]:
            binding["ref"] = archived_ref(Path(binding["ref"]))
        panel_sha = _write_json(panel_path, panel_result)
        panel_paths.append(panel_path)
        trusted.add(panel_sha)
    gate, errors = finalize(
        outcome.report_path,
        panel_paths,
        trusted_panel_result_sha256=frozenset(trusted),
    )
    assert errors == []
    assert gate is not None
    gate["execution_report"]["ref"] = archived_ref(outcome.report_path)
    for binding in gate["panel_results"]:
        binding["ref"] = archived_ref(Path(binding["ref"]))
    _write_json(run_root / "quality-gate.json", gate)
    return tmp_path, trusted


def test_release_gate_accepts_a_semantically_verified_commit_bound_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    evidence_root, trusted = _valid_evidence(tmp_path, monkeypatch)

    assert (
        verify_release_gate(
            evidence_root,
            GIT_SHA,
            trusted_panel_result_sha256=frozenset(trusted),
        )
        == []
    )


def test_release_gate_rejects_a_schema_incomplete_synthetic_bundle(tmp_path: Path) -> None:
    _, _, _, panel_sha = _evidence(tmp_path)

    errors = verify_release_gate(
        tmp_path,
        GIT_SHA,
        trusted_panel_result_sha256=frozenset({panel_sha}),
    )

    assert any(error.startswith("semantic verification: execution report:") for error in errors)


def test_release_gate_rejects_a_different_release_commit(tmp_path: Path) -> None:
    _, _, _, panel_sha = _evidence(tmp_path)

    errors = verify_release_gate(
        tmp_path,
        "f" * 40,
        trusted_panel_result_sha256=frozenset({panel_sha}),
    )

    assert "execution report Git SHA does not match the release commit" in errors


def test_release_gate_rejects_tampered_bound_inputs(tmp_path: Path) -> None:
    _, report_path, panel_path, panel_sha = _evidence(tmp_path)
    report_path.write_text("{}\n", encoding="utf-8")
    panel_path.write_text("{}\n", encoding="utf-8")

    errors = verify_release_gate(
        tmp_path,
        GIT_SHA,
        trusted_panel_result_sha256=frozenset({panel_sha}),
    )

    assert "execution report SHA-256 mismatch" in errors
    assert f"panel result {RUN_ID}-REAL-001 SHA-256 mismatch" in errors


def test_release_gate_requires_one_quality_gate(tmp_path: Path) -> None:
    assert verify_release_gate(
        tmp_path,
        GIT_SHA,
        trusted_panel_result_sha256=frozenset({"a" * 64}),
    ) == ["release evidence must contain exactly one quality-gate.json; found 0"]


def test_release_gate_requires_external_panel_approval(tmp_path: Path) -> None:
    _evidence(tmp_path)

    assert verify_release_gate(
        tmp_path,
        GIT_SHA,
        trusted_panel_result_sha256=frozenset(),
    ) == ["publication has no externally approved panel-result SHA-256 digests"]
