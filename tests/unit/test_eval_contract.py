from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from evals.validate import (
    ROOT,
    dataset_sha256,
    load_json,
    load_jsonl,
    sha256_file,
    sha256_json,
    validate_contracts,
    validate_report,
)


def _status_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(result["status"] for result in results)
    return {
        "selected": len(results),
        "pass": counts["PASS"],
        "fail": counts["FAIL"],
        "blocked": counts["BLOCKED"],
        "not_applicable": counts["NOT_APPLICABLE"],
    }


def _build_report(
    profile: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    checks = load_json(ROOT / "checklist.json")["items"]
    cases = load_jsonl(ROOT / "scenarios" / "core.jsonl")
    selected_checks = [check for check in checks if profile in check["profiles"]]
    selected_cases = [case for case in cases if profile in case["profiles"]]
    evidence_id = "ev-contract"
    execution_profiles = [{"profile_id": "test", "dimensions": {"runner": "unit"}}]
    selection = {
        "check_ids": [check["id"] for check in selected_checks],
        "case_ids": [case["case_id"] for case in selected_cases],
    }

    check_results = [
        {
            "check_id": check["id"],
            "status": "PASS",
            "evidence_ids": [evidence_id],
            "notes": "synthetic validator fixture",
            "blocked_reason": None,
        }
        for check in selected_checks
    ]
    evidence_labels = sorted(
        {label for check in selected_checks for label in check["evidence"]}
        | {
            label
            for case in selected_cases
            for assertion in case["assertions"]
            for label in assertion["evidence"]
        }
    )
    case_results: list[dict[str, Any]] = []
    for case in selected_cases:
        assertion_results = []
        for assertion in case["assertions"]:
            status = "NOT_APPLICABLE" if assertion["activation"] == "baseline_required" else "PASS"
            assertion_results.append(
                {
                    "assertion_id": assertion["id"],
                    "status": status,
                    "target": assertion["target"],
                    "expected": assertion["expected"],
                    "observed": assertion["expected"] if status == "PASS" else None,
                    "evidence_ids": [evidence_id] if status == "PASS" else [],
                    "notes": "synthetic validator fixture",
                }
            )
        case_results.append(
            {
                "case_id": case["case_id"],
                "status": "PASS",
                "quality_status": "NOT_REQUESTED",
                "duration_ms": 1,
                "assertion_results": assertion_results,
                "metric_ids": [],
                "evidence_ids": [evidence_id],
                "first_bad_boundary": None,
                "root_cause_confidence": 0,
                "blocked_reason": None,
            }
        )

    report = {
        "schema_version": "1.0",
        "run_id": f"test-{profile}",
        "profile": profile,
        "status": "PASS",
        "quality_status": "NOT_REQUESTED",
        "blocked_prerequisites": [],
        "manifest": {
            "started_at": "2026-08-28T00:00:00Z",
            "ended_at": "2026-08-28T00:01:00Z",
            "environment": "unit-test",
            "service_version": "0.1.0",
            "git_sha": "0123456789abcdef0123456789abcdef01234567",
            "git_dirty": True,
            **({"app_image_digest": "sha256:" + "a" * 64} if profile == "release" else {}),
            "dataset_sha256": dataset_sha256(),
            "checklist_sha256": sha256_file(ROOT / "checklist.json"),
            "action_catalog_sha256": sha256_file(ROOT / "action_catalog.json"),
            "execution_profile_sha256": sha256_json(execution_profiles),
            "selection_sha256": sha256_json(
                {
                    "check_ids": sorted(selection["check_ids"]),
                    "case_ids": sorted(selection["case_ids"]),
                }
            ),
            "config_fingerprint": "0" * 64,
            "baseline": None,
            "graph_backend": "neo4j",
            "hardware_profile": "unit-test",
            "cache_state": "disabled",
            "concurrency": 1,
            "random_seed": 20260828,
            "execution_profiles": execution_profiles,
            "models": {},
            "pipeline_versions": {},
            "ontology_version": "1",
            "evaluator": {
                "kind": "agent",
                "name": "contract-test",
                "version": "1",
                "rubric_version": "1",
            },
            "adapter": {
                "kind": "unit-test",
                "executable": None,
                "executable_sha256": None,
                "arguments_sha256": None,
                "capabilities": [],
                "timeout_s": None,
            },
        },
        "selection": selection,
        "gate": {
            "passed": True,
            "reason": "all gating results pass",
            "check_status_counts": _status_counts(check_results),
            "case_status_counts": _status_counts(case_results),
        },
        "metrics": [],
        "check_results": check_results,
        "case_results": case_results,
        "findings": [],
        "cleanup": {
            "status": "PASS",
            "created_resources": [],
            "removed_resources": [],
            "remaining_resources": [],
            "notes": "nothing created",
        },
        "evidence": [
            {
                "evidence_id": evidence_id,
                "labels": evidence_labels,
                "kind": "file",
                "ref": "evals/README.md",
                "sha256": sha256_file(ROOT / "README.md"),
                "redacted": True,
            }
        ],
        "judge_packets": [],
        "artifact_refs": [],
    }
    return report, checks, cases


def _write_report(tmp_path: Path, report: dict[str, Any]) -> Path:
    evidence_path = tmp_path / "evidence" / "contract.md"
    evidence_path.parent.mkdir()
    evidence_path.write_bytes((ROOT / "README.md").read_bytes())
    report["evidence"][0]["ref"] = str(evidence_path)
    report["evidence"][0]["sha256"] = sha256_file(evidence_path)
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return path


def _attach_baseline(tmp_path: Path, report: dict[str, Any], *, profile: str | None = None) -> None:
    manifest = report["manifest"]
    compatibility_fields = (
        "dataset_sha256",
        "checklist_sha256",
        "action_catalog_sha256",
        "execution_profile_sha256",
        "selection_sha256",
        "config_fingerprint",
        "service_version",
        "graph_backend",
        "hardware_profile",
        "cache_state",
        "concurrency",
        "models",
        "pipeline_versions",
        "ontology_version",
    )
    compatibility = {field: manifest[field] for field in compatibility_fields}
    compatibility["profile"] = profile or report["profile"]
    baseline = {
        "schema_version": "1.0",
        "baseline_id": "baseline-test",
        "promoted_at": "2026-08-28T00:00:00Z",
        "promoted_by": "contract-test",
        "source_run_uri": "artifact://test/source-report.json",
        "source_report_sha256": "f" * 64,
        "compatibility": compatibility,
        "metrics": [],
        "threshold_policies": [],
    }
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    manifest["baseline"] = {
        "baseline_id": "baseline-test",
        "uri": str(baseline_path),
        "sha256": sha256_file(baseline_path),
    }


def test_eval_contracts_validate() -> None:
    errors, case_count, check_count, action_count = validate_contracts()
    assert errors == []
    assert (case_count, check_count, action_count) == (46, 78, 35)


def test_complete_daily_report_validates(tmp_path: Path) -> None:
    report, checks, cases = _build_report("daily")
    assert validate_report(_write_report(tmp_path, report), checks, cases) == []


def test_missing_assertion_result_is_rejected(tmp_path: Path) -> None:
    report, checks, cases = _build_report("daily")
    report["case_results"][0]["assertion_results"].pop()
    errors = validate_report(_write_report(tmp_path, report), checks, cases)
    assert any("assertion results do not match" in error for error in errors)


def test_non_gating_gap_failure_does_not_fail_run(tmp_path: Path) -> None:
    report, checks, cases = _build_report("release")
    result = next(item for item in report["case_results"] if item["case_id"] == "ING-003")
    result["assertion_results"][0]["status"] = "FAIL"
    result["assertion_results"][0]["observed"] = False
    result["status"] = "FAIL"
    result["first_bad_boundary"] = "L6_INGESTION"
    report["findings"] = [
        {
            "finding_id": "F-ING-003",
            "severity": "HIGH",
            "title": "Raw artifact re-extraction is unavailable",
            "first_bad_boundary": "L6_INGESTION",
            "case_ids": ["ING-003"],
            "check_ids": ["ING-005"],
            "expected": "Stored raw artifacts can be re-extracted.",
            "observed": "No raw-artifact re-extraction capability exists.",
            "impact": "Knowledge fetched during extractor outage remains unusable.",
            "evidence_ids": ["ev-contract"],
            "reproduced_count": 1,
            "root_cause_hypothesis": "Reprocess only replays published episodes.",
            "confidence": 1,
            "recommended_action": "Add a versioned raw-artifact re-extraction path.",
            "verification_check_ids": ["ING-005"],
        }
    ]
    report["gate"]["case_status_counts"] = _status_counts(report["case_results"])
    assert validate_report(_write_report(tmp_path, report), checks, cases) == []


def test_gating_failure_requires_failed_run(tmp_path: Path) -> None:
    report, checks, cases = _build_report("daily")
    result = next(item for item in report["case_results"] if item["case_id"] == "E2E-001")
    result["assertion_results"][0]["status"] = "FAIL"
    result["assertion_results"][0]["observed"] = False
    result["status"] = "FAIL"
    report["gate"]["passed"] = False
    report["gate"]["case_status_counts"] = _status_counts(report["case_results"])
    errors = validate_report(_write_report(tmp_path, report), checks, cases)
    assert any("run status should be FAIL" in error for error in errors)


def test_cleanup_leftovers_are_rejected(tmp_path: Path) -> None:
    report, checks, cases = _build_report("daily")
    report["cleanup"]["created_resources"] = ["scope:test"]
    report["cleanup"]["remaining_resources"] = ["scope:test"]
    errors = validate_report(_write_report(tmp_path, report), checks, cases)
    assert any("PASS cleanup has leftovers" in error for error in errors)


def test_selected_p0_check_cannot_be_not_applicable(tmp_path: Path) -> None:
    report, checks, cases = _build_report("daily")
    result = next(item for item in report["check_results"] if item["check_id"] == "PRE-003")
    result["status"] = "NOT_APPLICABLE"
    result["evidence_ids"] = []
    report["gate"]["check_status_counts"] = _status_counts(report["check_results"])
    errors = validate_report(_write_report(tmp_path, report), checks, cases)
    assert any("selected P0 check PRE-003 cannot be N/A" in error for error in errors)


def test_compatible_local_baseline_validates(tmp_path: Path) -> None:
    report, checks, cases = _build_report("daily")
    _attach_baseline(tmp_path, report)
    assert validate_report(_write_report(tmp_path, report), checks, cases) == []


def test_baseline_from_another_profile_is_rejected(tmp_path: Path) -> None:
    report, checks, cases = _build_report("daily")
    _attach_baseline(tmp_path, report, profile="weekly")
    errors = validate_report(_write_report(tmp_path, report), checks, cases)
    assert any("baseline is incompatible on profile" in error for error in errors)
