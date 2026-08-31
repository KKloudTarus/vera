"""Finalize one immutable VERA execution report with its panel results."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from evals.judging import aggregate as aggregate_module

ROOT = Path(__file__).resolve().parent


def _load(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _is_run_owned(path: Path, run_root: Path) -> bool:
    return path.resolve().is_relative_to(run_root)


def _schema_errors(value: Any, schema_name: str, label: str) -> list[str]:
    validator = Draft202012Validator(
        _load(ROOT / "schemas" / schema_name), format_checker=FormatChecker()
    )
    return [f"{label}: schema: {error.message}" for error in validator.iter_errors(value)]


def finalize(
    report_path: Path,
    panel_result_paths: list[Path],
    *,
    trusted_panel_result_sha256: frozenset[str],
) -> tuple[dict[str, Any] | None, list[str]]:
    try:
        report = _load(report_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return None, [f"unreadable execution report: {exc}"]
    errors = [
        f"execution report: {error}"
        for error in aggregate_module._execution_report_errors(report_path)
    ]
    if report.get("quality_status") != "PENDING_JUDGMENT":
        errors.append("execution report is not pending external judgment")
    if (
        report.get("profile") == "release"
        and report.get("manifest", {}).get("git_dirty") is not False
    ):
        errors.append("release quality cannot be finalized from a dirty source tree")
    packet_refs = report.get("judge_packets")
    if not isinstance(packet_refs, list) or not packet_refs:
        errors.append("execution report has no judge packets")
        packet_refs = []
    expected_packets = {
        item.get("packet_id"): item
        for item in packet_refs
        if isinstance(item, dict) and isinstance(item.get("packet_id"), str)
    }
    if len(expected_packets) != len(packet_refs):
        errors.append("execution report has invalid or duplicate packet IDs")

    report_path = report_path.resolve()
    run_root = report_path.parent
    report_sha256 = _sha256(report_path)
    results_by_packet: dict[str, tuple[dict[str, Any], Path]] = {}
    for path in panel_result_paths:
        path = path.resolve()
        if not _is_run_owned(path, run_root):
            errors.append(f"{path}: panel result is outside the immutable run directory")
            continue
        try:
            result = _load(path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{path}: unreadable panel result: {exc}")
            continue
        schema_errors = _schema_errors(result, "panel-result.schema.json", str(path))
        if schema_errors:
            errors.extend(schema_errors)
            continue
        if result.get("schema_version") != "1.1":
            errors.append(f"{path}: panel result lacks transitive input bindings")
            continue
        panel_result_sha256 = _sha256(path)
        if panel_result_sha256 not in trusted_panel_result_sha256:
            errors.append(f"{path}: panel result SHA-256 is not externally trusted")
        packet_id = result["packet_id"]
        if packet_id in results_by_packet:
            errors.append(f"duplicate panel result for {packet_id}")
            continue
        packet_ref = expected_packets.get(packet_id)
        if packet_ref is None:
            errors.append(f"panel result references unknown packet {packet_id}")
            continue
        if result["inputs"]["report_sha256"] != report_sha256:
            errors.append(f"{packet_id}: execution report SHA-256 mismatch")
        if result["inputs"]["packet_sha256"] != packet_ref.get("sha256"):
            errors.append(f"{packet_id}: judge packet SHA-256 mismatch")
        assignment_path = Path(result["inputs"]["assignment_ref"]).resolve()
        assignment_sha256 = result["inputs"]["assignment_sha256"]
        if not _is_run_owned(assignment_path, run_root):
            errors.append(f"{packet_id}: assignment is outside the immutable run directory")
            continue
        try:
            actual_assignment_sha256 = _sha256(assignment_path)
        except OSError:
            errors.append(f"{packet_id}: assignment file is unavailable")
            continue
        if actual_assignment_sha256 != assignment_sha256:
            errors.append(f"{packet_id}: assignment SHA-256 mismatch")
        judgment_paths: list[Path] = []
        for binding in result["inputs"]["judgments"]:
            judgment_path = Path(binding["ref"]).resolve()
            judgment_paths.append(judgment_path)
            if not _is_run_owned(judgment_path, run_root):
                errors.append(
                    f"{packet_id}: judgment is outside the immutable run directory "
                    f"for {binding['actor_id']}"
                )
                continue
            try:
                actual_judgment_sha256 = _sha256(judgment_path)
            except OSError:
                errors.append(
                    f"{packet_id}: judgment file is unavailable for {binding['actor_id']}"
                )
                continue
            if actual_judgment_sha256 != binding["sha256"]:
                errors.append(f"{packet_id}: judgment SHA-256 mismatch for {binding['actor_id']}")
        packet_path = Path(str(packet_ref["ref"])).resolve()
        if not _is_run_owned(packet_path, run_root):
            errors.append(f"{packet_id}: judge packet is outside the immutable run directory")
            continue
        regenerated, aggregate_errors = aggregate_module.aggregate(
            packet_path,
            report_path,
            judgment_paths,
            assignment_path,
        )
        if aggregate_errors:
            errors.extend(f"{packet_id}: {error}" for error in aggregate_errors)
        elif regenerated != result:
            errors.append(f"{packet_id}: panel result differs from deterministic aggregation")
        expected_status = aggregate_module._combine_status(
            result["hard_gate_status"], result["quality_status"]
        )
        if result["status"] != expected_status:
            errors.append(f"{packet_id}: panel status is internally inconsistent")
        results_by_packet[packet_id] = (result, path)
    missing_packets = sorted(set(expected_packets) - set(results_by_packet))
    if missing_packets:
        errors.append(f"missing panel results for {missing_packets}")
    if errors:
        return None, errors

    panel_results: list[dict[str, Any]] = []
    quality_status = "PASS"
    for packet_id in sorted(results_by_packet):
        result, path = results_by_packet[packet_id]
        if result["status"] == "FAIL" or result["quality_status"] == "FAIL":
            quality_status = "FAIL"
        elif quality_status == "PASS" and result["status"] != "PASS":
            quality_status = "BLOCKED"
        panel_results.append(
            {
                "packet_id": packet_id,
                "ref": str(path.resolve()),
                "sha256": _sha256(path),
                "status": result["status"],
                "quality_status": result["quality_status"],
                "overall_score": result["overall_score"],
                "judge_count": result["judge_count"],
            }
        )
    combined_quality = "DISPUTED" if quality_status == "BLOCKED" else quality_status
    status = aggregate_module._combine_status(report["status"], combined_quality)
    finalized = {
        "schema_version": "1.0",
        "run_id": report["run_id"],
        "profile": report["profile"],
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "execution_report": {
            "ref": str(report_path.resolve()),
            "sha256": report_sha256,
            "status": report["status"],
            "quality_status": report["quality_status"],
        },
        "panel_results": panel_results,
        "quality_status": quality_status,
        "status": status,
        "reason": (
            "execution gates and every bound panel result pass"
            if status == "PASS"
            else "execution gates or bound panel quality did not pass"
        ),
    }
    schema_errors = _schema_errors(
        finalized, "final-quality-gate.schema.json", "final quality gate"
    )
    return (None, schema_errors) if schema_errors else (finalized, [])


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("panel_results", type=Path, nargs="+")
    parser.add_argument(
        "--trusted-panel-result-sha256",
        action="append",
        required=True,
        help="externally approved panel-result digest; repeat for each panel",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result, errors = finalize(
        args.report,
        args.panel_results,
        trusted_panel_result_sha256=frozenset(args.trusted_panel_result_sha256),
    )
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    if result is None:
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    try:
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(payload)
    except FileExistsError:
        print(f"refusing to overwrite immutable quality gate: {args.output}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
