"""Finalize one immutable VERA execution report with its panel results."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
import tempfile
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from evals.judging import aggregate as aggregate_module

ROOT = Path(__file__).resolve().parent


def _load(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


@dataclass(frozen=True, slots=True)
class _JsonSnapshot:
    path: Path
    data: bytes
    value: Any
    sha256: str


def _snapshot(path: Path) -> _JsonSnapshot:
    resolved = path.resolve()
    data = resolved.read_bytes()
    return _JsonSnapshot(
        path=resolved,
        data=data,
        value=json.loads(data.decode("utf-8")),
        sha256=hashlib.sha256(data).hexdigest(),
    )


@contextmanager
def _materialize(snapshot: _JsonSnapshot, run_root: Path) -> Iterator[Path]:
    with tempfile.NamedTemporaryFile(
        dir=run_root, prefix=".finalize-snapshot-", suffix=".json", delete=False
    ) as handle:
        handle.write(snapshot.data)
        temporary = Path(handle.name)
    try:
        yield temporary
    finally:
        temporary.unlink(missing_ok=True)


def _changed_inputs(snapshots: dict[Path, _JsonSnapshot]) -> list[str]:
    changed: list[str] = []
    for path, snapshot in snapshots.items():
        try:
            if path.read_bytes() != snapshot.data:
                changed.append(str(path))
        except OSError:
            changed.append(str(path))
    return changed


def _is_run_owned(path: Path, run_root: Path) -> bool:
    return path.resolve().is_relative_to(run_root)


def _resolve_run_owned_ref(value: object, run_root: Path, run_id: str) -> Path | None:
    if not isinstance(value, str) or not value:
        return None
    direct = Path(value).resolve()
    if _is_run_owned(direct, run_root):
        return direct
    parts = Path(value).parts
    positions = [index for index, part in enumerate(parts) if part == run_id]
    if len(positions) != 1 or positions[0] == len(parts) - 1:
        return None
    archived = run_root.joinpath(*parts[positions[0] + 1 :]).resolve()
    return archived if _is_run_owned(archived, run_root) else None


def _remapped_report_snapshot(
    report_snapshot: _JsonSnapshot, run_root: Path, run_id: str
) -> _JsonSnapshot:
    report = copy.deepcopy(report_snapshot.value)
    evidence = report.get("evidence")
    if isinstance(evidence, list):
        for binding in evidence:
            if isinstance(binding, dict):
                path = _resolve_run_owned_ref(binding.get("ref"), run_root, run_id)
                if path is not None:
                    binding["ref"] = str(path)
    judge_packets = report.get("judge_packets")
    if isinstance(judge_packets, list):
        for binding in judge_packets:
            if isinstance(binding, dict):
                path = _resolve_run_owned_ref(binding.get("ref"), run_root, run_id)
                if path is not None:
                    binding["ref"] = str(path)
    data = (json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()
    return _JsonSnapshot(
        path=report_snapshot.path,
        data=data,
        value=report,
        sha256=hashlib.sha256(data).hexdigest(),
    )


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
    report_path = report_path.resolve()
    run_root = report_path.parent
    snapshots: dict[Path, _JsonSnapshot] = {}

    def snapshot(path: Path) -> _JsonSnapshot:
        resolved = path.resolve()
        existing = snapshots.get(resolved)
        if existing is None:
            existing = _snapshot(resolved)
            snapshots[resolved] = existing
        return existing

    try:
        report_snapshot = snapshot(report_path)
        report = report_snapshot.value
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        return None, [f"unreadable execution report: {exc}"]
    errors: list[str] = []
    if not isinstance(report, dict):
        return None, [*errors, "execution report is not an object"]
    run_id = report.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        return None, [*errors, "execution report run_id is invalid"]
    for label in ("evidence", "judge_packets"):
        bindings = report.get(label)
        if not isinstance(bindings, list):
            continue
        for binding in bindings:
            ref = binding.get("ref") if isinstance(binding, dict) else None
            path = _resolve_run_owned_ref(ref, run_root, run_id)
            if path is None:
                continue
            try:
                snapshot(path)
            except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
                errors.append(f"execution report {label} input is unavailable: {ref}")
    remapped_report_snapshot = _remapped_report_snapshot(report_snapshot, run_root, run_id)
    with _materialize(remapped_report_snapshot, run_root) as report_copy:
        errors = [
            f"execution report: {error}"
            for error in aggregate_module._execution_report_errors(report_copy)
        ]
    if report.get("quality_status") != "PENDING_JUDGMENT":
        errors.append("execution report is not pending external judgment")
    manifest = report.get("manifest")
    if report.get("profile") == "release" and (
        not isinstance(manifest, dict) or manifest.get("git_dirty") is not False
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

    report_sha256 = report_snapshot.sha256
    results_by_packet: dict[str, tuple[dict[str, Any], Path, str]] = {}
    for path in panel_result_paths:
        path = path.resolve()
        if not _is_run_owned(path, run_root):
            errors.append(f"{path}: panel result is outside the immutable run directory")
            continue
        try:
            result_snapshot = snapshot(path)
            result = result_snapshot.value
        except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{path}: unreadable panel result: {exc}")
            continue
        schema_errors = _schema_errors(result, "panel-result.schema.json", str(path))
        if schema_errors:
            errors.extend(schema_errors)
            continue
        if result.get("schema_version") != "1.1":
            errors.append(f"{path}: panel result lacks transitive input bindings")
            continue
        panel_result_sha256 = result_snapshot.sha256
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
        assignment_path = _resolve_run_owned_ref(
            result["inputs"]["assignment_ref"], run_root, run_id
        )
        assignment_sha256 = result["inputs"]["assignment_sha256"]
        if assignment_path is None:
            errors.append(f"{packet_id}: assignment is outside the immutable run directory")
            continue
        try:
            assignment_snapshot = snapshot(assignment_path)
            actual_assignment_sha256 = assignment_snapshot.sha256
        except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
            errors.append(f"{packet_id}: assignment file is unavailable")
            continue
        if actual_assignment_sha256 != assignment_sha256:
            errors.append(f"{packet_id}: assignment SHA-256 mismatch")
        judgment_snapshots: list[_JsonSnapshot] = []
        for binding in result["inputs"]["judgments"]:
            judgment_path = _resolve_run_owned_ref(binding["ref"], run_root, run_id)
            if judgment_path is None:
                errors.append(
                    f"{packet_id}: judgment is outside the immutable run directory "
                    f"for {binding['actor_id']}"
                )
                continue
            try:
                judgment_snapshot = snapshot(judgment_path)
                actual_judgment_sha256 = judgment_snapshot.sha256
                judgment_snapshots.append(judgment_snapshot)
            except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
                errors.append(
                    f"{packet_id}: judgment file is unavailable for {binding['actor_id']}"
                )
                continue
            if actual_judgment_sha256 != binding["sha256"]:
                errors.append(f"{packet_id}: judgment SHA-256 mismatch for {binding['actor_id']}")
        packet_path = _resolve_run_owned_ref(packet_ref["ref"], run_root, run_id)
        if packet_path is None:
            errors.append(f"{packet_id}: judge packet is outside the immutable run directory")
            continue
        try:
            packet_snapshot = snapshot(packet_path)
        except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
            errors.append(f"{packet_id}: judge packet is unavailable")
            continue
        with ExitStack() as stack:
            report_copy = stack.enter_context(_materialize(remapped_report_snapshot, run_root))
            packet_copy = stack.enter_context(_materialize(packet_snapshot, run_root))
            assignment_copy = stack.enter_context(_materialize(assignment_snapshot, run_root))
            judgment_copies = [
                stack.enter_context(_materialize(item, run_root)) for item in judgment_snapshots
            ]
            regenerated, aggregate_errors = aggregate_module.aggregate(
                packet_copy,
                report_copy,
                judgment_copies,
                assignment_copy,
            )
            if regenerated is not None:
                regenerated["inputs"]["report_sha256"] = report_sha256
                regenerated["inputs"]["assignment_ref"] = result["inputs"]["assignment_ref"]
                original_judgments = {
                    item["actor_id"]: item["ref"] for item in result["inputs"]["judgments"]
                }
                for binding in regenerated["inputs"]["judgments"]:
                    binding["ref"] = original_judgments[binding["actor_id"]]
        if aggregate_errors:
            errors.extend(f"{packet_id}: {error}" for error in aggregate_errors)
        elif regenerated != result:
            errors.append(f"{packet_id}: panel result differs from deterministic aggregation")
        expected_status = aggregate_module._combine_status(
            result["hard_gate_status"], result["quality_status"]
        )
        if result["status"] != expected_status:
            errors.append(f"{packet_id}: panel status is internally inconsistent")
        results_by_packet[packet_id] = (result, path, panel_result_sha256)
    missing_packets = sorted(set(expected_packets) - set(results_by_packet))
    if missing_packets:
        errors.append(f"missing panel results for {missing_packets}")
    changed_inputs = _changed_inputs(snapshots)
    if changed_inputs:
        errors.append(f"finalization inputs changed while being verified: {changed_inputs}")
    if errors:
        return None, errors

    panel_results: list[dict[str, Any]] = []
    quality_status = "PASS"
    for packet_id in sorted(results_by_packet):
        result, path, panel_result_sha256 = results_by_packet[packet_id]
        if result["status"] == "FAIL" or result["quality_status"] == "FAIL":
            quality_status = "FAIL"
        elif quality_status == "PASS" and result["status"] != "PASS":
            quality_status = "BLOCKED"
        panel_results.append(
            {
                "packet_id": packet_id,
                "ref": str(path.resolve()),
                "sha256": panel_result_sha256,
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
    changed_inputs = _changed_inputs(snapshots)
    if changed_inputs:
        return None, [f"finalization inputs changed while being finalized: {changed_inputs}"]
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
