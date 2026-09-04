"""Verify immutable release evidence before publishing an image."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker

from evals.judging.finalize import finalize

ROOT = Path(__file__).resolve().parent
_GIT_SHA = re.compile(r"^[a-f0-9]{40}$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_IMAGE_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")


def _load(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _schema_errors(value: Any) -> list[str]:
    validator: Any = Draft202012Validator(
        _load(ROOT / "judging" / "schemas" / "final-quality-gate.schema.json"),
        format_checker=FormatChecker(),
    )
    return [f"quality gate schema: {error.message}" for error in validator.iter_errors(value)]


def _bound_file(
    run_root: Path, run_id: str, ref: object, label: str
) -> tuple[Path | None, str | None]:
    if not isinstance(ref, str):
        return None, f"{label} ref is missing"
    parts = PurePosixPath(ref).parts
    positions = [index for index, part in enumerate(parts) if part == run_id]
    if len(positions) != 1 or positions[0] == len(parts) - 1:
        return None, f"{label} ref is not bound to run {run_id!r}"
    candidate = run_root.joinpath(*parts[positions[0] + 1 :]).resolve()
    try:
        candidate.relative_to(run_root.resolve())
    except ValueError:
        return None, f"{label} ref escapes the evidence run directory"
    if not candidate.is_file():
        return None, f"{label} file is missing"
    return candidate, None


def verify_release_gate(
    evidence_root: Path,
    expected_git_sha: str,
    *,
    trusted_panel_result_sha256: frozenset[str],
) -> list[str]:
    errors: list[str] = []
    if _GIT_SHA.fullmatch(expected_git_sha) is None:
        return ["expected Git SHA must contain exactly 40 lowercase hexadecimal characters"]
    if not trusted_panel_result_sha256:
        return ["publication has no externally approved panel-result SHA-256 digests"]
    invalid_trusted = sorted(
        value for value in trusted_panel_result_sha256 if _SHA256.fullmatch(value) is None
    )
    if invalid_trusted:
        return ["publication contains an invalid approved panel-result SHA-256 digest"]
    gate_paths = sorted(evidence_root.rglob("quality-gate.json"))
    if len(gate_paths) != 1:
        return [
            f"release evidence must contain exactly one quality-gate.json; found {len(gate_paths)}"
        ]
    gate_path = gate_paths[0]
    try:
        raw_gate: Any = _load(gate_path)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return [f"quality gate is unreadable: {exc}"]
    errors.extend(_schema_errors(raw_gate))
    if errors or not isinstance(raw_gate, dict):
        return errors
    gate = cast(dict[str, Any], raw_gate)
    run_id = cast(str, gate["run_id"])
    run_root = gate_path.parent.resolve()
    if run_root.name != run_id:
        errors.append("quality gate directory does not match its run_id")
    if gate["profile"] != "release":
        errors.append("quality gate profile is not release")
    if gate["status"] != "PASS" or gate["quality_status"] != "PASS":
        errors.append("quality gate did not pass")

    report_binding = cast(dict[str, Any], gate["execution_report"])
    report_path: Path | None = None
    report_path, error = _bound_file(
        run_root, run_id, report_binding.get("ref"), "execution report"
    )
    if error is not None:
        errors.append(error)
    elif report_path is not None:
        if _sha256(report_path) != report_binding["sha256"]:
            errors.append("execution report SHA-256 mismatch")
        else:
            try:
                raw_report: Any = _load(report_path)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                errors.append(f"execution report is unreadable: {exc}")
            else:
                if not isinstance(raw_report, dict):
                    errors.append("execution report is not an object")
                else:
                    report = cast(dict[str, Any], raw_report)
                    manifest = report.get("manifest")
                    if report.get("run_id") != run_id:
                        errors.append("execution report run_id does not match the quality gate")
                    if report.get("profile") != "release":
                        errors.append("execution report profile is not release")
                    if report.get("status") != "PASS":
                        errors.append("execution report did not pass")
                    if report.get("quality_status") != "PENDING_JUDGMENT":
                        errors.append("execution report was not finalized from pending judgment")
                    if not isinstance(manifest, dict):
                        errors.append("execution report manifest is missing")
                    else:
                        typed_manifest = cast(dict[str, Any], manifest)
                        if typed_manifest.get("git_sha") != expected_git_sha:
                            errors.append(
                                "execution report Git SHA does not match the release commit"
                            )
                        if typed_manifest.get("git_dirty") is not False:
                            errors.append(
                                "execution report was not produced from a clean source tree"
                            )
                    if report_binding["status"] != report.get("status"):
                        errors.append("quality gate execution status differs from its report")
                    if report_binding["quality_status"] != report.get("quality_status"):
                        errors.append("quality gate judgment status differs from its report")

    packet_ids: set[str] = set()
    panel_refs: set[str] = set()
    panel_paths: list[Path] = []
    panel_sha256: set[str] = set()
    for binding_value in gate["panel_results"]:
        binding = cast(dict[str, Any], binding_value)
        packet_id = cast(str, binding["packet_id"])
        if packet_id in packet_ids:
            errors.append(f"duplicate panel packet ID {packet_id!r}")
        packet_ids.add(packet_id)
        panel_sha256.add(cast(str, binding["sha256"]))
        ref = cast(str, binding["ref"])
        if ref in panel_refs:
            errors.append(f"duplicate panel result ref {ref!r}")
        panel_refs.add(ref)
        panel_path, panel_error = _bound_file(run_root, run_id, ref, f"panel result {packet_id}")
        if panel_error is not None:
            errors.append(panel_error)
            continue
        if panel_path is None:
            continue
        panel_paths.append(panel_path)
        if _sha256(panel_path) != binding["sha256"]:
            errors.append(f"panel result {packet_id} SHA-256 mismatch")
            continue
        try:
            raw_panel: Any = _load(panel_path)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            errors.append(f"panel result {packet_id} is unreadable: {exc}")
            continue
        if not isinstance(raw_panel, dict):
            errors.append(f"panel result {packet_id} is not an object")
            continue
        panel = cast(dict[str, Any], raw_panel)
        for field in ("packet_id", "status", "quality_status", "overall_score", "judge_count"):
            if panel.get(field) != binding[field]:
                errors.append(f"panel result {packet_id} differs from its quality gate on {field}")
        if panel.get("status") != "PASS" or panel.get("quality_status") != "PASS":
            errors.append(f"panel result {packet_id} did not pass")
    if panel_sha256 != trusted_panel_result_sha256:
        errors.append("quality gate panel digests differ from the externally approved digest set")
    if report_path is not None and len(panel_paths) == len(gate["panel_results"]):
        regenerated, semantic_errors = finalize(
            report_path,
            panel_paths,
            trusted_panel_result_sha256=trusted_panel_result_sha256,
        )
        errors.extend(f"semantic verification: {error}" for error in semantic_errors)
        if regenerated is not None:
            for field in (
                "schema_version",
                "run_id",
                "profile",
                "quality_status",
                "status",
                "reason",
            ):
                if regenerated[field] != gate[field]:
                    errors.append(
                        f"quality gate differs from deterministic finalization on {field}"
                    )
    if not errors and report_path is not None:
        report = cast(dict[str, Any], _load(report_path))
        digest = cast(dict[str, Any], report["manifest"]).get("app_image_digest")
        if (
            not isinstance(digest, str)
            or _IMAGE_DIGEST.fullmatch(digest) is None
            or digest == "sha256:" + "0" * 64
        ):
            errors.append("execution report lacks an immutable evaluated application image digest")
    return errors


def release_image_digest(evidence_root: Path) -> str:
    gate_path = next(evidence_root.rglob("quality-gate.json"))
    gate = cast(dict[str, Any], _load(gate_path))
    run_id = cast(str, gate["run_id"])
    report_path, error = _bound_file(
        gate_path.parent.resolve(),
        run_id,
        cast(dict[str, Any], gate["execution_report"])["ref"],
        "execution report",
    )
    if error is not None or report_path is None:
        raise ValueError(error or "execution report is unavailable")
    report = cast(dict[str, Any], _load(report_path))
    return cast(str, cast(dict[str, Any], report["manifest"])["app_image_digest"])


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence_root", type=Path)
    parser.add_argument("expected_git_sha")
    return parser


def main() -> int:
    args = _parser().parse_args()
    trusted = frozenset(
        value.strip()
        for value in os.environ.get("VERA_TRUSTED_PANEL_RESULT_SHA256", "").split(",")
        if value.strip()
    )
    errors = verify_release_gate(
        args.evidence_root,
        args.expected_git_sha,
        trusted_panel_result_sha256=trusted,
    )
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print(release_image_digest(args.evidence_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
