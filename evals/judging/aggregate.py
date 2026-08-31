"""Validate independent judgments and aggregate one VERA panel result."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parent
EVAL_ROOT = ROOT.parent
PROJECT_ROOT = EVAL_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from evals.validate import load_cases, load_json, sha256_json, validate_report  # noqa: E402


def _load(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalized_identifier(value: str) -> str:
    return value.strip().casefold()


def _schema_errors(value: Any, schema_name: str, label: str) -> list[str]:
    validator = Draft202012Validator(
        _load(ROOT / "schemas" / schema_name), format_checker=FormatChecker()
    )
    return [f"{label}: schema: {error.message}" for error in validator.iter_errors(value)]


def _execution_report_errors(report_path: Path) -> list[str]:
    checks = load_json(EVAL_ROOT / "checklist.json")["items"]
    cases = load_cases()
    try:
        report = load_json(report_path)
        run_id = report.get("run_id")
        if isinstance(run_id, str) and run_id.endswith("-slice"):
            selection = report.get("selection")
            if not isinstance(selection, dict):
                return ["slice report lacks a selection object"]
            check_ids = selection.get("check_ids")
            case_ids = selection.get("case_ids")
            if not isinstance(check_ids, list) or not all(
                isinstance(item, str) for item in check_ids
            ):
                return ["slice report check selection is invalid"]
            if not isinstance(case_ids, list) or not all(
                isinstance(item, str) for item in case_ids
            ):
                return ["slice report case selection is invalid"]
            checks_by_id = {item["id"]: item for item in checks}
            cases_by_id = {item["case_id"]: item for item in cases}
            unknown_checks = sorted(set(check_ids) - set(checks_by_id))
            unknown_cases = sorted(set(case_ids) - set(cases_by_id))
            if unknown_checks or unknown_cases:
                return [
                    "slice report selects unknown contracts: "
                    f"checks={unknown_checks}, cases={unknown_cases}"
                ]
            profile = report.get("profile")
            if any(profile not in checks_by_id[item]["profiles"] for item in check_ids) or any(
                profile not in cases_by_id[item]["profiles"] for item in case_ids
            ):
                return ["slice report selects a contract outside its profile"]
            checks = [checks_by_id[item] for item in check_ids]
            cases = [cases_by_id[item] for item in case_ids]
        return validate_report(report_path, checks, cases)
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        return [f"report semantic validation failed: {exc}"]


def _combine_status(hard_gate_status: str, quality_status: str) -> str:
    if hard_gate_status == "FAIL" or quality_status == "FAIL":
        return "FAIL"
    if hard_gate_status == "BLOCKED" or quality_status in {
        "DISPUTED",
        "INSUFFICIENT_PANEL",
    }:
        return "BLOCKED"
    return "PASS"


def aggregate(
    packet_path: Path,
    report_path: Path,
    judgment_paths: list[Path],
    assignment_path: Path,
) -> tuple[dict[str, Any] | None, list[str]]:
    try:
        packet = _load(packet_path)
        report = _load(report_path)
        assignment = _load(assignment_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return None, [f"unreadable panel input: {exc}"]

    errors = [f"execution report: {error}" for error in _execution_report_errors(report_path)]
    packet_sha256 = _sha256(packet_path)
    report_sha256 = _sha256(report_path)
    assignment_sha256 = _sha256(assignment_path)
    errors.extend(_schema_errors(packet, "judge-packet.schema.json", "packet"))
    errors.extend(_schema_errors(assignment, "panel-assignment.schema.json", "assignment"))
    if errors:
        return None, errors
    errors.extend(_schema_errors(packet.get("rubric"), "rubric.schema.json", "rubric"))
    errors.extend(
        _schema_errors(packet.get("panel_policy"), "panel-policy.schema.json", "panel policy")
    )
    if errors:
        return None, errors

    source_material = packet.get("source_material", {})
    documents = source_material.get("documents")
    if not (
        isinstance(documents, list)
        and source_material.get("document_count") == len(documents)
        and source_material.get("sha256") == sha256_json(documents)
    ):
        errors.append("packet source-material digest mismatch")

    packet_ref = next(
        (
            item
            for item in report.get("judge_packets", [])
            if item.get("packet_id") == packet.get("packet_id")
        ),
        None,
    )
    if packet_ref is None:
        errors.append("execution report does not reference this judge packet")
    elif (
        packet_ref.get("case_id") != packet.get("run", {}).get("case_id")
        or packet_ref.get("sha256") != packet_sha256
    ):
        errors.append("execution report judge packet binding mismatch")
    if packet.get("run", {}).get("run_id") != report.get("run_id"):
        errors.append("execution report run_id does not match judge packet")
    if (
        assignment.get("packet_id") != packet.get("packet_id")
        or assignment.get("packet_sha256") != packet_sha256
    ):
        errors.append("panel assignment does not bind to the judge packet")

    rubric = packet.get("rubric", {})
    dimensions = rubric.get("dimensions", []) if isinstance(rubric, dict) else []
    dimension_ids = [item.get("id") for item in dimensions if isinstance(item, dict)]
    weights = [item.get("weight") for item in dimensions if isinstance(item, dict)]
    if (
        len(dimension_ids) != len(dimensions)
        or len(dimension_ids) != len(set(dimension_ids))
        or len(weights) != len(dimensions)
        or not all(
            isinstance(weight, (int, float)) and not isinstance(weight, bool) for weight in weights
        )
        or abs(sum(weights) - 1.0) > 1e-6
    ):
        errors.append("rubric dimensions or weights are invalid")

    assigned_judges = assignment.get("judges", [])
    assignment_ids = [
        _normalized_identifier(item.get("actor_id", ""))
        for item in assigned_judges
        if isinstance(item, dict)
    ]
    if len(assignment_ids) != len(set(assignment_ids)):
        errors.append("panel assignment has duplicate actor IDs")
    assignments_by_actor = {
        _normalized_identifier(item["actor_id"]): item
        for item in assigned_judges
        if isinstance(item, dict) and isinstance(item.get("actor_id"), str)
    }

    judgments: list[dict[str, Any]] = []
    judgment_bindings: list[dict[str, str]] = []
    for path in judgment_paths:
        try:
            judgment = _load(path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{path}: unreadable judgment: {exc}")
            continue
        judgment_schema_errors = _schema_errors(judgment, "judgment.schema.json", str(path))
        if judgment_schema_errors:
            errors.extend(judgment_schema_errors)
            continue
        if judgment.get("packet_id") != packet.get("packet_id"):
            errors.append(f"{path}: packet_id mismatch")
        if judgment.get("packet_sha256") != packet_sha256:
            errors.append(f"{path}: packet SHA-256 mismatch")
        judge = judgment["judge"]
        actor_key = _normalized_identifier(judge["actor_id"])
        assigned = assignments_by_actor.get(actor_key)
        identity_fields = (
            "actor_id",
            "role",
            "provider",
            "model_family",
            "model_id",
            "version",
        )
        if assigned is None:
            errors.append(f"{path}: judge is not in the trusted panel assignment")
        elif any(judge[field] != assigned[field] for field in identity_fields):
            errors.append(f"{path}: judge identity differs from trusted assignment")
        judgments.append(judgment)
        judgment_bindings.append(
            {
                "actor_id": judge["actor_id"],
                "ref": str(path.resolve()),
                "sha256": _sha256(path),
            }
        )

    actor_ids = [_normalized_identifier(item["judge"]["actor_id"]) for item in judgments]
    if len(actor_ids) != len(set(actor_ids)):
        errors.append("duplicate judge actor_id")
    if errors:
        return None, errors

    dimension_ids_set = set(dimension_ids)
    weights_by_id = {item["id"]: float(item["weight"]) for item in dimensions}
    applicability = {item["id"]: item["applicability"] for item in dimensions}
    critical_rule_ids = {item["id"] for item in rubric.get("critical_failure_rules", [])}
    reference_ids = [item["ref"] for item in packet["reference_catalog"]]
    if len(reference_ids) != len(set(reference_ids)):
        return None, ["packet reference catalog has duplicate IDs"]
    known_references = set(reference_ids)
    for judgment in judgments:
        actor_id = judgment["judge"]["actor_id"]
        results = judgment["dimension_results"]
        if set(results) != dimension_ids_set:
            errors.append(f"{actor_id}: dimension set mismatch")
            continue
        applicable_scores: dict[str, float] = {}
        for dimension_id, result in results.items():
            status = result["status"]
            score = result["score"]
            if applicability[dimension_id] == "always" and status != "SCORED":
                errors.append(f"{actor_id}: always-applicable dimension is N/A")
            unknown_refs = set(result["evidence_refs"]) - known_references
            if unknown_refs:
                errors.append(f"{actor_id}: unknown evidence refs {sorted(unknown_refs)}")
            if status == "SCORED":
                applicable_scores[dimension_id] = float(score)
        applicable_weight = sum(weights_by_id[item] for item in applicable_scores)
        if applicable_weight <= 0:
            errors.append(f"{actor_id}: no applicable dimensions")
        else:
            expected_overall = (
                sum(applicable_scores[item] * weights_by_id[item] for item in applicable_scores)
                / applicable_weight
            )
            if abs(expected_overall - float(judgment["overall_score"])) > 0.01:
                errors.append(f"{actor_id}: overall score is inconsistent")
        for failure in judgment["critical_failures"]:
            if failure["rule_id"] not in critical_rule_ids:
                errors.append(f"{actor_id}: unknown critical failure rule")
            unknown_refs = set(failure["evidence_refs"]) - known_references
            if unknown_refs:
                errors.append(f"{actor_id}: unknown critical-failure refs {sorted(unknown_refs)}")
    if errors:
        return None, errors

    policy = packet["panel_policy"]
    trusted_judges = [
        assignments_by_actor[_normalized_identifier(item["judge"]["actor_id"])]
        for item in judgments
    ]
    roles = sorted({item["role"] for item in trusted_judges})
    providers = sorted({item["provider"] for item in trusted_judges})
    model_families = sorted({item["model_family"] for item in trusted_judges})
    panel_sufficient = (
        len(judgments) >= policy["minimum_judges"]
        and set(policy["required_roles"]) <= set(roles)
        and len({_normalized_identifier(item) for item in providers})
        >= policy["minimum_distinct_providers"]
        and len({_normalized_identifier(item) for item in model_families})
        >= policy["minimum_distinct_model_families"]
    )

    dimension_medians: dict[str, float] = {}
    applicability_disputed = False
    for dimension_id in sorted(dimension_ids_set):
        scores = [
            float(item["dimension_results"][dimension_id]["score"])
            for item in judgments
            if item["dimension_results"][dimension_id]["status"] == "SCORED"
        ]
        if scores:
            dimension_medians[dimension_id] = median(scores)
            if (
                applicability[dimension_id] != "always"
                and len(scores) < policy["minimum_dimension_judges"]
            ):
                applicability_disputed = True
    applicable_weight = sum(weights_by_id[item] for item in dimension_medians)
    overall_score = (
        sum(dimension_medians[item] * weights_by_id[item] for item in dimension_medians)
        / applicable_weight
        if applicable_weight
        else None
    )
    overall_scores = [float(item["overall_score"]) for item in judgments]
    max_score_spread = max(overall_scores) - min(overall_scores) if overall_scores else None

    failures_by_rule: dict[str, set[str]] = defaultdict(set)
    critical_failures: list[dict[str, Any]] = []
    for judgment in judgments:
        actor_id = judgment["judge"]["actor_id"]
        for failure in judgment["critical_failures"]:
            failures_by_rule[failure["rule_id"]].add(actor_id)
            critical_failures.append({"actor_id": actor_id, **failure})
    confirmed_rules = {
        rule_id
        for rule_id, actors in failures_by_rule.items()
        if len(actors) >= policy["critical_failure_minimum_votes"]
    }
    for failure in critical_failures:
        failure["confirmed"] = failure["rule_id"] in confirmed_rules
    critical_failure_disputed = bool(failures_by_rule) and not bool(confirmed_rules)
    score_disputed = (
        max_score_spread is not None and max_score_spread > policy["max_panel_score_spread"]
    )
    disputed = score_disputed or applicability_disputed or critical_failure_disputed
    quality_passed = (
        overall_score is not None
        and overall_score >= policy["overall_pass_threshold"]
        and dimension_medians.get("grounding", 0) >= policy["grounding_pass_threshold"]
        and all(score >= policy["dimension_pass_threshold"] for score in dimension_medians.values())
        and not confirmed_rules
    )
    if confirmed_rules:
        quality_status = "FAIL"
    elif not panel_sufficient:
        quality_status = "INSUFFICIENT_PANEL"
    elif disputed:
        quality_status = "DISPUTED"
    elif quality_passed:
        quality_status = "PASS"
    else:
        quality_status = "FAIL"

    case_id = packet["run"]["case_id"]
    if not any(item.get("case_id") == case_id for item in report.get("case_results", [])):
        return None, [f"execution report lacks case result {case_id}"]
    hard_gate_status = report["status"]
    status = _combine_status(hard_gate_status, quality_status)
    human_review_required = disputed or bool(critical_failures)
    result = {
        "schema_version": "1.1",
        "packet_id": packet["packet_id"],
        "status": status,
        "quality_status": quality_status,
        "hard_gate_status": hard_gate_status,
        "judge_count": len(judgments),
        "roles": roles,
        "providers": providers,
        "model_families": model_families,
        "dimension_medians": dimension_medians,
        "overall_score": overall_score,
        "max_score_spread": max_score_spread,
        "critical_failures": critical_failures,
        "human_review_required": human_review_required,
        "inputs": {
            "packet_sha256": packet_sha256,
            "report_sha256": report_sha256,
            "assignment_ref": str(assignment_path.resolve()),
            "assignment_sha256": assignment_sha256,
            "judgments": sorted(judgment_bindings, key=lambda item: item["actor_id"].casefold()),
        },
        "notes": (
            "Deterministic hard gates and external panel quality are combined with AND semantics."
        ),
    }
    result_errors = _schema_errors(result, "panel-result.schema.json", "panel result")
    return (None, result_errors) if result_errors else (result, [])


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packet", type=Path)
    parser.add_argument("report", type=Path)
    parser.add_argument("judgments", type=Path, nargs="+")
    parser.add_argument("--assignment", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result, errors = aggregate(args.packet, args.report, args.judgments, args.assignment)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    payload = json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(payload, end="")
    else:
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(payload, encoding="utf-8")
        temporary.replace(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
