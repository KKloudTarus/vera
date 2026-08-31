"""Validate VERA evaluation contracts and run reports."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

try:
    from jsonschema import Draft202012Validator, FormatChecker
except ImportError as exc:  # pragma: no cover - exercised by installation smoke checks
    raise SystemExit('jsonschema is required; install the project with ".[dev]"') from exc

try:
    from evals.assertions import assertion_passes
    from evals.generate_load_fixture import GENERATOR_VERSION, generate
except ModuleNotFoundError:  # Direct execution: python evals/validate.py
    from assertions import assertion_passes  # type: ignore[no-redef]
    from generate_load_fixture import GENERATOR_VERSION, generate

ROOT = Path(__file__).resolve().parent
CASE_ID = re.compile(r"^[A-Z][A-Z0-9]*-[0-9]{3}$")
CHECK_ID = re.compile(r"^[A-Z]+-[0-9]{3}$")
PROFILES = {"daily", "nightly", "weekly", "release"}
STATUSES = {"PASS", "FAIL", "BLOCKED", "NOT_APPLICABLE"}


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number}: record must be an object")
            records.append(value)
    return records


def load_cases() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for path in sorted((ROOT / "scenarios").glob("*.jsonl")):
        cases.extend(load_jsonl(path))
    return cases


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_json(value: Any) -> str:
    serialized = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


def dataset_sha256() -> str:
    paths = [
        *(ROOT / "scenarios").glob("*.jsonl"),
        *(ROOT / "fixtures").glob("*.json"),
    ]
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.as_posix()):
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def source_tree_sha256(root: Path | None = None) -> str:
    root = ROOT if root is None else root
    workspace = root.parent
    excluded_parts = {
        "__pycache__",
        ".cache",
        ".pytest_cache",
        ".state",
        "baselines",
        "runs",
        "test-runs",
        "tests",
    }
    paths: set[Path] = set()
    for directory in (workspace / "src", workspace / "migrations", root):
        if not directory.is_dir():
            continue
        paths.update(
            path
            for path in directory.rglob("*")
            if path.is_file() and not excluded_parts.intersection(path.parts)
        )
    paths.update(
        path
        for path in (
            workspace / "alembic.ini",
            workspace / "constraints.lock",
            workspace / "pyproject.toml",
        )
        if path.is_file()
    )
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.relative_to(workspace).as_posix()):
        relative = path.relative_to(workspace).as_posix().encode()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def fixture_data(case: dict[str, Any]) -> dict[str, Any]:
    declared = case.get("fixture", {})
    combined: dict[str, Any] = {}
    fixture_file = declared.get("file")
    if fixture_file:
        combined.update(load_json(ROOT.parent / fixture_file))
    combined.update(declared)
    return combined


def fixture_ref_exists(case: dict[str, Any], reference: str) -> bool:
    return fixture_ref_value(case, reference) is not _MISSING_FIXTURE


_MISSING_FIXTURE = object()


def fixture_ref_value(case: dict[str, Any], reference: str) -> Any:
    path = reference.removeprefix("fixture.")
    current: Any = fixture_data(case)
    for part in path.split("."):
        match = re.fullmatch(r"([^\[]+)(?:\[(\d+)\])?", part)
        if match is None or not isinstance(current, dict) or match.group(1) not in current:
            return _MISSING_FIXTURE
        current = current[match.group(1)]
        if match.group(2) is not None:
            index = int(match.group(2))
            if not isinstance(current, list) or index >= len(current):
                return _MISSING_FIXTURE
            current = current[index]
    return current


def require(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def _contains_key(value: Any, forbidden: set[str]) -> bool:
    if isinstance(value, dict):
        return bool(set(value) & forbidden) or any(
            _contains_key(item, forbidden) for item in value.values()
        )
    if isinstance(value, list):
        return any(_contains_key(item, forbidden) for item in value)
    return False


def _validate_external_panel_case(case: dict[str, Any], errors: list[str]) -> None:
    if case.get("mode") != "qualitative":
        return
    case_id = case.get("case_id", "<missing>")
    evaluation = case.get("evaluation")
    require(
        isinstance(evaluation, dict),
        f"{case_id}: qualitative case requires an external panel contract",
        errors,
    )
    if not isinstance(evaluation, dict):
        return
    require(
        evaluation.get("method") == "external_panel",
        f"{case_id}: qualitative output must be judged outside the execution run",
        errors,
    )
    for field in ("task_ref", "documents_ref"):
        reference = evaluation.get(field)
        require(
            isinstance(reference, str)
            and reference.startswith("fixture.")
            and fixture_ref_exists(case, reference),
            f"{case_id}: {field} must resolve inside the frozen fixture",
            errors,
        )
    candidate_ref = evaluation.get("candidate_output_ref")
    observed_roots = {
        path.split(".", 1)[0] for step in case.get("steps", []) for path in step.get("observe", [])
    }
    require(
        isinstance(candidate_ref, str) and candidate_ref.split(".", 1)[0] in observed_roots,
        f"{case_id}: candidate_output_ref is not produced by a scenario step",
        errors,
    )
    for field in ("rubric_path", "panel_policy_path"):
        declared_path = evaluation.get(field)
        path = (ROOT / str(declared_path)).resolve()
        try:
            path.relative_to(ROOT.resolve())
            safe = True
        except ValueError:
            safe = False
        require(
            safe and path.is_file(),
            f"{case_id}: {field} must be an existing file under evals/",
            errors,
        )
    rubric_path = (ROOT / str(evaluation.get("rubric_path", ""))).resolve()
    if rubric_path.is_file():
        rubric = load_json(rubric_path)
        rubric_validator = Draft202012Validator(
            load_json(ROOT / "judging" / "schemas" / "rubric.schema.json"),
            format_checker=FormatChecker(),
        )
        for schema_error in rubric_validator.iter_errors(rubric):
            errors.append(f"{case_id}: rubric schema: {schema_error.message}")
        dimensions = rubric.get("dimensions", [])
        dimension_ids = [item.get("id") for item in dimensions if isinstance(item, dict)]
        weights = [item.get("weight") for item in dimensions if isinstance(item, dict)]
        require(
            len(dimension_ids) == len(dimensions) and len(dimension_ids) == len(set(dimension_ids)),
            f"{case_id}: rubric dimension IDs must be unique",
            errors,
        )
        require(
            len(weights) == len(dimensions)
            and all(
                isinstance(weight, (int, float)) and not isinstance(weight, bool)
                for weight in weights
            )
            and abs(sum(weights) - 1.0) <= 1e-6,
            f"{case_id}: rubric weights must sum to 1",
            errors,
        )
        critical_ids = [
            item.get("id")
            for item in rubric.get("critical_failure_rules", [])
            if isinstance(item, dict)
        ]
        require(
            len(critical_ids) == len(set(critical_ids)),
            f"{case_id}: rubric critical-failure IDs must be unique",
            errors,
        )
        require(
            not _contains_key(
                rubric,
                {"answer_key", "canonical_answer", "expected_answer", "reference_answer"},
            ),
            f"{case_id}: qualitative rubric cannot contain a canonical answer",
            errors,
        )
    panel_policy_path = (ROOT / str(evaluation.get("panel_policy_path", ""))).resolve()
    if panel_policy_path.is_file():
        panel_policy = load_json(panel_policy_path)
        panel_validator = Draft202012Validator(
            load_json(ROOT / "judging" / "schemas" / "panel-policy.schema.json"),
            format_checker=FormatChecker(),
        )
        for schema_error in panel_validator.iter_errors(panel_policy):
            errors.append(f"{case_id}: panel policy schema: {schema_error.message}")
    assertion_targets = {assertion.get("target") for assertion in case.get("assertions", [])}
    require(
        all(
            isinstance(target, str)
            and not target.startswith(f"{str(candidate_ref).split('.', 1)[0]}.")
            for target in assertion_targets
        ),
        f"{case_id}: execution assertions cannot hard-code candidate answer content",
        errors,
    )
    require(
        {"evaluation.packet_ready", "evaluation.candidate_output_present"} <= assertion_targets,
        f"{case_id}: qualitative case must prove packet and candidate output readiness",
        errors,
    )
    require(
        any(step.get("action") == "agent.run" for step in case.get("steps", [])),
        f"{case_id}: qualitative case requires a candidate agent run",
        errors,
    )


def validate_contracts() -> tuple[list[str], int, int, int]:
    errors: list[str] = []
    case_schema = load_json(ROOT / "schemas" / "case.schema.json")
    checklist_schema = load_json(ROOT / "schemas" / "checklist.schema.json")
    run_schema = load_json(ROOT / "schemas" / "run.schema.json")
    baseline_schema = load_json(ROOT / "schemas" / "baseline.schema.json")
    judging_schemas = [
        load_json(path) for path in sorted((ROOT / "judging" / "schemas").glob("*.json"))
    ]
    checklist = load_json(ROOT / "checklist.json")
    catalog = load_json(ROOT / "action_catalog.json")
    for fixture in (ROOT / "fixtures").glob("*.json"):
        load_json(fixture)

    load_fixture = load_json(ROOT / "fixtures" / "load.json")
    generator = load_fixture["generator"]
    require(
        generator["version"] == GENERATOR_VERSION,
        "load fixture generator version does not match implementation",
        errors,
    )
    generated_digest = generate(
        output=None,
        scopes=generator["scope_count"],
        facts_per_scope=generator["facts_per_scope"],
        queries=generator["query_count"],
        seed=generator["seed"],
    )
    require(
        generated_digest == generator["corpus_sha256"],
        "load fixture digest does not match generator output",
        errors,
    )

    cases = load_cases()

    real_world_fixture = load_json(ROOT / "fixtures" / "daily_real_world.json")
    variation_axes = real_world_fixture.get("variation_axes", {})
    require(
        len(variation_axes.get("personas", [])) >= 5
        and len(variation_axes.get("domains", [])) >= 5
        and len(variation_axes.get("task_shapes", [])) >= 5,
        "daily real-world fixture lacks persona, domain, or task-shape breadth",
        errors,
    )
    workflows = real_world_fixture.get("workflows", {})
    require(
        len(workflows.get("executive_daily_brief", {}).get("documents", [])) >= 20,
        "executive daily workflow must contain dozens of documents",
        errors,
    )
    require(
        len(workflows) >= 5
        and all(
            isinstance(workflow.get("task"), dict)
            and isinstance(workflow.get("documents"), list)
            and workflow["documents"]
            for workflow in workflows.values()
        ),
        "every real-world workflow needs a task and source documents",
        errors,
    )
    panel_policy = load_json(ROOT / "judging" / "panel-policy.json")
    require(
        panel_policy.get("minimum_judges", 0) >= 4
        and panel_policy.get("minimum_distinct_model_families", 0) >= 3
        and panel_policy.get("minimum_distinct_providers", 0) >= 2,
        "judge panel policy lacks model and provider diversity",
        errors,
    )
    role_catalog = load_json(ROOT / "judging" / "roles.json")
    roles = role_catalog.get("roles", {})
    require(
        role_catalog.get("schema_version") == "1.0",
        "invalid judge role catalog version",
        errors,
    )
    require(isinstance(roles, dict), "judge role catalog must contain a roles object", errors)
    require(
        set(panel_policy.get("required_roles", [])) <= set(roles),
        "judge panel role is missing from the role catalog",
        errors,
    )
    for role_id, role in roles.items():
        require(
            isinstance(role, dict)
            and isinstance(role.get("primary_scrutiny"), str)
            and bool(role["primary_scrutiny"])
            and isinstance(role.get("checks"), list)
            and bool(role["checks"])
            and all(isinstance(item, str) and item for item in role["checks"]),
            f"judge role {role_id!r} is invalid",
            errors,
        )

    retrieval_fixture = load_json(ROOT / "fixtures" / "retrieval.json")
    fact_ids = [item["fact_id"] for item in retrieval_fixture["facts"]]
    query_ids = [item["query_id"] for item in retrieval_fixture["queries"]]
    require(len(fact_ids) == len(set(fact_ids)), "duplicate retrieval fact ID", errors)
    require(len(query_ids) == len(set(query_ids)), "duplicate retrieval query ID", errors)
    for query in retrieval_fixture["queries"]:
        require(
            set(query["relevance"]) <= set(fact_ids),
            f"{query['query_id']}: relevance references an unknown fact",
            errors,
        )
    learning_case = next(case for case in cases if case["case_id"] == "LEARN-001")
    train_ids = set(learning_case["fixture"]["train_query_ids"])
    holdout_ids = set(learning_case["fixture"]["holdout_query_ids"])
    require(train_ids <= set(query_ids), "LEARN-001 has unknown training query IDs", errors)
    require(holdout_ids <= set(query_ids), "LEARN-001 has unknown holdout query IDs", errors)
    require(not train_ids & holdout_ids, "LEARN-001 train and holdout overlap", errors)

    for schema in (
        case_schema,
        checklist_schema,
        run_schema,
        baseline_schema,
        *judging_schemas,
    ):
        Draft202012Validator.check_schema(schema)
    case_validator = Draft202012Validator(case_schema, format_checker=FormatChecker())
    for case in cases:
        for error in case_validator.iter_errors(case):
            errors.append(f"{case.get('case_id', '<missing>')}: schema: {error.message}")
    checklist_validator = Draft202012Validator(checklist_schema, format_checker=FormatChecker())
    for error in checklist_validator.iter_errors(checklist):
        errors.append(f"checklist schema: {error.message}")
    actions = {item["name"]: item for item in catalog["actions"]}
    operators = set(catalog["operators"])
    required_case_fields = set(case_schema["required"])
    allowed_case_fields = set(case_schema["properties"])
    allowed_suites = set(case_schema["properties"]["suite"]["enum"])
    required_step_fields = set(case_schema["$defs"]["step"]["required"])
    required_assertion_fields = set(case_schema["$defs"]["assertion"]["required"])

    case_ids = [case.get("case_id") for case in cases]
    require(len(case_ids) == len(set(case_ids)), "duplicate case ID", errors)
    for case in cases:
        case_id = str(case.get("case_id", "<missing>"))
        require(bool(CASE_ID.fullmatch(case_id)), f"{case_id}: invalid case ID", errors)
        require(
            required_case_fields <= set(case),
            f"{case_id}: missing required fields {sorted(required_case_fields - set(case))}",
            errors,
        )
        require(
            set(case) <= allowed_case_fields,
            f"{case_id}: unknown fields {sorted(set(case) - allowed_case_fields)}",
            errors,
        )
        require(case.get("suite") in allowed_suites, f"{case_id}: invalid suite", errors)
        profiles = set(case.get("profiles", []))
        require(bool(profiles) and profiles <= PROFILES, f"{case_id}: invalid profiles", errors)

        step_ids = [step.get("id") for step in case.get("steps", [])]
        require(len(step_ids) == len(set(step_ids)), f"{case_id}: duplicate step ID", errors)
        used_actions: list[dict[str, Any]] = []
        for step in case.get("steps", []) + case.get("cleanup", []):
            require(
                required_step_fields <= set(step),
                f"{case_id}/{step.get('id')}: missing step fields",
                errors,
            )
            action_name = step.get("action")
            require(
                action_name in actions,
                f"{case_id}/{step.get('id')}: unknown action {action_name!r}",
                errors,
            )
            if action_name in actions:
                action = actions[action_name]
                used_actions.append(action)
                step_input = step.get("input", {})
                for input_requirement in action.get("required_inputs", []):
                    choices = input_requirement.split("|")
                    require(
                        any(choice in step_input for choice in choices),
                        f"{case_id}/{step.get('id')}: action {action_name} needs one of {choices}",
                        errors,
                    )
                for input_name, input_value in step_input.items():
                    if input_name.endswith("_file") and isinstance(input_value, str):
                        require(
                            (ROOT.parent / input_value).is_file(),
                            f"{case_id}/{step.get('id')}: missing file {input_value}",
                            errors,
                        )
                    if input_name == "fixture" and isinstance(input_value, str):
                        require(
                            fixture_ref_exists(case, input_value),
                            f"{case_id}/{step.get('id')}: unknown fixture {input_value}",
                            errors,
                        )
                    if (
                        input_name.endswith("_ref")
                        and isinstance(input_value, str)
                        and input_value.startswith("fixture.")
                    ):
                        require(
                            fixture_ref_exists(case, input_value),
                            f"{case_id}/{step.get('id')}: unknown fixture ref {input_value}",
                            errors,
                        )
                if action_name == "calibration.evaluate":
                    require(
                        step_input.get("apply") is False,
                        f"{case_id}/{step.get('id')}: calibration must not apply weights",
                        errors,
                    )
                    require(
                        step_input.get("group_ids_ref") == "scope.group_ids",
                        f"{case_id}/{step.get('id')}: calibration must use run-owned groups",
                        errors,
                    )
                require(
                    case.get("isolation") in action["allowed_isolation"],
                    f"{case_id}/{step.get('id')}: action {action_name} is unsafe for "
                    f"isolation {case.get('isolation')}",
                    errors,
                )

        missing_capability = any(
            action["availability"] == "product_capability_missing" for action in used_actions
        )
        require(
            case.get("capability_expectation")
            == ("gap_probe" if missing_capability else "supported"),
            f"{case_id}: capability_expectation does not match action availability",
            errors,
        )

        assertion_ids = [item.get("id") for item in case.get("assertions", [])]
        require(
            len(assertion_ids) == len(set(assertion_ids)),
            f"{case_id}: duplicate assertion ID",
            errors,
        )
        observed_roots = {
            str(path).split(".", 1)[0].split("[", 1)[0]
            for step in case.get("steps", []) + case.get("cleanup", [])
            for path in step.get("observe", [])
        }
        runner_derived_targets = {
            "client_scope_effect",
            "evaluation.candidate_output_present",
            "evaluation.packet_ready",
        }
        for assertion in case.get("assertions", []):
            assertion_id = assertion.get("id", "<missing>")
            require(
                required_assertion_fields <= set(assertion),
                f"{case_id}/{assertion_id}: missing assertion fields",
                errors,
            )
            require(
                assertion.get("operator") in operators,
                f"{case_id}/{assertion_id}: unknown operator",
                errors,
            )
            target = str(assertion.get("target", ""))
            require(
                target in runner_derived_targets
                or target.split(".", 1)[0].split("[", 1)[0] in observed_roots,
                f"{case_id}/{assertion_id}: target is outside every declared observation root",
                errors,
            )
            if assertion.get("activation") == "baseline_required":
                require(
                    isinstance(assertion.get("minimum_sample_size"), int)
                    and assertion["minimum_sample_size"] > 0,
                    f"{case_id}/{assertion_id}: baseline assertion needs minimum_sample_size",
                    errors,
                )
                require(
                    bool(assertion.get("threshold_source")),
                    f"{case_id}/{assertion_id}: baseline assertion needs threshold_source",
                    errors,
                )

        _validate_external_panel_case(case, errors)

        fixture_file = case.get("fixture", {}).get("file") or case.get("fixture", {}).get(
            "questions_file"
        )
        if fixture_file:
            fixture_path = ROOT.parent / fixture_file
            require(fixture_path.is_file(), f"{case_id}: missing fixture {fixture_file}", errors)

    checks = checklist.get("items", [])
    required_check_fields = set(checklist_schema["properties"]["items"]["items"]["required"])
    check_ids = [check.get("id") for check in checks]
    require(len(check_ids) == len(set(check_ids)), "duplicate checklist ID", errors)
    known_cases = set(case_ids)
    referenced_cases: set[str] = set()
    for check in checks:
        check_id = str(check.get("id", "<missing>"))
        require(
            required_check_fields <= set(check),
            f"{check_id}: missing checklist fields",
            errors,
        )
        require(bool(CHECK_ID.fullmatch(check_id)), f"{check_id}: invalid checklist ID", errors)
        profiles = set(check.get("profiles", []))
        require(bool(profiles) and profiles <= PROFILES, f"{check_id}: invalid profiles", errors)
        for case_id in check.get("scenario_ids", []):
            referenced_cases.add(case_id)
            require(case_id in known_cases, f"{check_id}: unknown case reference {case_id}", errors)
            if case_id in known_cases:
                case = cases[case_ids.index(case_id)]
                require(
                    bool(profiles & set(case["profiles"])),
                    f"{check_id}: no profile overlaps {case_id}",
                    errors,
                )
        scenario_ids = check.get("scenario_ids", [])
        if scenario_ids:
            for profile in profiles:
                require(
                    any(
                        profile in cases[case_ids.index(case_id)]["profiles"]
                        for case_id in scenario_ids
                        if case_id in known_cases
                    ),
                    f"{check_id}: no mapped scenario covers profile {profile}",
                    errors,
                )
    require(
        known_cases <= referenced_cases,
        f"unreferenced cases: {sorted(known_cases - referenced_cases)}",
        errors,
    )

    return errors, len(cases), len(checks), len(actions)


def validate_report(
    path: Path, checks: list[dict[str, Any]], cases: list[dict[str, Any]]
) -> list[str]:
    errors: list[str] = []
    report = load_json(path)
    validator = Draft202012Validator(
        load_json(ROOT / "schemas" / "run.schema.json"),
        format_checker=FormatChecker(),
    )
    schema_errors = [f"{path}: schema: {error.message}" for error in validator.iter_errors(report)]
    errors.extend(schema_errors)
    if not isinstance(report, dict):
        return errors
    manifest = report.get("manifest", {})
    contract_digests = {
        "checklist_sha256": sha256_file(ROOT / "checklist.json"),
        "action_catalog_sha256": sha256_file(ROOT / "action_catalog.json"),
        "dataset_sha256": dataset_sha256(),
        "source_tree_sha256": source_tree_sha256(),
    }
    for field, expected_digest in contract_digests.items():
        actual_digest = manifest.get(field)
        if actual_digest is not None:
            require(
                actual_digest == expected_digest,
                f"{path}: manifest {field} does not match checked-in contract",
                errors,
            )
    execution_profiles = manifest.get("execution_profiles", [])
    execution_profile_digest = manifest.get("execution_profile_sha256")
    if execution_profile_digest is not None:
        require(
            execution_profile_digest == sha256_json(execution_profiles),
            f"{path}: execution_profile_sha256 does not match execution_profiles",
            errors,
        )

    baseline_ref = manifest.get("baseline")
    if baseline_ref is not None:
        baseline_uri = baseline_ref.get("uri", "")
        if "://" in baseline_uri:
            errors.append(f"{path}: baseline must be materialized as a local immutable file")
        else:
            baseline_path = Path(baseline_uri)
            if not baseline_path.is_absolute():
                baseline_path = ROOT.parent / baseline_path
            if not baseline_path.is_file():
                errors.append(f"{path}: baseline file does not exist: {baseline_path}")
            else:
                baseline_digest = sha256_file(baseline_path)
                require(
                    baseline_digest == baseline_ref.get("sha256"),
                    f"{path}: baseline digest mismatch",
                    errors,
                )
                baseline = load_json(baseline_path)
                baseline_validator = Draft202012Validator(
                    load_json(ROOT / "schemas" / "baseline.schema.json"),
                    format_checker=FormatChecker(),
                )
                for error in baseline_validator.iter_errors(baseline):
                    errors.append(f"{path}: baseline schema: {error.message}")
                require(
                    baseline.get("baseline_id") == baseline_ref.get("baseline_id"),
                    f"{path}: baseline ID mismatch",
                    errors,
                )
                compatibility = baseline.get("compatibility", {})
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
                for field in compatibility_fields:
                    require(
                        compatibility.get(field) == manifest.get(field),
                        f"{path}: baseline is incompatible on {field}",
                        errors,
                    )
                require(
                    compatibility.get("profile") == report.get("profile"),
                    f"{path}: baseline is incompatible on profile",
                    errors,
                )
    selection = report.get("selection", {})
    selected_checks = set(selection.get("check_ids", []))
    selected_cases = set(selection.get("case_ids", []))
    expected_selection_digest = sha256_json(
        {
            "check_ids": sorted(selected_checks),
            "case_ids": sorted(selected_cases),
        }
    )
    selection_digest = manifest.get("selection_sha256")
    if selection_digest is not None:
        require(
            selection_digest == expected_selection_digest,
            f"{path}: selection_sha256 does not match selection",
            errors,
        )
    check_results = report.get("check_results", [])
    case_results = report.get("case_results", [])
    result_checks = [item.get("check_id") for item in check_results]
    result_cases = [item.get("case_id") for item in case_results]

    profile = report.get("profile")
    expected_checks = {item["id"] for item in checks if profile in item["profiles"]}
    expected_cases = {item["case_id"] for item in cases if profile in item["profiles"]}
    require(
        selected_checks == expected_checks,
        f"{path}: check selection does not match profile",
        errors,
    )
    require(
        selected_cases == expected_cases,
        f"{path}: case selection does not match profile",
        errors,
    )
    require(
        selected_checks == set(result_checks),
        f"{path}: selected checks and results differ",
        errors,
    )
    require(
        selected_cases == set(result_cases),
        f"{path}: selected cases and results differ",
        errors,
    )
    require(
        len(result_checks) == len(set(result_checks)),
        f"{path}: duplicate check result",
        errors,
    )
    require(
        len(result_cases) == len(set(result_cases)),
        f"{path}: duplicate case result",
        errors,
    )

    evidence_ids = [item.get("evidence_id") for item in report.get("evidence", [])]
    metric_ids = [item.get("metric_id") for item in report.get("metrics", [])]
    require(
        len(evidence_ids) == len(set(evidence_ids)),
        f"{path}: duplicate evidence ID",
        errors,
    )
    require(
        len(metric_ids) == len(set(metric_ids)),
        f"{path}: duplicate metric ID",
        errors,
    )
    known_evidence = set(evidence_ids)
    known_metrics = set(metric_ids)
    evidence_by_id = {item.get("evidence_id"): item for item in report.get("evidence", [])}
    run_directory = path.resolve().parent
    for item in report.get("evidence", []):
        ref = item.get("ref")
        if not isinstance(ref, str):
            continue
        evidence_path = Path(ref).resolve()
        try:
            evidence_path.relative_to(run_directory)
        except ValueError:
            require(False, f"{path}: evidence ref escapes the run directory", errors)
            continue
        require(evidence_path.is_file(), f"{path}: evidence file does not exist", errors)
        if evidence_path.is_file():
            require(
                sha256_file(evidence_path) == item.get("sha256"),
                f"{path}: evidence SHA-256 mismatch for {item.get('evidence_id')}",
                errors,
            )
    judge_packets = report.get("judge_packets", [])
    packet_ids = [item.get("packet_id") for item in judge_packets]
    packet_case_ids = [item.get("case_id") for item in judge_packets]
    require(
        len(packet_ids) == len(set(packet_ids)),
        f"{path}: duplicate judge packet ID",
        errors,
    )
    require(
        len(packet_case_ids) == len(set(packet_case_ids)),
        f"{path}: duplicate judge packet case",
        errors,
    )
    packet_validator = Draft202012Validator(
        load_json(ROOT / "judging" / "schemas" / "judge-packet.schema.json"),
        format_checker=FormatChecker(),
    )
    for item in judge_packets:
        packet_path = Path(str(item.get("ref", ""))).resolve()
        try:
            packet_path.relative_to(run_directory)
        except ValueError:
            require(False, f"{path}: judge packet escapes the run directory", errors)
            continue
        require(packet_path.is_file(), f"{path}: judge packet file does not exist", errors)
        if not packet_path.is_file():
            continue
        require(
            sha256_file(packet_path) == item.get("sha256"),
            f"{path}: judge packet SHA-256 mismatch for {item.get('packet_id')}",
            errors,
        )
        try:
            packet = load_json(packet_path)
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"{path}: judge packet is unreadable: {exc}")
            continue
        for schema_error in packet_validator.iter_errors(packet):
            errors.append(f"{path}: judge packet schema: {schema_error.message}")
        packet_run = packet.get("run", {})
        require(
            packet.get("packet_id") == item.get("packet_id")
            and packet_run.get("case_id") == item.get("case_id"),
            f"{path}: judge packet identity mismatch",
            errors,
        )
        manifest = report.get("manifest", {})
        require(
            packet_run.get("run_id") == report.get("run_id")
            and packet_run.get("profile") == report.get("profile")
            and packet_run.get("dataset_sha256") == manifest.get("dataset_sha256")
            and packet_run.get("service_version") == manifest.get("service_version")
            and packet_run.get("git_sha") == manifest.get("git_sha"),
            f"{path}: judge packet run provenance mismatch",
            errors,
        )
        source_material = packet.get("source_material", {})
        documents = source_material.get("documents")
        require(
            isinstance(documents, list)
            and source_material.get("document_count") == len(documents)
            and source_material.get("sha256") == sha256_json(documents),
            f"{path}: judge packet source-material digest mismatch",
            errors,
        )
    checks_by_id = {item["id"]: item for item in checks}
    cases_by_id = {item["case_id"]: item for item in cases}
    qualitative_case_ids = {
        case_id
        for case_id in selected_cases
        if cases_by_id.get(case_id, {}).get("mode") == "qualitative"
    }
    require(
        set(packet_case_ids) <= qualitative_case_ids,
        f"{path}: judge packet references a non-qualitative case",
        errors,
    )
    expected_quality_status = (
        "PENDING_JUDGMENT"
        if judge_packets
        else ("NOT_ELIGIBLE" if qualitative_case_ids else "NOT_REQUESTED")
    )
    require(
        report.get("quality_status") == expected_quality_status,
        f"{path}: run quality_status should be {expected_quality_status}",
        errors,
    )
    for result in check_results:
        require(
            set(result.get("evidence_ids", [])) <= known_evidence,
            f"{path}: unknown evidence reference",
            errors,
        )
        require(result.get("status") in STATUSES, f"{path}: invalid result status", errors)
        if checks_by_id.get(result.get("check_id"), {}).get("priority") == "P0":
            require(
                result.get("status") != "NOT_APPLICABLE",
                f"{path}: selected P0 check {result.get('check_id')} cannot be N/A",
                errors,
            )
        if result.get("status") in {"PASS", "FAIL"}:
            require(
                bool(result.get("evidence_ids")),
                f"{path}: executed check {result.get('check_id')} lacks evidence",
                errors,
            )
            provided_labels = {
                label
                for evidence_id in result.get("evidence_ids", [])
                for label in evidence_by_id.get(evidence_id, {}).get("labels", [])
            }
            require(
                set(checks_by_id.get(result.get("check_id"), {}).get("evidence", []))
                <= provided_labels,
                f"{path}: executed check {result.get('check_id')} lacks declared evidence",
                errors,
            )
        if result.get("status") == "BLOCKED":
            require(
                bool(result.get("blocked_reason")),
                f"{path}: blocked result lacks reason",
                errors,
            )
    gating_fail = False
    gating_blocked = False
    failed_case_ids: set[str] = set()
    baseline_present = report.get("manifest", {}).get("baseline") is not None
    for result in case_results:
        require(
            set(result.get("evidence_ids", [])) <= known_evidence,
            f"{path}: unknown case evidence reference",
            errors,
        )

        require(
            set(result.get("metric_ids", [])) <= known_metrics,
            f"{path}: unknown metric reference",
            errors,
        )
        case_id = result.get("case_id")
        declared_case = cases_by_id.get(case_id, {})
        expected_case_quality = (
            "PENDING_JUDGMENT"
            if case_id in set(packet_case_ids)
            else ("NOT_ELIGIBLE" if declared_case.get("mode") == "qualitative" else "NOT_REQUESTED")
        )
        require(
            result.get("quality_status") == expected_case_quality,
            f"{path}: case quality_status mismatch for {case_id}",
            errors,
        )
        declared_assertions = {item["id"]: item for item in declared_case.get("assertions", [])}
        assertion_results = result.get("assertion_results", [])
        assertion_ids = [item.get("assertion_id") for item in assertion_results]
        require(
            len(assertion_ids) == len(set(assertion_ids)),
            f"{path}: duplicate assertion result in {case_id}",
            errors,
        )
        require(
            set(assertion_ids) == set(declared_assertions),
            f"{path}: assertion results do not match scenario {case_id}",
            errors,
        )
        for assertion in assertion_results:
            assertion_id = assertion.get("assertion_id")
            declared = declared_assertions.get(assertion_id, {})
            require(
                set(assertion.get("evidence_ids", [])) <= known_evidence,
                f"{path}: unknown assertion evidence",
                errors,
            )
            require(
                assertion.get("target") == declared.get("target"),
                f"{path}: assertion target changed for {case_id}/{assertion_id}",
                errors,
            )
            require(
                assertion.get("expected") == declared.get("expected"),
                f"{path}: assertion expected value changed for {case_id}/{assertion_id}",
                errors,
            )
            report_schema_version = report.get("schema_version")
            if report_schema_version == "1.1":
                require(
                    assertion.get("operator") == declared.get("operator"),
                    f"{path}: assertion operator changed for {case_id}/{assertion_id}",
                    errors,
                )
                declared_expected = declared.get("expected")
                if not (
                    isinstance(declared_expected, dict)
                    and set(declared_expected) == {"observation_ref"}
                ):
                    require(
                        assertion.get("resolved_expected") == declared_expected,
                        f"{path}: resolved expected value changed for {case_id}/{assertion_id}",
                        errors,
                    )
            assertion_status = assertion.get("status")
            require(
                assertion_status in STATUSES,
                f"{path}: invalid assertion status for {case_id}/{assertion_id}",
                errors,
            )
            if assertion_status in {"PASS", "FAIL"}:
                require(
                    bool(assertion.get("evidence_ids")),
                    f"{path}: executed assertion {case_id}/{assertion_id} lacks evidence",
                    errors,
                )
                provided_labels = {
                    label
                    for evidence_id in assertion.get("evidence_ids", [])
                    for label in evidence_by_id.get(evidence_id, {}).get("labels", [])
                }
                require(
                    set(declared.get("evidence", [])) <= provided_labels,
                    f"{path}: executed assertion {case_id}/{assertion_id} lacks declared evidence",
                    errors,
                )
            if report_schema_version == "1.1" and assertion_status == "PASS":
                require(
                    assertion.get("evaluation_kind") == "operator",
                    f"{path}: passing assertion {case_id}/{assertion_id} "
                    "was not operator-evaluated",
                    errors,
                )
                require(
                    assertion_passes(
                        str(assertion.get("operator")),
                        str(assertion.get("target")),
                        assertion.get("observed"),
                        assertion.get("resolved_expected"),
                        observed_present=assertion.get("observed_present") is True,
                        expected_present=assertion.get("expected_present") is True,
                    ),
                    f"{path}: assertion outcome contradicts observation "
                    f"for {case_id}/{assertion_id}",
                    errors,
                )
            activation = declared.get("activation")
            if activation in {"always", "agent_runner_required"}:
                require(
                    assertion_status != "NOT_APPLICABLE",
                    f"{path}: required assertion {case_id}/{assertion_id} is N/A",
                    errors,
                )
            if activation == "baseline_required" and not baseline_present:
                require(
                    assertion_status == "NOT_APPLICABLE",
                    f"{path}: no-baseline assertion {case_id}/{assertion_id} must be N/A",
                    errors,
                )
            if activation == "capability_probe":
                require(
                    assertion_status in {"PASS", "FAIL", "BLOCKED"},
                    f"{path}: capability probe {case_id}/{assertion_id} has invalid status",
                    errors,
                )
            if declared.get("gate") is True:
                gating_fail = gating_fail or assertion_status == "FAIL"
                gating_blocked = gating_blocked or assertion_status == "BLOCKED"

        assertion_statuses = [item.get("status") for item in assertion_results]
        if "FAIL" in assertion_statuses:
            expected_case_status = "FAIL"
        elif "BLOCKED" in assertion_statuses:
            expected_case_status = "BLOCKED"
        elif assertion_statuses and all(
            status == "NOT_APPLICABLE" for status in assertion_statuses
        ):
            expected_case_status = "NOT_APPLICABLE"
        else:
            expected_case_status = "PASS"
        require(
            result.get("status") == expected_case_status,
            f"{path}: case status contradicts assertions for {case_id}",
            errors,
        )
        if expected_case_status == "FAIL":
            failed_case_ids.add(str(case_id))
            require(
                result.get("first_bad_boundary") is not None,
                f"{path}: failed case {case_id} lacks first_bad_boundary",
                errors,
            )
        if result.get("status") == "BLOCKED":
            require(
                bool(result.get("blocked_reason")),
                f"{path}: blocked case {case_id} lacks reason",
                errors,
            )

    for result in check_results:
        check = checks_by_id.get(result.get("check_id"), {})
        if check.get("priority") == "P0":
            gating_fail = gating_fail or result.get("status") == "FAIL"
            gating_blocked = gating_blocked or result.get("status") == "BLOCKED"

    failed_check_ids = {
        str(result.get("check_id")) for result in check_results if result.get("status") == "FAIL"
    }
    findings = report.get("findings", [])
    finding_case_ids: set[str] = set()
    finding_check_ids: set[str] = set()
    all_check_ids = set(checks_by_id)
    for finding in findings:
        require(
            set(finding.get("evidence_ids", [])) <= known_evidence,
            f"{path}: finding references unknown evidence",
            errors,
        )
        case_ids_for_finding = set(finding.get("case_ids", []))
        check_ids_for_finding = set(finding.get("check_ids", []))
        finding_case_ids.update(case_ids_for_finding)
        finding_check_ids.update(check_ids_for_finding)
        require(
            case_ids_for_finding <= selected_cases,
            f"{path}: finding references an unselected case",
            errors,
        )
        require(
            check_ids_for_finding <= selected_checks,
            f"{path}: finding references an unselected check",
            errors,
        )
        require(
            set(finding.get("verification_check_ids", [])) <= all_check_ids,
            f"{path}: finding references an unknown verification check",
            errors,
        )
        require(
            finding.get("first_bad_boundary") is not None,
            f"{path}: finding lacks first_bad_boundary",
            errors,
        )
    require(
        failed_case_ids <= finding_case_ids,
        f"{path}: failed cases are missing findings",
        errors,
    )
    require(
        failed_check_ids <= finding_check_ids,
        f"{path}: failed checks are missing findings",
        errors,
    )

    valid_owners = selected_checks | selected_cases | {None, report.get("run_id")}
    for metric in report.get("metrics", []):
        require(
            metric.get("owner_id") in valid_owners,
            f"{path}: metric has unknown owner",
            errors,
        )

    gate = report.get("gate", {})
    result_groups = (
        ("check_status_counts", check_results),
        ("case_status_counts", case_results),
    )
    for key, results in result_groups:
        counts = Counter(item.get("status") for item in results)
        observed = gate.get(key, {})
        require(
            observed.get("selected") == len(results),
            f"{path}: {key}.selected mismatch",
            errors,
        )
        status_fields = (
            ("PASS", "pass"),
            ("FAIL", "fail"),
            ("BLOCKED", "blocked"),
            ("NOT_APPLICABLE", "not_applicable"),
        )
        for status, field in status_fields:
            require(
                observed.get(field) == counts[status],
                f"{path}: {key}.{field} mismatch",
                errors,
            )

    cleanup = report.get("cleanup", {})
    cleanup_status = cleanup.get("status")
    created_resources = set(cleanup.get("created_resources", []))
    removed_resources = set(cleanup.get("removed_resources", []))
    remaining_resources = set(cleanup.get("remaining_resources", []))
    if cleanup_status == "PASS":
        require(not remaining_resources, f"{path}: PASS cleanup has leftovers", errors)
        require(
            created_resources == removed_resources,
            f"{path}: PASS cleanup resource ledger does not reconcile",
            errors,
        )
    if cleanup_status == "NOT_APPLICABLE":
        require(
            not created_resources and not removed_resources and not remaining_resources,
            f"{path}: N/A cleanup contains resources",
            errors,
        )
    obs004 = next((item for item in check_results if item.get("check_id") == "OBS-004"), None)
    if obs004 is not None and obs004.get("status") == "PASS":
        require(
            all(item.get("redacted") is True for item in report.get("evidence", [])),
            f"{path}: OBS-004 passed with unredacted evidence",
            errors,
        )

    cleanup_failed = cleanup_status == "FAIL"
    cleanup_blocked = cleanup_status == "BLOCKED"
    expected_gate = not (gating_fail or gating_blocked or cleanup_failed or cleanup_blocked)
    require(
        gate.get("passed") is expected_gate,
        f"{path}: gate.passed contradicts results",
        errors,
    )

    if gating_fail or cleanup_failed:
        expected_run_status = "FAIL"
    elif gating_blocked or cleanup_blocked:
        expected_run_status = "BLOCKED"
    else:
        expected_run_status = "PASS"
    require(
        report.get("status") == expected_run_status,
        f"{path}: run status should be {expected_run_status}",
        errors,
    )
    blocked_prerequisites = report.get("blocked_prerequisites", [])
    if expected_run_status == "BLOCKED":
        require(
            bool(blocked_prerequisites),
            f"{path}: BLOCKED lacks blocked_prerequisites",
            errors,
        )
    if expected_run_status == "PASS":
        require(
            not blocked_prerequisites,
            f"{path}: PASS has blocked_prerequisites",
            errors,
        )
    return errors


def main() -> int:
    errors, case_count, check_count, action_count = validate_contracts()
    checklist = load_json(ROOT / "checklist.json")
    cases = load_cases()
    for argument in sys.argv[1:]:
        errors.extend(validate_report(Path(argument), checklist["items"], cases))

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(f"Valid: {case_count} cases, {check_count} checks, {action_count} allowlisted actions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
