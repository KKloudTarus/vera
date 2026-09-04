"""Execute VERA evaluation scenarios through a production action adapter."""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import math
import re
import shutil
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker

try:
    from evals.adapters import (
        ActionDriver,
        ActionRequest,
        ActionResponse,
        AdapterProtocolError,
        SubprocessActionDriver,
        UnavailableActionDriver,
        finish_finalizer,
    )
    from evals.assertions import assertion_passes
    from evals.validate import (
        ROOT,
        dataset_sha256,
        fixture_data,
        load_case_dependencies,
        load_cases,
        load_json,
        sha256_file,
        sha256_json,
        source_tree_sha256,
        validate_contracts,
        validate_report,
    )
except ModuleNotFoundError:  # Direct execution: python evals/runner.py
    from adapters import (  # type: ignore[no-redef]
        ActionDriver,
        ActionRequest,
        ActionResponse,
        AdapterProtocolError,
        SubprocessActionDriver,
        UnavailableActionDriver,
        finish_finalizer,
    )
    from assertions import assertion_passes  # type: ignore[no-redef]
    from validate import (  # type: ignore[no-redef]
        ROOT,
        dataset_sha256,
        fixture_data,
        load_case_dependencies,
        load_cases,
        load_json,
        sha256_file,
        sha256_json,
        source_tree_sha256,
        validate_contracts,
        validate_report,
    )

_MISSING = object()
_PATH_PART = re.compile(r"([^\[\]]+)(?:\[(\d+)\])?")
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_IMAGE_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2}
_SEVERITY_ORDER = {"BLOCKER": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
_SECRET_KEYS = frozenset(
    {
        "authorization",
        "cookie",
        "credentials",
        "password",
        "private_key",
        "secret",
        "token",
        "access_token",
        "refresh_token",
        "api_key",
        "auth_token",
        "client_secret",
        "x_api_key",
    }
)
_PAYLOAD_KEYS = frozenset({"body", "content", "prompt", "query", "question", "text"})
_PII_KEYS = frozenset(
    {"email", "email_address", "full_name", "ip_address", "phone", "phone_number"}
)
_CANDIDATE_IDENTITY_KEYS = frozenset(
    {
        "actor_id",
        "agent_name",
        "candidate_id",
        "model_family",
        "model_id",
        "prompt_version",
        "provider",
    }
)
_GOLD_LABEL_KEYS = frozenset(
    {
        "answer_key",
        "canonical_answer",
        "expected",
        "expected_answer",
        "expected_triples",
        "reference_answer",
        "relevance",
    }
)
_MUTATING_EFFECTS = frozenset(
    {"synthetic_write", "failure_injection", "external", "load", "cleanup"}
)
_RUNNER_ACTIONS = frozenset({"parity.verify", "result.score"})
_RUNNER_METRICS_BY_RUBRIC = {
    "answer-v1": frozenset(
        {
            "answer_completeness",
            "citation_correctness",
            "citation_coverage",
            "grounded_claim_precision",
            "task_success",
            "temporal_answer_accuracy",
            "tokens_per_accepted_answer",
        }
    ),
    "extraction-v1": frozenset(
        {"claim_precision", "claim_recall", "extraction_yield", "schema_invalid_count"}
    ),
    "outcome-v1": frozenset({"task_success"}),
    "retrieval-v1": frozenset({"hit_at_1", "hit_at_5", "mrr", "ndcg_at_5"}),
}
_ASSERTION_METRICS = {
    "score.precision_delta": "claim_precision",
    "score.recall_delta": "claim_recall",
    "score.hit_at_5_delta_pp": "hit_at_5",
    "score.mrr_relative_delta": "mrr",
    "profiles.http_reference.p95_ms": "p95_ms",
    "profiles.max_error_rate": "error_rate",
    "profiles.max_hit_at_5_delta_pp": "hit_at_5",
    "profiles.p95_time_to_searchable_relative_delta": "time_to_searchable_p95_ms",
    "score.agent_p95_relative_delta": "agent_p95_ms",
    "score.accepted_answer_tokens_relative_delta": "tokens_per_accepted_answer",
    "quality.hit_at_5_delta_pp": "hit_at_5",
}
_FRAMEWORK_CHECKS = frozenset(
    {
        "PRE-001",
        "PRE-002",
        "PRE-003",
        "PRE-004",
        "OBS-004",
        "REP-001",
        "REP-002",
        "REP-003",
    }
)


class RunnerError(RuntimeError):
    """The evaluation runner cannot safely complete the requested run."""


def _order_cases(
    cases: list[dict[str, Any]], dependencies: dict[str, list[str]]
) -> list[dict[str, Any]]:
    remaining = {case["case_id"]: case for case in cases}
    selected_ids = set(remaining)
    completed: set[str] = set()
    ordered: list[dict[str, Any]] = []
    while remaining:
        ready = [
            case
            for case_id, case in remaining.items()
            if set(dependencies.get(case_id, [])) & selected_ids <= completed
        ]
        if not ready:
            unresolved = ", ".join(sorted(remaining))
            raise RunnerError(f"case dependency cycle among selected cases: {unresolved}")
        case = min(
            ready,
            key=lambda item: (_PRIORITY_ORDER[item["priority"]], item["case_id"]),
        )
        ordered.append(case)
        completed.add(case["case_id"])
        del remaining[case["case_id"]]
    return ordered


class InputResolutionError(RunnerError):
    """A scenario input reference cannot be resolved."""


@dataclass(frozen=True, slots=True)
class RunConfig:
    profile: str
    environment: str
    service_version: str
    git_sha: str
    git_dirty: bool
    app_image_digest: str | None
    graph_backend: str
    hardware_profile: str
    cache_state: str
    concurrency: int
    random_seed: int
    execution_profiles: tuple[dict[str, Any], ...]
    models: dict[str, str | None]
    pipeline_versions: dict[str, str | None]
    ontology_version: str
    evaluator: dict[str, str]
    quality_config: dict[str, Any]
    run_context: dict[str, Any]
    allowed_effects: frozenset[str]
    output_root: Path
    baseline_path: Path | None = None
    run_id: str | None = None
    step_timeout_s: float = 120.0
    retain_synthetic_payloads: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any], *, root: Path = ROOT) -> RunConfig:
        allowed = {
            "profile",
            "environment",
            "service_version",
            "git_sha",
            "git_dirty",
            "app_image_digest",
            "graph_backend",
            "hardware_profile",
            "cache_state",
            "concurrency",
            "random_seed",
            "execution_profiles",
            "models",
            "pipeline_versions",
            "ontology_version",
            "evaluator",
            "quality_config",
            "run_context",
            "allowed_effects",
            "output_root",
            "baseline_path",
            "run_id",
            "step_timeout_s",
            "retain_synthetic_payloads",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"run config has unknown fields: {sorted(unknown)}")
        required = {
            "profile",
            "environment",
            "service_version",
            "git_sha",
            "git_dirty",
            "graph_backend",
            "hardware_profile",
            "cache_state",
            "concurrency",
            "random_seed",
            "ontology_version",
        }
        missing = required - set(value)
        if missing:
            raise ValueError(f"run config is missing fields: {sorted(missing)}")
        profile = str(value["profile"])
        if profile not in {"daily", "nightly", "weekly", "release"}:
            raise ValueError(f"invalid profile: {profile}")
        cache_state = str(value["cache_state"])
        if cache_state not in {"cold", "warm", "mixed", "disabled"}:
            raise ValueError(f"invalid cache_state: {cache_state}")
        concurrency = int(value["concurrency"])
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        timeout = float(value.get("step_timeout_s", 120.0))
        if timeout <= 0:
            raise ValueError("step_timeout_s must be positive")
        run_id_value = value.get("run_id")
        if run_id_value is not None and not _SAFE_RUN_ID.fullmatch(str(run_id_value)):
            raise ValueError("run_id contains unsafe characters")
        if not isinstance(value["git_dirty"], bool):
            raise ValueError("git_dirty must be a boolean")
        if len(str(value["git_sha"])) < 7:
            raise ValueError("git_sha must contain at least seven characters")
        app_image_digest = value.get("app_image_digest")
        if app_image_digest is not None and (
            not isinstance(app_image_digest, str)
            or _IMAGE_DIGEST.fullmatch(app_image_digest) is None
        ):
            raise ValueError("app_image_digest must be an immutable sha256 digest")
        if profile == "release" and app_image_digest is None:
            raise ValueError("release config must bind the evaluated application image digest")
        output_value = value.get("output_root", "runs")
        output_root = Path(str(output_value))
        if not output_root.is_absolute():
            output_root = root / output_root
        baseline_value = value.get("baseline_path")
        baseline_path = Path(str(baseline_value)).resolve() if baseline_value else None
        execution_profiles = value.get("execution_profiles") or [
            {"profile_id": profile, "dimensions": {}}
        ]
        if not isinstance(execution_profiles, list) or not all(
            isinstance(item, dict) for item in execution_profiles
        ):
            raise ValueError("execution_profiles must be an array of objects")
        evaluator = value.get("evaluator") or {
            "kind": "agent",
            "name": "vera-eval-runner",
            "version": "1.0",
            "rubric_version": "1.0",
        }
        if not isinstance(evaluator, dict):
            raise ValueError("evaluator must be an object")
        evaluator_fields = {
            key: str(evaluator.get(key, ""))
            for key in (
                "kind",
                "name",
                "version",
                "rubric_version",
            )
        }
        if evaluator_fields["kind"] not in {"agent", "human", "hybrid"}:
            raise ValueError("evaluator.kind must be agent, human, or hybrid")
        allowed_effects = value.get("allowed_effects", ["read", "judge"])
        if not isinstance(allowed_effects, list) or not all(
            isinstance(item, str) for item in allowed_effects
        ):
            raise ValueError("allowed_effects must be an array of strings")
        models = value.get("models", {})
        pipelines = value.get("pipeline_versions", {})
        quality_config = value.get("quality_config", {})
        run_context = value.get("run_context", {})
        for name, item in (
            ("models", models),
            ("pipeline_versions", pipelines),
            ("quality_config", quality_config),
            ("run_context", run_context),
        ):
            if not isinstance(item, dict):
                raise ValueError(f"{name} must be an object")
        retain_payloads = value.get("retain_synthetic_payloads", False)
        if not isinstance(retain_payloads, bool):
            raise ValueError("retain_synthetic_payloads must be a boolean")
        return cls(
            profile=profile,
            environment=str(value["environment"]),
            service_version=str(value["service_version"]),
            git_sha=str(value["git_sha"]),
            git_dirty=value["git_dirty"],
            app_image_digest=app_image_digest,
            graph_backend=str(value["graph_backend"]),
            hardware_profile=str(value["hardware_profile"]),
            cache_state=cache_state,
            concurrency=concurrency,
            random_seed=int(value["random_seed"]),
            execution_profiles=tuple(copy.deepcopy(execution_profiles)),
            models=copy.deepcopy(models),
            pipeline_versions=copy.deepcopy(pipelines),
            ontology_version=str(value["ontology_version"]),
            evaluator=evaluator_fields,
            quality_config=copy.deepcopy(quality_config),
            run_context=copy.deepcopy(run_context),
            allowed_effects=frozenset(allowed_effects),
            output_root=output_root.resolve(),
            baseline_path=baseline_path,
            run_id=str(run_id_value) if run_id_value is not None else None,
            step_timeout_s=timeout,
            retain_synthetic_payloads=retain_payloads,
        )

    @classmethod
    def from_path(cls, path: Path, *, root: Path = ROOT) -> RunConfig:
        value = load_json(path)
        if not isinstance(value, dict):
            raise ValueError("run config must be a JSON object")
        return cls.from_dict(value, root=root)


def _maximum_runner_timeout(config: RunConfig) -> float:
    maximum = config.step_timeout_s
    for case in load_cases():
        if config.profile not in case["profiles"]:
            continue
        for step in case["steps"]:
            timeout_s = float(step.get("timeout_s", config.step_timeout_s))
            action_wait = step.get("input", {}).get("timeout_s")
            if not isinstance(action_wait, bool) and isinstance(action_wait, (int, float)):
                timeout_s = max(timeout_s, float(action_wait) + 5.0)
            maximum = max(maximum, timeout_s)
    return maximum


def _subprocess_timeout(config: RunConfig, requested: float | None) -> float:
    runner_timeout = _maximum_runner_timeout(config)
    if requested is None:
        return runner_timeout + 5.0
    if requested <= runner_timeout:
        raise ValueError(
            "adapter timeout must exceed the maximum runner action timeout "
            f"of {runner_timeout:g} seconds"
        )
    return requested


@dataclass(frozen=True, slots=True)
class RunOutcome:
    run_id: str
    status: str
    report_path: Path
    summary_path: Path


@dataclass(slots=True)
class EvidenceStore:
    run_dir: Path
    retain_payloads: bool
    records: list[dict[str, Any]] = field(default_factory=list)
    _counter: int = 0

    def write(
        self,
        name: str,
        value: Any,
        *,
        kind: str = "file",
        labels: list[str] | tuple[str, ...] | None = None,
    ) -> str:
        self._counter += 1
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "evidence"
        evidence_id = f"ev-{self._counter:04d}-{slug[:48]}"
        path = self.run_dir / "evidence" / f"{evidence_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        safe_value = _redact(value, retain_payloads=self.retain_payloads)
        payload = (
            json.dumps(safe_value, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8")
            + b"\n"
        )
        _atomic_write(path, payload)
        self.records.append(
            {
                "evidence_id": evidence_id,
                "labels": list(dict.fromkeys(labels or [slug])),
                "kind": kind if kind in _EVIDENCE_KINDS else "file",
                "ref": str(path.resolve()),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "redacted": True,
            }
        )
        return evidence_id

    def audit(self) -> list[str]:
        issues: list[str] = []
        for record in self.records:
            path = Path(record["ref"])
            if not path.is_file():
                issues.append(f"missing evidence file: {record['evidence_id']}")
                continue
            if sha256_file(path) != record["sha256"]:
                issues.append(f"evidence digest mismatch: {record['evidence_id']}")
            value = load_json(path)
            issues.extend(_redaction_issues(value, retain_payloads=self.retain_payloads))
        return issues


_EVIDENCE_KINDS = frozenset(
    {"api", "mcp", "database", "graph", "log", "trace", "metric", "file", "human_label"}
)


def _atomic_write(path: Path, payload: bytes) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _redact(value: Any, *, retain_payloads: bool) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            lowered = re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")
            key_parts = set(lowered.split("_"))
            secret_key = (
                lowered in _SECRET_KEYS
                or lowered.endswith(("_password", "_secret", "_token", "_api_key"))
                or {"private", "key"} <= key_parts
            )
            if secret_key or lowered in _PII_KEYS:
                redacted[key] = "[REDACTED]"
            elif lowered in _PAYLOAD_KEYS and not retain_payloads:
                serialized = (
                    item
                    if isinstance(item, str)
                    else json.dumps(item, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
                )
                redacted[key] = {
                    "sha256": hashlib.sha256(serialized.encode()).hexdigest(),
                    "length": len(serialized),
                }
            else:
                redacted[key] = _redact(item, retain_payloads=retain_payloads)
        return redacted
    if isinstance(value, list):
        return [_redact(item, retain_payloads=retain_payloads) for item in value]
    if isinstance(value, tuple):
        return [_redact(item, retain_payloads=retain_payloads) for item in value]
    if isinstance(value, str) and value.lower().startswith("bearer "):
        return "[REDACTED]"
    return value


def _blind_candidate_metadata(value: Any) -> Any:
    if isinstance(value, dict):
        blinded: dict[str, Any] = {}
        for key, item in value.items():
            normalized = re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")
            if normalized not in _CANDIDATE_IDENTITY_KEYS:
                blinded[key] = _blind_candidate_metadata(item)
        return blinded
    if isinstance(value, (list, tuple)):
        return [_blind_candidate_metadata(item) for item in value]
    return value


def _candidate_output_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (dict, list, tuple)):
        return bool(value) and any(
            _candidate_output_present(item)
            for item in (value.values() if isinstance(value, dict) else value)
        )
    return False


def _prepare_candidate_for_judging(
    candidate: Any,
) -> tuple[Any, list[Any], list[Any], list[Any]]:
    output_fields = ("answer", "output", "response", "text")
    if isinstance(candidate, dict):
        output_key = next((key for key in output_fields if key in candidate), None)
        if output_key is None:
            raise RunnerError("candidate response lacks a final-output field")
        candidate_output = _redact(candidate[output_key], retain_payloads=True)
        metadata = {key: value for key, value in candidate.items() if key not in output_fields}
        safe_metadata = _blind_candidate_metadata(_redact(metadata, retain_payloads=True))
        tool_trace = safe_metadata.get("tool_calls", [])
        citations = safe_metadata.get("citations", [])
        result_ids = safe_metadata.get("used_result_ids", safe_metadata.get("result_ids", []))
    else:
        candidate_output = _redact(candidate, retain_payloads=True)
        tool_trace = []
        citations = []
        result_ids = []
    if not _candidate_output_present(candidate_output):
        raise RunnerError("candidate final output is empty")
    for name, value in (
        ("tool trace", tool_trace),
        ("citations", citations),
        ("result IDs", result_ids),
    ):
        if not isinstance(value, list):
            raise RunnerError(f"candidate {name} must be an array")
    return candidate_output, tool_trace, citations, result_ids


def _redaction_issues(value: Any, *, retain_payloads: bool, path: str = "$") -> list[str]:
    issues: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")
            key_parts = set(lowered.split("_"))
            secret_key = (
                lowered in _SECRET_KEYS
                or lowered.endswith(("_password", "_secret", "_token", "_api_key"))
                or {"private", "key"} <= key_parts
                or lowered in _PII_KEYS
            )
            if secret_key and item != "[REDACTED]":
                issues.append(f"unredacted sensitive field: {path}.{key}")
            elif lowered in _PAYLOAD_KEYS and not retain_payloads:
                if not (
                    isinstance(item, dict)
                    and set(item) == {"sha256", "length"}
                    and isinstance(item["sha256"], str)
                    and isinstance(item["length"], int)
                ):
                    issues.append(f"unredacted payload field: {path}.{key}")
            else:
                issues.extend(
                    _redaction_issues(item, retain_payloads=retain_payloads, path=f"{path}.{key}")
                )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            issues.extend(
                _redaction_issues(item, retain_payloads=retain_payloads, path=f"{path}[{index}]")
            )
    elif isinstance(value, str) and value.lower().startswith("bearer "):
        issues.append(f"unredacted bearer value: {path}")
    return issues


def _lookup(root: Any, path: str) -> Any:
    current = root
    if not path:
        return current
    for raw_part in path.split("."):
        match = _PATH_PART.fullmatch(raw_part)
        if match is None or not isinstance(current, dict) or match.group(1) not in current:
            return _MISSING
        current = current[match.group(1)]
        if match.group(2) is not None:
            index = int(match.group(2))
            if not isinstance(current, list) or index >= len(current):
                return _MISSING
            current = current[index]
    return current


def _deep_merge(target: dict[str, Any], incoming: dict[str, Any]) -> None:
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = copy.deepcopy(value)


def _set_path(root: dict[str, Any], path: str, value: Any) -> None:
    current: dict[str, Any] = root
    parts = path.split(".")
    for position, raw_part in enumerate(parts):
        match = _PATH_PART.fullmatch(raw_part)
        if match is None:
            raise RunnerError(f"invalid observation path: {path}")
        name, index_text = match.groups()
        last = position == len(parts) - 1
        if index_text is None:
            if last:
                current[name] = copy.deepcopy(value)
            else:
                child = current.setdefault(name, {})
                if not isinstance(child, dict):
                    raise RunnerError(f"observation path collides with a scalar: {path}")
                current = child
            continue
        index = int(index_text)
        values = current.setdefault(name, [])
        if not isinstance(values, list):
            raise RunnerError(f"observation path collides with a scalar: {path}")
        while len(values) <= index:
            values.append({})
        if last:
            values[index] = copy.deepcopy(value)
        else:
            if not isinstance(values[index], dict):
                raise RunnerError(f"observation path collides with a scalar: {path}")
            current = values[index]


def _contains_null(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, dict):
        return any(_contains_null(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_null(item) for item in value)
    return False


def _strip_gold_labels(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_gold_labels(item)
            for key, item in value.items()
            if key not in _GOLD_LABEL_KEYS and not key.startswith("expected_")
        }
    if isinstance(value, list):
        return [_strip_gold_labels(item) for item in value]
    return copy.deepcopy(value)


def _observation_path_is_declared(path: str, declared: tuple[str, ...]) -> bool:
    path_root = re.split(r"[.[]", path, maxsplit=1)[0]
    return any(re.split(r"[.[]", root, maxsplit=1)[0] == path_root for root in declared)


def _observation_leaf_paths(value: Any, prefix: str = "") -> list[str]:
    if isinstance(value, dict):
        if not value:
            return [prefix] if prefix else []
        paths: list[str] = []
        for key, item in value.items():
            child = f"{prefix}.{key}" if prefix else key
            paths.extend(_observation_leaf_paths(item, child))
        return paths
    if isinstance(value, list):
        if not value:
            return [prefix] if prefix else []
        paths = []
        for index, item in enumerate(value):
            paths.extend(_observation_leaf_paths(item, f"{prefix}[{index}]"))
        return paths
    return [prefix] if prefix else []


def _observation_contract_errors(
    observations: dict[str, Any], declared: tuple[str, ...]
) -> list[str]:
    unexpected = sorted(
        path
        for path in _observation_leaf_paths(observations)
        if not _observation_path_is_declared(path, declared)
    )
    missing = sorted(path for path in declared if _lookup(observations, path) is _MISSING)
    errors: list[str] = []
    if unexpected:
        errors.append(f"undeclared observation paths: {unexpected}")
    if missing:
        errors.append(f"omitted observations: {missing}")
    return errors


def _resolve_reference(
    reference: str,
    *,
    fixture: dict[str, Any],
    observations: dict[str, Any],
    run_context: dict[str, Any],
) -> Any:
    roots: list[tuple[str, Any]] = [
        ("fixture.", fixture),
        ("run_context.", run_context),
    ]
    for prefix, root in roots:
        if reference.startswith(prefix):
            resolved = _lookup(root, reference.removeprefix(prefix))
            if resolved is _MISSING:
                raise InputResolutionError(f"unresolved reference: {reference}")
            return copy.deepcopy(resolved)
    for root in (observations, fixture, run_context):
        resolved = _lookup(root, reference)
        if resolved is not _MISSING:
            return copy.deepcopy(resolved)
    raise InputResolutionError(f"unresolved reference: {reference}")


def _resolve_inputs(
    declared: dict[str, Any],
    *,
    fixture: dict[str, Any],
    observations: dict[str, Any],
    run_context: dict[str, Any],
) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for key, value in declared.items():
        if key == "fixture_file" and isinstance(value, str):
            path = (ROOT.parent / value).resolve()
            try:
                path.relative_to(ROOT.parent.resolve())
            except ValueError as exc:
                raise InputResolutionError(
                    f"fixture file escapes the evaluation root: {value}"
                ) from exc
            if not path.is_file():
                raise InputResolutionError(f"fixture file does not exist: {value}")
            resolved[key] = {
                "path": value,
                "sha256": sha256_file(path),
                "data": load_json(path),
            }
        elif key == "fixture" and isinstance(value, str):
            fixture_reference = value.removeprefix("fixture.")
            item = _lookup(fixture, fixture_reference)
            if item is _MISSING:
                raise InputResolutionError(f"unresolved fixture reference: {value}")
            resolved[key] = copy.deepcopy(item)
        elif key.endswith("_ref") and isinstance(value, str):
            item = _resolve_reference(
                value,
                fixture=fixture,
                observations=observations,
                run_context=run_context,
            )
            if key in {"matrix_ref", "targets_ref"} and _contains_null(item):
                raise InputResolutionError(f"{key} contains unresolved null target values")
            resolved[key] = item
        else:
            resolved[key] = copy.deepcopy(value)
    return resolved


def _expected_value(expected: Any, observations: dict[str, Any]) -> Any:
    if isinstance(expected, dict) and set(expected) == {"observation_ref"}:
        reference = expected["observation_ref"]
        if not isinstance(reference, str):
            return _MISSING
        return _lookup(observations, reference)
    return expected


def _assertion_passes(
    assertion: dict[str, Any], observations: dict[str, Any]
) -> tuple[bool, Any, str]:
    target = str(assertion["target"])
    operator = str(assertion["operator"])
    observed = _lookup(observations, target)
    if operator == "unchanged" and target.endswith(".after") and observed is not _MISSING:
        before = _lookup(observations, f"{target.removesuffix('.after')}.before")
        if before is not _MISSING:
            observed = {"before": before, "after": observed}
    expected = _expected_value(assertion.get("expected"), observations)
    observed_present = observed is not _MISSING
    expected_present = expected is not _MISSING
    report_observed = None if not observed_present else observed
    passed = assertion_passes(
        operator,
        target,
        report_observed,
        None if not expected_present else expected,
        observed_present=observed_present,
        expected_present=expected_present,
    )
    return passed, report_observed, f"operator={operator}"


def _status_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(item["status"] for item in results)
    return {
        "selected": len(results),
        "pass": counts["PASS"],
        "fail": counts["FAIL"],
        "blocked": counts["BLOCKED"],
        "not_applicable": counts["NOT_APPLICABLE"],
    }


def _case_status(assertions: list[dict[str, Any]]) -> str:
    statuses = [item["status"] for item in assertions]
    if "FAIL" in statuses:
        return "FAIL"
    if "BLOCKED" in statuses:
        return "BLOCKED"
    if statuses and all(status == "NOT_APPLICABLE" for status in statuses):
        return "NOT_APPLICABLE"
    return "PASS"


def _blocked_case_result(
    case: dict[str, Any],
    reason: str,
    evidence_ids: list[str],
    *,
    baseline_present: bool,
) -> dict[str, Any]:
    return {
        "case_id": case["case_id"],
        "status": "BLOCKED",
        "quality_status": (
            "NOT_ELIGIBLE" if case.get("mode") == "qualitative" else "NOT_REQUESTED"
        ),
        "duration_ms": 0.0,
        "assertion_results": [
            {
                "assertion_id": assertion["id"],
                "status": (
                    "NOT_APPLICABLE"
                    if assertion["activation"] == "baseline_required" and not baseline_present
                    else "BLOCKED"
                ),
                "target": assertion["target"],
                "operator": assertion["operator"],
                "expected": assertion["expected"],
                "resolved_expected": (
                    None
                    if isinstance(assertion["expected"], dict)
                    and set(assertion["expected"]) == {"observation_ref"}
                    else assertion["expected"]
                ),
                "observed_present": False,
                "expected_present": not (
                    isinstance(assertion["expected"], dict)
                    and set(assertion["expected"]) == {"observation_ref"}
                ),
                "evaluation_kind": (
                    "not_applicable"
                    if assertion["activation"] == "baseline_required" and not baseline_present
                    else "blocked"
                ),
                "observed": None,
                "evidence_ids": evidence_ids,
                "notes": reason,
            }
            for assertion in case["assertions"]
        ],
        "metric_ids": [],
        "evidence_ids": evidence_ids,
        "first_bad_boundary": None,
        "root_cause_confidence": 0.0,
        "blocked_reason": reason,
    }


def _normalize_metric(
    metric: dict[str, Any],
    *,
    owner_id: str,
    metric_id: str,
    declared_names: set[str],
) -> dict[str, Any]:
    allowed = {"name", "dimensions", "unit", "value", "sample_size"}
    unknown = set(metric) - allowed
    if unknown:
        raise AdapterProtocolError(
            f"adapter metric has runner-owned or unknown fields: {sorted(unknown)}"
        )
    name = metric.get("name")
    unit = metric.get("unit")
    if not isinstance(name, str) or not name or not isinstance(unit, str) or not unit:
        raise AdapterProtocolError("metric requires non-empty name and unit")
    if name not in declared_names:
        raise AdapterProtocolError(f"adapter returned undeclared metric: {name}")
    dimensions = metric.get("dimensions", {})
    if not isinstance(dimensions, dict):
        raise AdapterProtocolError("metric dimensions must be an object")
    sample_size = metric.get("sample_size", 0)
    if not isinstance(sample_size, int) or isinstance(sample_size, bool) or sample_size < 0:
        raise AdapterProtocolError("metric sample_size must be a non-negative integer")
    value = metric.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AdapterProtocolError("adapter metric value must be numeric")
    return {
        "metric_id": metric_id,
        "owner_id": owner_id,
        "name": name,
        "dimensions": dimensions,
        "unit": unit,
        "value": value,
        "sample_size": sample_size,
        "baseline": None,
        "delta_absolute": None,
        "delta_relative": None,
        "comparator": None,
        "threshold": None,
        "threshold_source": None,
        "status": "PASS",
    }


def _canonical_value(value: Any) -> str:
    if isinstance(value, dict):
        normalized = {
            key: json.loads(_canonical_value(item)) for key, item in sorted(value.items())
        }
    elif isinstance(value, list):
        items = [json.loads(_canonical_value(item)) for item in value]
        normalized = sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
    else:
        normalized = value
    return json.dumps(normalized, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _snapshot_items(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("facts", "assertions", "edges", "items", "records"):
            items = value.get(key)
            if isinstance(items, list):
                return items
        return [{"key": key, "value": item} for key, item in sorted(value.items())]
    return [value]


def _resolve_snapshot_input(
    value: Any,
    *,
    fixture: dict[str, Any],
    observations: dict[str, Any],
) -> Any:
    if not isinstance(value, str):
        return value
    if value == "graph":
        recovered_graph = _lookup(observations, "recovery.graph")
        if recovered_graph is not _MISSING:
            return recovered_graph
    resolved = _lookup(observations, value)
    if resolved is not _MISSING:
        return resolved
    if value == "expected_fixture":
        profile_expected = _lookup(observations, "profiles.expected_fixture")
        recovery_expected = _lookup(observations, "recovery.expected")
        if recovery_expected is not _MISSING:
            return recovery_expected
        return fixture if profile_expected is _MISSING else profile_expected
    raise InputResolutionError(f"runner cannot resolve parity snapshot: {value}")


def _parity_observations(
    inputs: dict[str, Any],
    *,
    fixture: dict[str, Any],
    observations: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    policy = inputs.get("comparison_policy")
    before_input = inputs.get("before_ref", inputs.get("before_snapshot", _MISSING))
    after_input = inputs.get("after_ref", inputs.get("after_snapshot", _MISSING))
    if before_input is _MISSING or after_input is _MISSING:
        raise InputResolutionError("parity.verify requires before and after snapshots")
    before = _resolve_snapshot_input(before_input, fixture=fixture, observations=observations)
    after = _resolve_snapshot_input(after_input, fixture=fixture, observations=observations)
    if policy == "temporal-v1":
        aliases = {
            "current": "current_search",
            "as_of": "as_of_search",
            "intervals": "intervals",
            "provenance": "provenance",
        }
        if isinstance(before, dict) and isinstance(after, dict):
            parity = {
                name: _canonical_value(before.get(source)) == _canonical_value(after.get(source))
                for name, source in aliases.items()
            }
        else:
            equivalent = _canonical_value(before) == _canonical_value(after)
            parity = dict.fromkeys(aliases, equivalent)
    elif policy in {"ingestion-v1", "projection-v1"}:
        expected = Counter(_canonical_value(item) for item in _snapshot_items(before))
        actual = Counter(_canonical_value(item) for item in _snapshot_items(after))
        missing_count = sum((expected - actual).values())
        duplicate_count = sum((actual - expected).values())
        if policy == "ingestion-v1":
            parity = {
                "missing_fact_count": missing_count,
                "duplicate_fact_count": duplicate_count,
            }
        else:
            parity = {"missing_count": missing_count, "duplicate_count": duplicate_count}
    else:
        raise InputResolutionError(f"unsupported parity comparison policy: {policy!r}")
    return {"parity": parity}, {
        "policy": policy,
        "before": before,
        "after": after,
        "parity": parity,
    }


def _claim_triples(value: Any) -> tuple[list[dict[str, str]], int]:
    triples: list[dict[str, str]] = []
    invalid = 0

    def collect(item: Any) -> None:
        nonlocal invalid
        if isinstance(item, list):
            for child in item:
                collect(child)
            return
        if isinstance(item, dict) and "triple" in item:
            collect(item["triple"])
            return
        if isinstance(item, dict) and {"subject", "predicate", "object"} <= set(item):
            triple = {key: item[key] for key in ("subject", "predicate", "object")}
            if all(isinstance(part, str) and part for part in triple.values()):
                triples.append(triple)
            else:
                invalid += 1
            return
        if isinstance(item, dict):
            for child in item.values():
                collect(child)
            return
        invalid += 1

    collect(value)
    return triples, invalid


def _metric(name: str, value: float | int, sample_size: int, unit: str) -> dict[str, Any]:
    return {"name": name, "unit": unit, "value": value, "sample_size": sample_size}


def _extraction_score(
    fixture: dict[str, Any], observations: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    expected, _ = _claim_triples(
        [record.get("expected_triples", []) for record in fixture.get("records", [])]
    )
    actual_value = _lookup(observations, "actual_claims")
    if actual_value is _MISSING:
        raise InputResolutionError("extraction scoring requires actual_claims")
    actual, invalid = _claim_triples(actual_value)
    expected_counts = Counter(_canonical_value(item) for item in expected)
    actual_counts = Counter(_canonical_value(item) for item in actual)
    true_positives = sum((expected_counts & actual_counts).values())
    precision = true_positives / len(actual) if actual else (1.0 if not expected else 0.0)
    recall = true_positives / len(expected) if expected else 1.0
    yield_value = len(actual) / len(fixture.get("records", [])) if fixture.get("records") else 0.0
    sample_size = len(expected)
    return (
        {"schema_invalid_count": invalid},
        [
            _metric("claim_precision", precision, sample_size, "ratio"),
            _metric("claim_recall", recall, sample_size, "ratio"),
            _metric("extraction_yield", yield_value, len(fixture.get("records", [])), "ratio"),
            _metric("schema_invalid_count", invalid, len(actual) + invalid, "count"),
        ],
    )


def _ranked_result_ids(value: Any) -> dict[str, list[str]]:
    ranked: dict[str, list[str]] = {}
    if isinstance(value, dict):
        for query_id, results in value.items():
            if isinstance(results, dict):
                results = results.get("results", results.get("ranked_results", []))
            if isinstance(results, list):
                ranked[str(query_id)] = [
                    str(item.get("fact_id", item.get("id", item)))
                    if isinstance(item, dict)
                    else str(item)
                    for item in results
                ]
        return ranked
    if isinstance(value, list):
        for index, item in enumerate(value):
            if not isinstance(item, dict):
                continue
            query_id = str(item.get("query_id", index))
            results = item.get("results", item.get("ranked_results", []))
            if isinstance(results, list):
                ranked[query_id] = [
                    str(result.get("fact_id", result.get("id", result)))
                    if isinstance(result, dict)
                    else str(result)
                    for result in results
                ]
    return ranked


def _retrieval_score(
    fixture: dict[str, Any], observations: dict[str, Any], *, k: int
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ranked_value = _lookup(observations, "ranked_results")
    if ranked_value is _MISSING:
        raise InputResolutionError("retrieval scoring requires ranked_results")
    ranked = _ranked_result_ids(ranked_value)
    positive_queries = [query for query in fixture.get("queries", []) if query.get("relevance")]
    hits_at_1 = 0
    hits_at_k = 0
    reciprocal_ranks: list[float] = []
    ndcg_values: list[float] = []
    critical_misses = 0
    for query in positive_queries:
        relevant = set(query["relevance"])
        result_ids = ranked.get(str(query["query_id"]), [])
        relevant_ranks = [index + 1 for index, item in enumerate(result_ids) if item in relevant]
        if result_ids[:1] and result_ids[0] in relevant:
            hits_at_1 += 1
        if any(rank <= k for rank in relevant_ranks):
            hits_at_k += 1
        else:
            critical_misses += 1
        reciprocal_ranks.append(0.0 if not relevant_ranks else 1.0 / min(relevant_ranks))
        gains = [
            1.0 / math.log2(index + 2)
            for index, item in enumerate(result_ids[:k])
            if item in relevant
        ]
        ideal = [1.0 / math.log2(index + 2) for index in range(min(k, len(relevant)))]
        ndcg_values.append(sum(gains) / sum(ideal) if ideal else 1.0)
    count = len(positive_queries)

    def ratio(value: int) -> float:
        return value / count if count else 1.0

    return (
        {"critical_miss_count": critical_misses},
        [
            _metric("hit_at_1", ratio(hits_at_1), count, "ratio"),
            _metric("hit_at_5", ratio(hits_at_k), count, "ratio"),
            _metric("mrr", sum(reciprocal_ranks) / count if count else 1.0, count, "ratio"),
            _metric("ndcg_at_5", sum(ndcg_values) / count if count else 1.0, count, "ratio"),
        ],
    )


def _text_values(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [text for item in value for text in _text_values(item)]
    if isinstance(value, dict):
        return [text for item in value.values() for text in _text_values(item)]
    return []


def _answer_score(
    fixture: dict[str, Any], observations: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    questions = fixture.get("questions", fixture.get("queries", []))
    runs_value = _lookup(observations, "runs")
    if isinstance(questions, list) and isinstance(runs_value, list) and runs_value:
        labeled: list[tuple[str | None, list[str]]] = []
        for question in questions:
            if not isinstance(question, dict):
                raise InputResolutionError("answer scoring requires object question labels")
            expected = question.get("expected")
            if isinstance(expected, str):
                labels = [expected.casefold()]
            elif (
                isinstance(expected, list)
                and expected
                and all(isinstance(value, str) for value in expected)
            ):
                labels = [str(value).casefold() for value in expected]
            else:
                raise InputResolutionError("answer scoring requires expected labels per question")
            query_id = question.get("query_id")
            labeled.append((str(query_id) if isinstance(query_id, str) else None, labels))

        query_indexes = {
            query_id: index
            for index, (query_id, _labels) in enumerate(labeled)
            if query_id is not None
        }
        accepted_count = 0
        accepted_tokens = 0
        accepted_usage_complete = True
        material_errors = 0
        unsupported_claim_count = 0
        supported_labels = 0
        factual_label_count = 0
        factual_runs = 0
        grounded_factual_runs = 0
        cited_count = 0
        valid_cited_count = 0

        for run_value in runs_value:
            if not isinstance(run_value, dict):
                raise InputResolutionError("answer scoring requires object runs")
            query_id = run_value.get("query_id")
            question_index = (
                query_indexes.get(query_id)
                if isinstance(query_id, str)
                else run_value.get("question_index")
            )
            if (
                isinstance(question_index, bool)
                or not isinstance(question_index, int)
                or not 0 <= question_index < len(labeled)
            ):
                raise InputResolutionError("answer run omitted a valid question identity")
            expected = labeled[question_index][1]
            answer = run_value.get("answer")
            abstained = run_value.get("abstained")
            if not isinstance(answer, str) or not isinstance(abstained, bool):
                raise InputResolutionError("answer run omitted answer or abstention state")
            answer_text = answer.casefold()
            used_ids = {
                str(value)
                for value in run_value.get("used_result_ids", [])
                if isinstance(value, str)
            }
            citations = run_value.get("citations", [])
            cited_ids = {
                str(item["result_id"])
                for item in citations
                if isinstance(item, dict) and isinstance(item.get("result_id"), str)
            }
            cited_count += len(cited_ids)
            valid_cited_count += len(cited_ids & used_ids)
            run_unsupported = run_value.get("unsupported_claim_count", 0)
            if isinstance(run_unsupported, bool) or not isinstance(run_unsupported, int):
                raise InputResolutionError("answer run has invalid unsupported_claim_count")

            expects_abstention = expected == ["abstain"]
            if expects_abstention:
                correct = abstained
                unsupported_claim_count += max(run_unsupported, int(not abstained))
            else:
                factual_runs += 1
                factual_label_count += len(expected)
                matched = sum(label in answer_text for label in expected)
                supported_labels += matched
                grounded = bool(used_ids) and bool(cited_ids) and cited_ids <= used_ids
                grounded_factual_runs += int(grounded)
                correct = matched == len(expected) and not abstained and grounded
                unsupported_claim_count += run_unsupported
            accepted = correct and run_unsupported == 0
            accepted_count += int(accepted)
            material_errors += int(not accepted)
            if accepted:
                usage = run_value.get("token_usage")
                if not isinstance(usage, dict):
                    accepted_usage_complete = False
                else:
                    total = usage.get("total_tokens")
                    if isinstance(total, bool) or not isinstance(total, int):
                        prompt = usage.get("prompt_tokens")
                        completion = usage.get("completion_tokens")
                        if (
                            isinstance(prompt, bool)
                            or not isinstance(prompt, int)
                            or isinstance(completion, bool)
                            or not isinstance(completion, int)
                        ):
                            accepted_usage_complete = False
                        else:
                            total = prompt + completion
                    if isinstance(total, int) and not isinstance(total, bool):
                        accepted_tokens += total

        run_count = len(runs_value)
        completeness = supported_labels / factual_label_count if factual_label_count else 1.0
        grounded_precision = grounded_factual_runs / factual_runs if factual_runs else 1.0
        citation_correctness = (
            valid_cited_count / cited_count if cited_count else float(factual_runs == 0)
        )
        citation_coverage = grounded_precision
        metrics = [
            _metric("grounded_claim_precision", grounded_precision, factual_runs, "ratio"),
            _metric("answer_completeness", completeness, factual_label_count, "ratio"),
            _metric("citation_correctness", citation_correctness, cited_count, "ratio"),
            _metric("citation_coverage", citation_coverage, factual_runs, "ratio"),
            _metric("temporal_answer_accuracy", completeness, run_count, "ratio"),
            _metric("task_success", accepted_count / run_count, run_count, "ratio"),
        ]
        if accepted_count and accepted_usage_complete:
            metrics.append(
                _metric(
                    "tokens_per_accepted_answer",
                    accepted_tokens / accepted_count,
                    accepted_count,
                    "tokens/answer",
                )
            )
        return (
            {
                "citation_correctness": citation_correctness,
                "grounded_claim_precision": grounded_precision,
                "unsupported_claim_count": unsupported_claim_count,
                "material_error_count": material_errors,
            },
            metrics,
        )

    answer_value = _lookup(observations, "answers")
    if answer_value is _MISSING:
        answer_value = _lookup(observations, "agent.answer")
    if answer_value is _MISSING:
        answer_value = _lookup(observations, "runs")
    answer_text = "\n".join(_text_values(answer_value)).casefold()
    expected_answers = [
        str(item["expected"]).casefold()
        for item in questions
        if isinstance(item, dict) and isinstance(item.get("expected"), str)
    ]
    factual = [answer for answer in expected_answers if answer != "abstain"]
    supported = sum(answer in answer_text for answer in factual)
    abstention_expected = "abstain" in expected_answers
    structured_abstention = any(
        _lookup(observations, path) is True
        for path in ("answers.budget.abstained", "agent.abstained")
    )
    abstained = structured_abstention or any(
        token in answer_text for token in ("abstain", "unknown", "not available")
    )
    unsupported_claim_count = int(abstention_expected and not abstained)
    result_ids_value = _lookup(observations, "result_ids")
    if result_ids_value is _MISSING:
        result_ids_value = _lookup(observations, "agent.used_result_ids")
    result_ids = set(_text_values(result_ids_value))
    citations_value = _lookup(observations, "citations")
    if citations_value is _MISSING:
        citations_value = _lookup(observations, "agent.citations")
    citations = citations_value if isinstance(citations_value, list) else []
    cited_ids = {
        str(item.get("result_id"))
        for item in citations
        if isinstance(item, dict) and item.get("result_id") is not None
    }
    citation_correctness = len(cited_ids & result_ids) / len(cited_ids) if cited_ids else 0.0
    completeness = supported / len(factual) if factual else 1.0
    citation_coverage = min(1.0, len(cited_ids & result_ids) / len(factual)) if factual else 1.0
    sample_size = max(1, len(questions))
    material_errors = len(factual) - supported + unsupported_claim_count
    return (
        {
            "citation_correctness": citation_correctness,
            "grounded_claim_precision": completeness,
            "unsupported_claim_count": unsupported_claim_count,
            "material_error_count": material_errors,
        },
        [
            _metric("grounded_claim_precision", completeness, sample_size, "ratio"),
            _metric("answer_completeness", completeness, sample_size, "ratio"),
            _metric("citation_correctness", citation_correctness, len(cited_ids), "ratio"),
            _metric("citation_coverage", citation_coverage, sample_size, "ratio"),
            _metric("temporal_answer_accuracy", completeness, len(factual), "ratio"),
            _metric("task_success", int(material_errors == 0), sample_size, "ratio"),
        ],
    )


def _outcome_score(
    fixture: dict[str, Any], observations: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    answer_value = _lookup(observations, "agent.answer")
    if answer_value is _MISSING:
        raise InputResolutionError("outcome scoring requires agent.answer")
    answer_text = "\n".join(_text_values(answer_value)).casefold()
    required = {
        str(part).casefold()
        for fact in fixture.get("facts", [])
        if isinstance(fact, dict)
        for key, part in fact.get("triple", {}).items()
        if key in {"subject", "object"}
        if isinstance(part, str) and part != "Payment API"
    }
    missing = sorted(part for part in required if part not in answer_text)
    citations = _lookup(observations, "agent.citations")
    evidence_complete = isinstance(citations, list) and bool(citations)
    task_completed = not missing
    return (
        {
            "task_completed": task_completed,
            "material_error_count": len(missing),
            "evidence_complete": evidence_complete,
        },
        [_metric("task_success", int(task_completed and evidence_complete), 1, "ratio")],
    )


def _execute_runner_action(
    action_name: str,
    inputs: dict[str, Any],
    *,
    fixture: dict[str, Any],
    observations: dict[str, Any],
    declared_metrics: set[str],
    evidence_labels: list[str],
) -> ActionResponse:
    try:
        if action_name == "parity.verify":
            derived, evidence_payload = _parity_observations(
                inputs, fixture=fixture, observations=observations
            )
            metrics: list[dict[str, Any]] = []
            parity = cast(dict[str, Any], derived["parity"])
            policy = inputs.get("comparison_policy")
            if policy == "temporal-v1":
                parity_value = float(all(value is True for value in parity.values()))
                metrics.extend(
                    (
                        _metric("projection_parity", parity_value, 1, "ratio"),
                        _metric("temporal_parity", parity_value, 1, "ratio"),
                    )
                )
                rebuild_duration = _lookup(observations, "rebuild.duration_ms")
                if isinstance(rebuild_duration, (int, float)) and not isinstance(
                    rebuild_duration, bool
                ):
                    metrics.append(_metric("rebuild_duration_ms", float(rebuild_duration), 1, "ms"))
            elif policy in {"ingestion-v1", "projection-v1"}:
                missing_key = "missing_fact_count" if policy == "ingestion-v1" else "missing_count"
                duplicate_key = (
                    "duplicate_fact_count" if policy == "ingestion-v1" else "duplicate_count"
                )
                parity_value = float(
                    parity.get(missing_key) == 0 and parity.get(duplicate_key) == 0
                )
                metrics.append(_metric("projection_parity", parity_value, 1, "ratio"))
        elif action_name == "result.score":
            rubric = inputs.get("rubric_version")
            if rubric == "extraction-v1":
                score, metrics = _extraction_score(fixture, observations)
            elif rubric == "retrieval-v1":
                k = inputs.get("k", 5)
                if not isinstance(k, int) or isinstance(k, bool) or k < 1:
                    raise InputResolutionError("retrieval scoring requires a positive integer k")
                score, metrics = _retrieval_score(fixture, observations, k=k)
            elif rubric == "answer-v1":
                score, metrics = _answer_score(fixture, observations)
            elif rubric == "outcome-v1":
                score, metrics = _outcome_score(fixture, observations)
            else:
                raise InputResolutionError(f"unsupported scoring rubric: {rubric!r}")
            derived = {"score": score}
            evidence_payload = {"rubric_version": rubric, "score": score}
        else:
            raise InputResolutionError(f"unsupported runner action: {action_name}")
    except InputResolutionError as exc:
        return ActionResponse(status="FAIL", message=str(exc))
    selected_metrics = tuple(metric for metric in metrics if metric["name"] in declared_metrics)
    descriptors = tuple(
        {"label": label, "kind": "file", **evidence_payload} for label in evidence_labels
    )
    return ActionResponse(
        status="PASS",
        observations=derived,
        metrics=selected_metrics,
        evidence=descriptors,
    )


def _runner_owned_metric_names(case: dict[str, Any]) -> set[str]:
    owned: set[str] = set()
    for step in case["steps"]:
        if step["action"] == "parity.verify":
            policy = step["input"].get("comparison_policy")
            if policy == "temporal-v1":
                owned.update({"projection_parity", "temporal_parity", "rebuild_duration_ms"})
            elif policy in {"ingestion-v1", "projection-v1"}:
                owned.add("projection_parity")
            continue
        if step["action"] != "result.score":
            continue
        rubric = step["input"].get("rubric_version")
        if isinstance(rubric, str):
            owned.update(_RUNNER_METRICS_BY_RUBRIC.get(rubric, ()))
    return owned & set(case.get("metrics", []))


def _baseline_delta(target: str, current: float, baseline: float) -> float | None:
    if target.endswith("_relative_delta"):
        return None if baseline == 0 else (current - baseline) / abs(baseline)
    if target.endswith("_delta_pp"):
        return (current - baseline) * 100
    return current - baseline


class EvaluationRunner:
    def __init__(
        self,
        config: RunConfig,
        driver: ActionDriver | None = None,
        *,
        root: Path = ROOT,
    ) -> None:
        self._config = config
        self._driver = driver or UnavailableActionDriver()
        self._root = root
        errors, _, _, _ = validate_contracts()
        if errors:
            raise RunnerError("evaluation contracts are invalid:\n" + "\n".join(errors))
        catalog = load_json(root / "action_catalog.json")
        checklist = load_json(root / "checklist.json")
        self._actions = {item["name"]: item for item in catalog["actions"]}
        self._checks: list[dict[str, Any]] = checklist["items"]
        self._cases = load_cases()
        self._metric_counter = 0
        self._mutation_attempted = False
        self._judge_packets: list[dict[str, Any]] = []
        self._budget_started = 0.0
        self._duration_limit_s: float | None = None
        self._run_deadline: float | None = None
        self._cost_limit_usd: float | None = None
        self._action_cost_limit_usd: float | None = None
        self._cost_usd = 0.0
        self._cost_complete = True
        self._budget_reason: str | None = None

    def _start_budget(self) -> None:
        self._budget_started = time.monotonic()
        budget = self._config.run_context.get("cost_budget")
        if not isinstance(budget, dict):
            return
        duration = budget.get("max_duration_s")
        cost = budget.get("max_cost_usd")
        action_cost = budget.get("max_action_cost_usd")
        if (
            not isinstance(duration, bool)
            and isinstance(duration, (int, float))
            and math.isfinite(duration)
            and duration > 0
        ):
            self._duration_limit_s = float(duration)
            self._run_deadline = self._budget_started + self._duration_limit_s
        if (
            not isinstance(cost, bool)
            and isinstance(cost, (int, float))
            and math.isfinite(cost)
            and cost > 0
        ):
            self._cost_limit_usd = float(cost)
        if (
            not isinstance(action_cost, bool)
            and isinstance(action_cost, (int, float))
            and math.isfinite(action_cost)
            and action_cost > 0
        ):
            self._action_cost_limit_usd = float(action_cost)

    def _cost_admission_reason(self, action_name: str) -> str | None:
        if self._cost_limit_usd is None or self._action_cost_limit_usd is None:
            return None
        remaining = self._cost_limit_usd - self._cost_usd
        if remaining + 1e-12 >= self._action_cost_limit_usd:
            return None
        self._budget_reason = (
            f"insufficient cost budget to reserve {self._action_cost_limit_usd:.6f} USD "
            f"for {action_name}: {max(0.0, remaining):.6f} USD remains"
        )
        return self._budget_reason

    def _budgeted_run_context(self, context: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(context)
        budget = result.get("cost_budget")
        if not isinstance(budget, dict):
            return result
        if self._cost_limit_usd is not None:
            budget["remaining_cost_usd"] = max(0.0, self._cost_limit_usd - self._cost_usd)
        if self._action_cost_limit_usd is not None:
            budget["reserved_action_cost_usd"] = self._action_cost_limit_usd
        return result

    def _bounded_timeout(self, requested_s: float) -> float:
        if self._run_deadline is None:
            return requested_s
        remaining = self._run_deadline - time.monotonic()
        if remaining <= 0:
            self._budget_reason = "run duration budget was exhausted"
            raise TimeoutError(self._budget_reason)
        return min(requested_s, remaining)

    def _duration_exhausted(self) -> bool:
        return self._run_deadline is not None and time.monotonic() >= self._run_deadline

    def _record_action_cost(
        self, action_name: str, response: ActionResponse, *, required: bool
    ) -> str | None:
        if not required:
            return None
        cost = response.cost_usd
        if cost is None:
            self._cost_complete = False
            if self._budget_reason is None:
                self._budget_reason = f"{action_name} omitted complete provider cost reporting"
            return self._budget_reason
        if not math.isfinite(cost) or cost < 0:
            self._cost_complete = False
            if self._budget_reason is None:
                self._budget_reason = f"{action_name} reported invalid provider cost"
            return self._budget_reason
        self._cost_usd += cost
        if self._cost_limit_usd is not None and self._cost_usd > self._cost_limit_usd:
            self._budget_reason = (
                f"run cost budget exceeded: {self._cost_usd:.6f} USD used, "
                f"{self._cost_limit_usd:.6f} USD allowed"
            )
            return self._budget_reason
        if self._action_cost_limit_usd is not None and cost > self._action_cost_limit_usd:
            self._budget_reason = (
                f"{action_name} exceeded its reserved action cost: {cost:.6f} USD used, "
                f"{self._action_cost_limit_usd:.6f} USD reserved"
            )
            return self._budget_reason
        return None

    async def run(self) -> RunOutcome:
        started = datetime.now(UTC)
        self._start_budget()
        run_id = self._config.run_id or (
            f"{started.strftime('%Y%m%dT%H%M%SZ')}-{self._config.git_sha[:8]}-"
            f"{self._config.profile}"
        )
        if not _SAFE_RUN_ID.fullmatch(run_id):
            raise RunnerError("generated run_id is unsafe")
        selected_cases = _order_cases(
            [case for case in self._cases if self._config.profile in case["profiles"]],
            load_case_dependencies(self._root),
        )
        selected_checks = [
            check for check in self._checks if self._config.profile in check["profiles"]
        ]
        selection = {
            "check_ids": [check["id"] for check in selected_checks],
            "case_ids": [case["case_id"] for case in selected_cases],
        }
        manifest = self._build_manifest(started, selection)
        run_dir = self._config.output_root / run_id
        if run_dir.exists():
            raise RunnerError(f"run directory already exists: {run_dir}")
        run_dir.mkdir(parents=True)
        evidence = EvidenceStore(run_dir, self._config.retain_synthetic_payloads)
        framework_labels = sorted(
            {
                label
                for check in selected_checks
                if check["id"] in _FRAMEWORK_CHECKS and check["id"] not in {"PRE-003", "OBS-004"}
                for label in check["evidence"]
            }
        )
        framework_evidence_id = evidence.write(
            "frozen-run-manifest",
            {"manifest": manifest, "selection": selection},
            labels=framework_labels,
        )

        case_results: list[dict[str, Any]] = []
        metrics: list[dict[str, Any]] = []
        created_resources: list[str] = []
        mutation_attempted = False
        self._mutation_attempted = False
        self._judge_packets = []
        cleanup: dict[str, Any]
        cleanup_evidence: list[str]
        try:
            preflight = await self._preflight(run_id, selected_cases, evidence, manifest)
            stop_reason: str | None = None
            if preflight["status"] != "PASS":
                stop_reason = preflight["reason"]
            for case in selected_cases:
                if stop_reason is None and self._duration_exhausted():
                    self._budget_reason = "run duration budget was exhausted"
                    stop_reason = self._budget_reason
                if stop_reason is not None:
                    case_results.append(
                        _blocked_case_result(
                            case,
                            stop_reason,
                            preflight["evidence_ids"],
                            baseline_present=self._config.baseline_path is not None,
                        )
                    )
                    continue
                (
                    case_result,
                    case_metrics,
                    case_resources,
                    case_mutated,
                ) = await self._run_case(run_id, case, evidence, manifest)
                case_results.append(case_result)
                metrics.extend(case_metrics)
                created_resources.extend(case_resources)
                mutation_attempted = mutation_attempted or case_mutated
                if self._budget_reason is not None:
                    stop_reason = self._budget_reason
                assertions = {item["id"]: item for item in case["assertions"]}
                blocker = next(
                    (
                        item
                        for item in case_result["assertion_results"]
                        if item["status"] == "FAIL"
                        and assertions[item["assertion_id"]]["severity"] == "BLOCKER"
                    ),
                    None,
                )
                if blocker is not None:
                    stop_reason = (
                        f"execution stopped after BLOCKER assertion "
                        f"{case['case_id']}/{blocker['assertion_id']} failed"
                    )
        finally:
            cleanup_task = asyncio.create_task(
                self._cleanup(
                    run_id,
                    created_resources,
                    evidence,
                    manifest,
                    mutation_attempted=mutation_attempted or self._mutation_attempted,
                )
            )
            cleanup, cleanup_evidence = await finish_finalizer(cleanup_task)
        redaction_issues = evidence.audit()
        redaction_evidence_id = evidence.write(
            "artifact-safety-scan",
            {"passed": not redaction_issues, "issues": redaction_issues},
            labels=["redaction scan", "secret scan", "pseudonymization and retention policy"],
        )
        artifact_safety = {
            "status": "PASS" if not redaction_issues else "FAIL",
            "reason": (
                "all retained evidence passed integrity and redaction scans"
                if not redaction_issues
                else "; ".join(redaction_issues)
            ),
            "evidence_ids": [redaction_evidence_id],
        }
        check_results = await self._evaluate_checks(
            run_id=run_id,
            checks=selected_checks,
            case_results=case_results,
            cleanup=cleanup,
            evidence=evidence,
            manifest=manifest,
            framework_evidence_id=framework_evidence_id,
            preflight=preflight,
            artifact_safety=artifact_safety,
        )
        findings = self._build_findings(
            case_results=case_results,
            check_results=check_results,
            selected_checks=selected_checks,
            framework_evidence_id=framework_evidence_id,
        )

        gating_fail = (
            any(
                result["status"] == "FAIL"
                and next(check for check in selected_checks if check["id"] == result["check_id"])[
                    "priority"
                ]
                == "P0"
                for result in check_results
            )
            or cleanup["status"] == "FAIL"
        )
        gating_blocked = (
            any(
                result["status"] == "BLOCKED"
                and next(check for check in selected_checks if check["id"] == result["check_id"])[
                    "priority"
                ]
                == "P0"
                for result in check_results
            )
            or cleanup["status"] == "BLOCKED"
            or self._budget_reason is not None
        )
        case_by_id = {case["case_id"]: case for case in selected_cases}
        result_by_id = {result["case_id"]: result for result in case_results}
        for case_id, case in case_by_id.items():
            assertion_defs = {item["id"]: item for item in case["assertions"]}
            for assertion_result in result_by_id[case_id]["assertion_results"]:
                declared = assertion_defs[assertion_result["assertion_id"]]
                if declared["gate"] is True:
                    gating_fail = gating_fail or assertion_result["status"] == "FAIL"
                    gating_blocked = gating_blocked or assertion_result["status"] == "BLOCKED"
        if gating_fail:
            status = "FAIL"
        elif gating_blocked:
            status = "BLOCKED"
        else:
            status = "PASS"
        blocked_prerequisites = sorted(
            {
                reason
                for reason in [
                    *(result.get("blocked_reason") for result in case_results),
                    *(result.get("blocked_reason") for result in check_results),
                    cleanup.get("notes") if cleanup["status"] == "BLOCKED" else None,
                    self._budget_reason,
                ]
                if reason
            }
        )
        manifest["ended_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        elapsed_s = time.monotonic() - self._budget_started
        if (
            self._duration_limit_s is not None
            and elapsed_s > self._duration_limit_s
            and self._budget_reason is None
        ):
            self._budget_reason = "run duration budget was exhausted"
            status = "BLOCKED" if status == "PASS" else status
            blocked_prerequisites = sorted({*blocked_prerequisites, self._budget_reason})
        report = {
            "schema_version": "1.1",
            "run_id": run_id,
            "profile": self._config.profile,
            "status": status,
            "quality_status": (
                "PENDING_JUDGMENT"
                if self._judge_packets
                else (
                    "NOT_ELIGIBLE"
                    if any(case.get("mode") == "qualitative" for case in selected_cases)
                    else "NOT_REQUESTED"
                )
            ),
            "blocked_prerequisites": blocked_prerequisites if status == "BLOCKED" else [],
            "manifest": manifest,
            "selection": selection,
            "gate": {
                "passed": status == "PASS",
                "reason": self._gate_reason(status, check_results, case_results, cleanup),
                "check_status_counts": _status_counts(check_results),
                "case_status_counts": _status_counts(case_results),
            },
            "budget": {
                "status": "PASS" if self._budget_reason is None else "BLOCKED",
                "max_duration_s": self._duration_limit_s,
                "elapsed_s": round(elapsed_s, 6),
                "max_cost_usd": self._cost_limit_usd,
                "cost_usd": round(self._cost_usd, 9),
                "cost_complete": self._cost_complete,
                "reason": self._budget_reason,
            },
            "metrics": metrics,
            "check_results": check_results,
            "case_results": case_results,
            "findings": findings,
            "cleanup": cleanup,
            "evidence": evidence.records,
            "judge_packets": self._judge_packets,
            "artifact_refs": [
                record["ref"]
                for record in evidence.records
                if record["evidence_id"] in cleanup_evidence
            ],
        }
        report_path = run_dir / "report.json"
        _atomic_write(
            report_path,
            json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True).encode() + b"\n",
        )
        errors = validate_report(report_path, self._checks, self._cases)
        if errors:
            raise RunnerError("generated report is invalid:\n" + "\n".join(errors))
        summary_path = run_dir / "summary.md"
        _atomic_write(summary_path, self._render_summary(report).encode())
        return RunOutcome(run_id, status, report_path, summary_path)

    async def _preflight(
        self,
        run_id: str,
        selected_cases: list[dict[str, Any]],
        evidence: EvidenceStore,
        manifest: dict[str, Any],
    ) -> dict[str, Any]:
        selected_effects = sorted(
            {
                self._actions[step["action"]]["effect"]
                for case in selected_cases
                for step in [*case["steps"], *case.get("cleanup", [])]
            }
        )
        if not (_MUTATING_EFFECTS & set(selected_effects)):
            evidence_id = evidence.write(
                "safety-preflight-read-only",
                {"selected_effects": selected_effects},
                labels=["resolved scopes", "cost budget", "cleanup plan"],
            )
            return {
                "status": "PASS",
                "reason": "selected actions are read-only",
                "evidence_ids": [evidence_id],
            }
        scope = self._config.run_context.get("evaluation_scope")
        budget = self._config.run_context.get("cost_budget")
        static_failures: list[str] = []
        if not isinstance(scope, dict):
            static_failures.append("run_context.evaluation_scope is required")
        else:
            if scope.get("kind") not in {"dedicated_synthetic_scope", "ephemeral_stack"}:
                static_failures.append("evaluation scope kind is not isolated")
            if scope.get("run_owned") is not True:
                static_failures.append("evaluation scope is not explicitly run-owned")
            if scope.get("production_writable") is not False:
                static_failures.append("evaluation scope does not deny production writes")
            if not isinstance(scope.get("id"), str) or not scope["id"]:
                static_failures.append("evaluation scope ID is missing")
        if not isinstance(budget, dict):
            static_failures.append("run_context.cost_budget is required")
        else:
            for field_name in ("max_duration_s", "max_cost_usd", "max_action_cost_usd"):
                value = budget.get(field_name)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    or value <= 0
                ):
                    static_failures.append(f"cost budget {field_name} must be positive")
            maximum = budget.get("max_cost_usd")
            per_action = budget.get("max_action_cost_usd")
            if (
                isinstance(maximum, (int, float))
                and not isinstance(maximum, bool)
                and isinstance(per_action, (int, float))
                and not isinstance(per_action, bool)
                and per_action > maximum
            ):
                static_failures.append("cost budget max_action_cost_usd exceeds max_cost_usd")
        unsupported = [
            action_name
            for action_name in ("safety.preflight", "cleanup.run_scope")
            if not self._driver.supports(action_name)
        ]
        if unsupported:
            static_failures.append(
                "adapter lacks mandatory safety capabilities: " + ", ".join(unsupported)
            )
        if static_failures:
            reason = "; ".join(static_failures)
            evidence_id = evidence.write(
                "safety-preflight-blocked",
                {"reason": reason, "scope": scope, "cost_budget": budget},
                labels=["resolved scopes", "cost budget", "cleanup plan"],
            )
            return {"status": "BLOCKED", "reason": reason, "evidence_ids": [evidence_id]}
        request = ActionRequest(
            run_id=run_id,
            case_id="PRE-003",
            step_id="preflight",
            action="safety.preflight",
            isolation=str(scope["kind"]),
            effect=self._actions["safety.preflight"]["effect"],
            inputs={
                "evaluation_scope": scope,
                "cost_budget": budget,
                "selected_effects": selected_effects,
                "cleanup_action": "cleanup.run_scope",
            },
            observe=("safety",),
            run_context=self._budgeted_run_context(
                {
                    **self._config.run_context,
                    "quality_config": copy.deepcopy(self._config.quality_config),
                    "manifest": manifest,
                }
            ),
        )
        self._mutation_attempted = True
        try:
            timeout_s = self._bounded_timeout(self._config.step_timeout_s)
            response = await asyncio.wait_for(
                self._driver.execute(request),
                timeout=timeout_s,
            )
        except Exception as exc:
            message = (
                "run duration budget was exhausted during safety preflight"
                if self._duration_exhausted()
                else f"safety preflight adapter failed: {type(exc).__name__}"
            )
            if self._duration_exhausted():
                self._budget_reason = message
            response = ActionResponse(
                status="BLOCKED",
                message=message,
            )
        cost_reason = self._record_action_cost("safety.preflight", response, required=True)
        if cost_reason is not None:
            response = ActionResponse(
                status="BLOCKED",
                observations=response.observations,
                message=cost_reason,
                metrics=response.metrics,
                evidence=response.evidence,
                created_resources=response.created_resources,
                removed_resources=response.removed_resources,
                cost_usd=response.cost_usd,
            )
        safety = response.observations.get("safety", {})
        attested = (
            isinstance(safety, dict)
            and safety.get("scope_run_owned") is True
            and safety.get("production_writable") is False
            and safety.get("cost_bounded") is True
            and safety.get("cleanup_supported") is True
        )
        status = response.status
        reason = response.message or (
            "adapter safety preflight passed"
            if status == "PASS"
            else f"adapter safety preflight returned {status}"
        )
        if status == "NOT_APPLICABLE":
            status = "BLOCKED"
            reason = "safety preflight cannot be not applicable for mutating actions"
        if status == "PASS" and not attested:
            status = "FAIL"
            reason = "adapter did not attest every required safety property"
        evidence_ids = [
            evidence.write(
                "safety-preflight",
                {
                    "status": status,
                    "reason": reason,
                    "scope": scope,
                    "cost_budget": budget,
                    "observations": response.observations,
                },
                labels=["resolved scopes", "fixture prefix", "cost budget", "cleanup plan"],
            )
        ]
        return {"status": status, "reason": reason, "evidence_ids": evidence_ids}

    def _build_manifest(self, started: datetime, selection: dict[str, list[str]]) -> dict[str, Any]:
        baseline_ref = None
        if self._config.baseline_path is not None:
            if not self._config.baseline_path.is_file():
                raise RunnerError(f"baseline does not exist: {self._config.baseline_path}")
            baseline = load_json(self._config.baseline_path)
            baseline_ref = {
                "baseline_id": baseline["baseline_id"],
                "uri": str(self._config.baseline_path),
                "sha256": sha256_file(self._config.baseline_path),
            }
        profiles = list(self._config.execution_profiles)
        manifest = {
            "started_at": started.isoformat().replace("+00:00", "Z"),
            "ended_at": None,
            "environment": self._config.environment,
            "service_version": self._config.service_version,
            "git_sha": self._config.git_sha,
            "git_dirty": self._config.git_dirty,
            "source_tree_sha256": source_tree_sha256(self._root),
            "dataset_sha256": dataset_sha256(),
            "checklist_sha256": sha256_file(self._root / "checklist.json"),
            "action_catalog_sha256": sha256_file(self._root / "action_catalog.json"),
            "execution_profile_sha256": sha256_json(profiles),
            "selection_sha256": sha256_json(
                {
                    "check_ids": sorted(selection["check_ids"]),
                    "case_ids": sorted(selection["case_ids"]),
                }
            ),
            "config_fingerprint": sha256_json(self._config.quality_config),
            "baseline": baseline_ref,
            "graph_backend": self._config.graph_backend,
            "hardware_profile": self._config.hardware_profile,
            "cache_state": self._config.cache_state,
            "concurrency": self._config.concurrency,
            "random_seed": self._config.random_seed,
            "execution_profiles": profiles,
            "models": self._config.models,
            "pipeline_versions": self._config.pipeline_versions,
            "ontology_version": self._config.ontology_version,
            "evaluator": self._config.evaluator,
            "adapter": self._adapter_manifest(),
        }
        if self._config.app_image_digest is not None:
            manifest["app_image_digest"] = self._config.app_image_digest
        if self._config.baseline_path is not None:
            baseline = load_json(self._config.baseline_path)
            validator = Draft202012Validator(
                load_json(self._root / "schemas" / "baseline.schema.json"),
                format_checker=FormatChecker(),
            )
            schema_errors = sorted(error.message for error in validator.iter_errors(baseline))
            if schema_errors:
                raise RunnerError("baseline is invalid: " + "; ".join(schema_errors))
            compatibility = baseline["compatibility"]
            expected = {
                "dataset_sha256": manifest["dataset_sha256"],
                "profile": self._config.profile,
                "selection_sha256": manifest["selection_sha256"],
                "checklist_sha256": manifest["checklist_sha256"],
                "action_catalog_sha256": manifest["action_catalog_sha256"],
                "execution_profile_sha256": manifest["execution_profile_sha256"],
                "config_fingerprint": manifest["config_fingerprint"],
                "service_version": manifest["service_version"],
                "graph_backend": manifest["graph_backend"],
                "cache_state": manifest["cache_state"],
                "concurrency": manifest["concurrency"],
                "hardware_profile": manifest["hardware_profile"],
                "models": manifest["models"],
                "pipeline_versions": manifest["pipeline_versions"],
                "ontology_version": manifest["ontology_version"],
            }
            incompatible = [
                field
                for field, expected_value in expected.items()
                if compatibility.get(field) != expected_value
            ]
            if incompatible:
                raise RunnerError("baseline is incompatible on: " + ", ".join(sorted(incompatible)))
        return manifest

    def _adapter_manifest(self) -> dict[str, Any]:
        if isinstance(self._driver, SubprocessActionDriver):
            executable = shutil.which(self._driver.command[0]) or self._driver.command[0]
            executable_path = Path(executable).resolve()
            return {
                "kind": "subprocess",
                "executable": str(executable_path),
                "executable_sha256": (
                    sha256_file(executable_path) if executable_path.is_file() else None
                ),
                "arguments_sha256": sha256_json(list(self._driver.command[1:])),
                "capabilities": (
                    sorted(self._driver.capabilities)
                    if self._driver.capabilities is not None
                    else None
                ),
                "timeout_s": self._driver.timeout_s,
            }
        return {
            "kind": f"{type(self._driver).__module__}.{type(self._driver).__qualname__}",
            "executable": None,
            "executable_sha256": None,
            "arguments_sha256": None,
            "capabilities": None,
            "timeout_s": None,
        }

    def _prepare_judge_packet(
        self,
        *,
        run_id: str,
        case: dict[str, Any],
        fixture: dict[str, Any],
        observations: dict[str, Any],
        evidence: EvidenceStore,
        manifest: dict[str, Any],
    ) -> str:
        evaluation = case["evaluation"]
        task = _resolve_reference(
            evaluation["task_ref"],
            fixture=fixture,
            observations=observations,
            run_context={},
        )
        documents = _resolve_reference(
            evaluation["documents_ref"],
            fixture=fixture,
            observations=observations,
            run_context={},
        )
        candidate = _lookup(observations, evaluation["candidate_output_ref"])
        if candidate is _MISSING or candidate is None:
            raise RunnerError("candidate output is unavailable for external judging")
        if not isinstance(task, dict) or not isinstance(documents, list) or not documents:
            raise RunnerError("judge packet task and source documents are invalid")
        assets: dict[str, dict[str, Any]] = {}
        for asset_field in ("rubric_path", "panel_policy_path"):
            path = (self._root / evaluation[asset_field]).resolve()
            try:
                path.relative_to(self._root.resolve())
            except ValueError as exc:
                raise RunnerError(
                    f"judge asset escapes eval root: {evaluation[asset_field]}"
                ) from exc
            if not path.is_file():
                raise RunnerError(f"judge asset does not exist: {evaluation[asset_field]}")
            value = load_json(path)
            if not isinstance(value, dict):
                raise RunnerError(f"judge asset must be an object: {evaluation[asset_field]}")
            assets[asset_field] = value
        persona = "unspecified user"
        for workflow in fixture.get("workflows", {}).values():
            if isinstance(workflow, dict) and workflow.get("task") == task:
                persona = str(workflow.get("persona") or persona)
                break
        candidate_output, tool_trace, citations, result_ids = _prepare_candidate_for_judging(
            candidate
        )
        boundary_observations = {
            key: value
            for key, value in observations.items()
            if key not in {evaluation["candidate_output_ref"].split(".", 1)[0], "evaluation"}
        }
        hard_assertions: list[dict[str, Any]] = []
        for assertion in case["assertions"]:
            if assertion["target"].startswith("evaluation."):
                continue
            passed, observed, notes = _assertion_passes(assertion, observations)
            hard_assertions.append(
                {
                    "assertion_id": assertion["id"],
                    "severity": assertion["severity"],
                    "gate": assertion["gate"],
                    "target": assertion["target"],
                    "operator": assertion["operator"],
                    "expected": assertion["expected"],
                    "observed": observed,
                    "preliminary_status": "PASS" if passed else "FAIL",
                    "notes": notes,
                }
            )
        packet_id = f"{run_id}-{case['case_id']}"
        safe_documents = _redact(documents, retain_payloads=True)
        reference_catalog = [
            {"ref": "task", "kind": "task", "pointer": "/task"},
            {
                "ref": "candidate:output",
                "kind": "candidate",
                "pointer": "/candidate/output",
            },
        ]
        for index, document in enumerate(safe_documents):
            document_id = document.get("document_id") if isinstance(document, dict) else None
            reference_catalog.append(
                {
                    "ref": f"document:{document_id or index}",
                    "kind": "document",
                    "pointer": f"/source_material/documents/{index}",
                }
            )
        reference_catalog.extend(
            {
                "ref": f"tool_call:{index}",
                "kind": "tool_call",
                "pointer": f"/system_evidence/tool_trace/{index}",
            }
            for index in range(len(tool_trace))
        )
        reference_catalog.extend(
            {
                "ref": f"citation:{index}",
                "kind": "citation",
                "pointer": f"/system_evidence/citations/{index}",
            }
            for index in range(len(citations))
        )
        reference_catalog.extend(
            {
                "ref": f"result:{result_id}",
                "kind": "result",
                "pointer": f"/system_evidence/result_ids/{index}",
            }
            for index, result_id in enumerate(result_ids)
            if isinstance(result_id, (str, int, float)) and not isinstance(result_id, bool)
        )
        reference_catalog.extend(
            {
                "ref": f"hard:{assertion['assertion_id']}",
                "kind": "hard_assertion",
                "pointer": f"/hard_constraints/assertions/{index}",
            }
            for index, assertion in enumerate(hard_assertions)
        )
        reference_ids = [item["ref"] for item in reference_catalog]
        if len(reference_ids) != len(set(reference_ids)):
            raise RunnerError("judge packet reference catalog contains duplicate IDs")
        packet = {
            "schema_version": "1.0",
            "packet_id": packet_id,
            "run": {
                "run_id": run_id,
                "case_id": case["case_id"],
                "profile": self._config.profile,
                "dataset_sha256": manifest["dataset_sha256"],
                "service_version": manifest["service_version"],
                "git_sha": manifest["git_sha"],
                "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            },
            "scenario": {
                "title": case["title"],
                "objective": case["objective"],
                "persona": persona,
                "tags": case["tags"],
                "answer_key_policy": evaluation["answer_key_policy"],
            },
            "task": _redact(task, retain_payloads=True),
            "source_material": {
                "synthetic_only": True,
                "documents": safe_documents,
                "document_count": len(documents),
                "sha256": sha256_json(safe_documents),
            },
            "candidate": {
                "alias": "candidate-A",
                "identity_blinded": True,
                "output": candidate_output,
            },
            "system_evidence": {
                "boundary_observations": _redact(boundary_observations, retain_payloads=True),
                "tool_trace": tool_trace,
                "citations": citations,
                "result_ids": result_ids,
            },
            "hard_constraints": {
                "policy": (
                    "Deterministic failures remain authoritative and cannot be overridden "
                    "by panel scores."
                ),
                "assertions": hard_assertions,
            },
            "reference_catalog": reference_catalog,
            "rubric": assets["rubric_path"],
            "panel_policy": assets["panel_policy_path"],
            "judge_contract": {
                "instructions": "README.md#manual-judging-protocol",
                "judgment_schema": "judging/schemas/judgment.schema.json",
                "packet_sha256_excluded": True,
            },
        }
        redaction_issues = _redaction_issues(packet, retain_payloads=True)
        if redaction_issues:
            raise RunnerError("judge packet contains unsafe fields: " + "; ".join(redaction_issues))
        validator = Draft202012Validator(
            load_json(self._root / "judging" / "schemas" / "judge-packet.schema.json"),
            format_checker=FormatChecker(),
        )
        schema_errors = sorted(error.message for error in validator.iter_errors(packet))
        if schema_errors:
            raise RunnerError("judge packet is invalid: " + "; ".join(schema_errors))
        path = evidence.run_dir / "judge-packets" / f"{case['case_id']}.json"
        payload = json.dumps(packet, ensure_ascii=True, indent=2, sort_keys=True).encode() + b"\n"
        path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(path, payload)
        packet_sha256 = hashlib.sha256(payload).hexdigest()
        self._judge_packets.append(
            {
                "packet_id": packet_id,
                "case_id": case["case_id"],
                "ref": str(path.resolve()),
                "sha256": packet_sha256,
                "rubric_id": assets["rubric_path"].get("rubric_id"),
                "panel_policy_id": assets["panel_policy_path"].get("policy_id"),
                "status": "PENDING_JUDGMENT",
            }
        )
        return evidence.write(
            f"{case['case_id']}-judge-packet-manifest",
            {
                "packet_id": packet_id,
                "ref": str(path.resolve()),
                "sha256": packet_sha256,
                "document_count": len(documents),
                "candidate_output_present": True,
            },
            labels=[
                "judge_packet",
                "candidate_output",
                "tool_trace",
                "rubric_snapshot",
                "source_material_manifest",
                "judge packet manifest",
                "candidate output and tool trace",
                "rubric and panel policy snapshots",
                "source material digest",
                "hard-gate evidence",
            ],
        )

    async def _run_case(
        self,
        run_id: str,
        case: dict[str, Any],
        evidence: EvidenceStore,
        manifest: dict[str, Any],
    ) -> tuple[
        dict[str, Any],
        list[dict[str, Any]],
        list[str],
        bool,
    ]:
        started = time.perf_counter()
        observations: dict[str, Any] = {}
        evidence_ids: list[str] = []
        evidence_by_label: dict[str, str] = {}
        metrics: list[dict[str, Any]] = []
        declared_metric_names = set(case.get("metrics", []))
        runner_owned_metric_names = _runner_owned_metric_names(case)
        resources: list[str] = []
        blocked_reason: str | None = None
        failed_reason: str | None = None
        missing_capability = False
        mutation_attempted = False
        fixture = fixture_data(case)
        run_context = {
            **self._config.run_context,
            "run_id": run_id,
            "profile": self._config.profile,
            "environment": self._config.environment,
            "random_seed": self._config.random_seed,
            "quality_config": copy.deepcopy(self._config.quality_config),
            "manifest": manifest,
        }
        evaluation_scope = run_context.get("evaluation_scope")
        if isinstance(evaluation_scope, dict) and isinstance(evaluation_scope.get("id"), str):
            _set_path(observations, "scope.group_id", evaluation_scope["id"])
        cleanup_steps = case.get("cleanup", [])
        steps = [*case["steps"], *cleanup_steps]
        skip_regular_steps = False
        for step_index, step in enumerate(steps):
            is_cleanup_step = step_index >= len(case["steps"])
            if skip_regular_steps and not is_cleanup_step:
                continue
            action_name = step["action"]
            action = self._actions[action_name]
            cost_required = False
            step_labels = sorted(
                {
                    label
                    for assertion in case["assertions"]
                    if any(
                        _observation_path_is_declared(assertion["target"], (observed_path,))
                        for observed_path in step["observe"]
                    )
                    for label in assertion["evidence"]
                }
                | {
                    label
                    for check in self._checks
                    if case["case_id"] in check["scenario_ids"]
                    for label in check["evidence"]
                }
            )
            if action["effect"] not in self._config.allowed_effects:
                blocked_reason = (
                    f"effect {action['effect']} is not allowed for action {action_name}"
                )
                response = ActionResponse(status="BLOCKED", message=blocked_reason)
                request_inputs: dict[str, Any] = {}
            elif action_name in _RUNNER_ACTIONS:
                try:
                    resolved_inputs = _resolve_inputs(
                        step["input"],
                        fixture=fixture,
                        observations=observations,
                        run_context=self._budgeted_run_context(run_context),
                    )
                    request_inputs = _strip_gold_labels(resolved_inputs)
                    response = _execute_runner_action(
                        action_name,
                        resolved_inputs,
                        fixture=fixture,
                        observations=observations,
                        declared_metrics=declared_metric_names,
                        evidence_labels=step_labels,
                    )
                except InputResolutionError as exc:
                    blocked_reason = str(exc)
                    request_inputs = {}
                    response = ActionResponse(status="BLOCKED", message=blocked_reason)
            elif not self._driver.supports(action_name):
                if action["availability"] == "product_capability_missing":
                    missing_capability = True
                    failed_reason = f"product capability is unavailable: {action_name}"
                    response = ActionResponse(status="FAIL", message=failed_reason)
                else:
                    blocked_reason = f"no configured adapter supports action {action_name}"
                    response = ActionResponse(status="BLOCKED", message=blocked_reason)
                request_inputs = {}
            elif (admission_reason := self._cost_admission_reason(action_name)) is not None:
                blocked_reason = admission_reason
                request_inputs = {}
                response = ActionResponse(status="BLOCKED", message=admission_reason)
            else:
                budget_limited_timeout = False
                try:
                    resolved_inputs = _resolve_inputs(
                        step["input"],
                        fixture=fixture,
                        observations=observations,
                        run_context=self._budgeted_run_context(run_context),
                    )
                    request_inputs = _strip_gold_labels(resolved_inputs)
                    request = ActionRequest(
                        run_id=run_id,
                        case_id=case["case_id"],
                        step_id=step["id"],
                        action=action_name,
                        isolation=case["isolation"],
                        effect=action["effect"],
                        inputs=request_inputs,
                        observe=tuple(step["observe"]),
                        run_context=self._budgeted_run_context(run_context),
                        evidence_labels=tuple(step_labels),
                    )
                    mutation_attempted = mutation_attempted or action["effect"] in _MUTATING_EFFECTS
                    self._mutation_attempted = self._mutation_attempted or mutation_attempted
                    timeout_s = float(step.get("timeout_s", self._config.step_timeout_s))
                    action_wait = request_inputs.get("timeout_s")
                    if not isinstance(action_wait, bool) and isinstance(action_wait, (int, float)):
                        timeout_s = max(timeout_s, float(action_wait) + 5.0)
                    bounded_timeout_s = self._bounded_timeout(timeout_s)
                    budget_limited_timeout = bounded_timeout_s < timeout_s
                    cost_required = True
                    response = await asyncio.wait_for(
                        self._driver.execute(request), timeout=bounded_timeout_s
                    )
                except InputResolutionError as exc:
                    blocked_reason = str(exc)
                    request_inputs = {}
                    response = ActionResponse(status="BLOCKED", message=blocked_reason)
                except TimeoutError:
                    if budget_limited_timeout or self._duration_exhausted():
                        self._budget_reason = "run duration budget was exhausted"
                        blocked_reason = self._budget_reason
                    else:
                        blocked_reason = f"action {action_name} exceeded its runner timeout"
                    response = ActionResponse(status="BLOCKED", message=blocked_reason)
                except AdapterProtocolError as exc:
                    blocked_reason = f"adapter protocol error for {action_name}: {exc}"
                    response = ActionResponse(status="BLOCKED", message=blocked_reason)
                except Exception as exc:  # Boundary: adapter failures become explicit blockers.
                    blocked_reason = f"adapter failed for {action_name}: {type(exc).__name__}"
                    response = ActionResponse(status="BLOCKED", message=blocked_reason)
            if response.status == "PASS":
                observation_errors = _observation_contract_errors(
                    response.observations, tuple(step["observe"])
                )
                if observation_errors:
                    failed_reason = f"action {action_name} " + "; ".join(observation_errors)
                    response = ActionResponse(
                        status="FAIL",
                        observations=response.observations,
                        message=failed_reason,
                        metrics=response.metrics,
                        evidence=response.evidence,
                        created_resources=response.created_resources,
                        removed_resources=response.removed_resources,
                        cost_usd=response.cost_usd,
                    )
            cost_reason = self._record_action_cost(action_name, response, required=cost_required)
            if cost_reason is not None:
                blocked_reason = cost_reason
                response = ActionResponse(
                    status="BLOCKED",
                    observations=response.observations,
                    message=cost_reason,
                    metrics=response.metrics,
                    evidence=response.evidence,
                    created_resources=response.created_resources,
                    removed_resources=response.removed_resources,
                    cost_usd=response.cost_usd,
                )
            _deep_merge(observations, response.observations)
            if action_name == "result.score":
                unsupported_count = _lookup(response.observations, "score.unsupported_claim_count")
                if unsupported_count is not _MISSING:
                    _set_path(
                        observations,
                        "answers.budget.unsupported_claim_count",
                        unsupported_count,
                    )
            failure_labels = sorted(
                {label for assertion in case["assertions"] for label in assertion["evidence"]}
            )
            step_evidence_id = evidence.write(
                f"{case['case_id']}-{step['id']}-{action_name}",
                {
                    "case_id": case["case_id"],
                    "step_id": step["id"],
                    "action": action_name,
                    "availability": action["availability"],
                    "effect": action["effect"],
                    "inputs": request_inputs,
                    "status": response.status,
                    "observations": response.observations,
                    "observations_sha256": sha256_json(response.observations),
                    "message": response.message,
                    "created_resources": list(response.created_resources),
                    "removed_resources": list(response.removed_resources),
                },
                labels=failure_labels if response.status == "FAIL" else None,
            )
            evidence_ids.append(step_evidence_id)
            if response.status == "FAIL":
                for label in failure_labels:
                    evidence_by_label.setdefault(label, step_evidence_id)
            for index, descriptor in enumerate(response.evidence, start=1):
                label = descriptor.get("label")
                if not isinstance(label, str) or label not in step_labels:
                    failed_reason = (
                        f"action {action_name} returned evidence for an undeclared label: {label!r}"
                    )
                    skip_regular_steps = True
                    continue
                adapter_payload = {
                    key: value for key, value in descriptor.items() if key != "label"
                }
                adapter_evidence_id = evidence.write(
                    f"{case['case_id']}-{step['id']}-adapter-{index}",
                    adapter_payload,
                    kind=str(descriptor.get("kind", "file")),
                    labels=[label],
                )
                evidence_ids.append(adapter_evidence_id)
                evidence_by_label[label] = adapter_evidence_id
            if blocked_reason is not None and response.status == "PASS":
                skip_regular_steps = True
                continue
            resources.extend(response.created_resources)
            try:
                for metric in response.metrics:
                    metric_name = metric.get("name")
                    if (
                        action_name not in _RUNNER_ACTIONS
                        and metric_name in runner_owned_metric_names
                    ):
                        raise AdapterProtocolError(
                            f"adapter returned runner-owned metric: {metric_name}"
                        )
                    self._metric_counter += 1
                    normalized_metric = _normalize_metric(
                        metric,
                        owner_id=case["case_id"],
                        metric_id=f"metric-{self._metric_counter:05d}",
                        declared_names=declared_metric_names,
                    )
                    if any(item["name"] == normalized_metric["name"] for item in metrics):
                        raise AdapterProtocolError(
                            f"adapter returned duplicate metric: {normalized_metric['name']}"
                        )
                    metrics.append(normalized_metric)
            except AdapterProtocolError as exc:
                blocked_reason = f"adapter metric protocol error for {action_name}: {exc}"
                evidence_ids.append(
                    evidence.write(
                        f"{case['case_id']}-{step['id']}-metric-error",
                        {"reason": blocked_reason},
                    )
                )
                skip_regular_steps = True
                continue
            if response.status == "BLOCKED":
                blocked_reason = response.message or blocked_reason or f"{action_name} blocked"
                skip_regular_steps = True
                continue
            if response.status == "FAIL":
                failed_reason = response.message or failed_reason or f"{action_name} failed"
                skip_regular_steps = True
                continue
            if response.status == "NOT_APPLICABLE":
                blocked_reason = f"action {action_name} returned NOT_APPLICABLE for a selected case"
                skip_regular_steps = True
                continue

        if any(assertion["target"] == "client_scope_effect" for assertion in case["assertions"]):
            scope_probes = [
                assertion
                for assertion in case["assertions"]
                if assertion["operator"] == "not_contains"
                and assertion["target"].endswith(".results")
            ]
            if scope_probes:
                scope_effect = not all(
                    _assertion_passes(probe, observations)[0] for probe in scope_probes
                )
                _set_path(
                    observations,
                    "client_scope_effect",
                    scope_effect,
                )
                scope_labels = sorted(
                    {
                        label
                        for assertion in case["assertions"]
                        if assertion["target"] == "client_scope_effect"
                        for label in assertion["evidence"]
                    }
                )
                scope_evidence_id = evidence.write(
                    f"{case['case_id']}-derived-client-scope-effect",
                    {
                        "client_scope_effect": scope_effect,
                        "probe_targets": [probe["target"] for probe in scope_probes],
                    },
                    labels=scope_labels,
                )
                evidence_ids.append(scope_evidence_id)
                for label in scope_labels:
                    evidence_by_label[label] = scope_evidence_id

        returned_metric_names = {metric["name"] for metric in metrics}
        missing_metrics = sorted(declared_metric_names - returned_metric_names)
        if missing_metrics and blocked_reason is None and failed_reason is None:
            blocked_reason = "adapter omitted declared metrics: " + ", ".join(missing_metrics)
        failed_metric_names = sorted(
            metric["name"] for metric in metrics if metric["status"] == "FAIL"
        )
        blocked_metric_names = sorted(
            metric["name"]
            for metric in metrics
            if metric["status"] in {"BLOCKED", "NOT_APPLICABLE"}
        )
        if failed_metric_names and failed_reason is None:
            failed_reason = "adapter metrics failed: " + ", ".join(failed_metric_names)
        elif blocked_metric_names and blocked_reason is None:
            blocked_reason = "adapter metrics unavailable: " + ", ".join(blocked_metric_names)

        metrics_by_name = {metric["name"]: metric for metric in metrics}
        sample_blockers: dict[str, str] = {}
        baseline_unavailable: dict[str, str] = {}
        baseline: dict[str, Any] | None = None
        if self._config.baseline_path is not None:
            baseline = load_json(self._config.baseline_path)
        baseline_evidence_id: str | None = None
        for assertion in case["assertions"]:
            minimum_sample_size = assertion.get("minimum_sample_size")
            if minimum_sample_size is None:
                continue
            metric_name = _ASSERTION_METRICS.get(assertion["target"])
            current_metric = metrics_by_name.get(metric_name) if metric_name else None
            if current_metric is None or current_metric["sample_size"] < minimum_sample_size:
                reason = (
                    f"metric {metric_name or assertion['target']} has fewer than "
                    f"{minimum_sample_size} samples"
                )
                if assertion["activation"] == "baseline_required":
                    baseline_unavailable[assertion["id"]] = reason
                else:
                    sample_blockers[assertion["id"]] = reason
                continue
            if assertion["activation"] != "baseline_required" or baseline is None:
                continue
            baseline_metric = next(
                (
                    item
                    for item in baseline["metrics"]
                    if item["name"] == metric_name
                    and item["dimensions"] == current_metric["dimensions"]
                ),
                None,
            )
            if baseline_metric is None or baseline_metric["sample_size"] < minimum_sample_size:
                baseline_unavailable[assertion["id"]] = (
                    f"accepted baseline lacks {minimum_sample_size} comparable "
                    f"samples for {metric_name}"
                )
                continue
            current_value = current_metric["value"]
            if isinstance(current_value, bool) or not isinstance(current_value, (int, float)):
                baseline_unavailable[assertion["id"]] = (
                    f"current metric {metric_name} has no numeric value"
                )
                continue
            delta = _baseline_delta(
                assertion["target"], float(current_value), float(baseline_metric["value"])
            )
            if delta is None:
                baseline_unavailable[assertion["id"]] = (
                    f"baseline metric {metric_name} is zero; relative delta is undefined"
                )
                continue
            _set_path(observations, assertion["target"], delta)
            current_metric["baseline"] = {
                "baseline_id": baseline["baseline_id"],
                "value": baseline_metric["value"],
                "sample_size": baseline_metric["sample_size"],
            }
            current_metric["delta_absolute"] = float(current_value) - float(
                baseline_metric["value"]
            )
            current_metric["delta_relative"] = (
                None
                if baseline_metric["value"] == 0
                else current_metric["delta_absolute"] / abs(baseline_metric["value"])
            )
            current_metric["comparator"] = assertion["operator"]
            current_metric["threshold"] = assertion["expected"]
            current_metric["threshold_source"] = assertion.get("threshold_source")
            if baseline_evidence_id is None:
                baseline_evidence_id = evidence.write(
                    f"{case['case_id']}-accepted-baseline",
                    {
                        "baseline_id": baseline["baseline_id"],
                        "sha256": sha256_file(self._config.baseline_path),
                    },
                    labels=["baseline"],
                )
                evidence_ids.append(baseline_evidence_id)
                evidence_by_label["baseline"] = baseline_evidence_id

        if case.get("mode") == "qualitative":
            _set_path(observations, "evaluation.packet_ready", False)
            _set_path(observations, "evaluation.candidate_output_present", False)
            if blocked_reason is None and failed_reason is None:
                try:
                    packet_evidence_id = self._prepare_judge_packet(
                        run_id=run_id,
                        case=case,
                        fixture=fixture,
                        observations=observations,
                        evidence=evidence,
                        manifest=manifest,
                    )
                    evidence_ids.append(packet_evidence_id)
                    packet_labels = next(
                        item["labels"]
                        for item in evidence.records
                        if item["evidence_id"] == packet_evidence_id
                    )
                    for label in packet_labels:
                        evidence_by_label[label] = packet_evidence_id
                    _set_path(observations, "evaluation.packet_ready", True)
                    _set_path(observations, "evaluation.candidate_output_present", True)
                except RunnerError as exc:
                    blocked_reason = f"judge packet unavailable: {exc}"

        assertion_results: list[dict[str, Any]] = []
        baseline_present = self._config.baseline_path is not None
        for assertion in case["assertions"]:
            activation = assertion["activation"]
            raw_observed = _lookup(observations, assertion["target"])
            resolved_expected = _expected_value(assertion["expected"], observations)
            observed_present = raw_observed is not _MISSING
            expected_present = resolved_expected is not _MISSING
            if activation == "baseline_required" and not baseline_present:
                status = "NOT_APPLICABLE"
                observed: Any = None
                notes = "no compatible accepted baseline was configured"
                assertion_evidence: list[str] = []
            elif activation == "baseline_required" and assertion["id"] in baseline_unavailable:
                status = "NOT_APPLICABLE"
                observed = None
                notes = baseline_unavailable[assertion["id"]]
                assertion_evidence = []
            elif assertion["id"] in sample_blockers:
                status = "BLOCKED"
                observed_value = _lookup(observations, assertion["target"])
                observed = None if observed_value is _MISSING else observed_value
                notes = sample_blockers[assertion["id"]]
                blocked_reason = blocked_reason or notes
                assertion_evidence = evidence_ids
            elif activation == "capability_probe" and missing_capability:
                status = "FAIL"
                observed_value = _lookup(observations, assertion["target"])
                observed = None if observed_value is _MISSING else observed_value
                notes = failed_reason or "declared product capability is unavailable"
                assertion_evidence = evidence_ids
            elif blocked_reason is not None:
                status = "BLOCKED"
                observed_value = _lookup(observations, assertion["target"])
                observed = None if observed_value is _MISSING else observed_value
                notes = blocked_reason
                assertion_evidence = evidence_ids
            elif failed_reason is not None:
                observed_value = _lookup(observations, assertion["target"])
                observed = None if observed_value is _MISSING else observed_value
                status = "FAIL"
                notes = f"scenario action failed: {failed_reason}"
                assertion_evidence = evidence_ids
            else:
                passed, observed, notes = _assertion_passes(assertion, observations)
                status = "PASS" if passed else "FAIL"
                if failed_reason and not passed:
                    notes = f"{notes}; {failed_reason}"
                assertion_evidence = list(
                    dict.fromkeys(
                        evidence_by_label[label]
                        for label in assertion["evidence"]
                        if label in evidence_by_label
                    )
                )
                missing_evidence = sorted(set(assertion["evidence"]) - evidence_by_label.keys())
                if missing_evidence:
                    status = "BLOCKED"
                    notes = "missing required evidence: " + ", ".join(missing_evidence)
                    blocked_reason = blocked_reason or notes
            assertion_results.append(
                {
                    "assertion_id": assertion["id"],
                    "status": status,
                    "target": assertion["target"],
                    "operator": assertion["operator"],
                    "expected": assertion["expected"],
                    "resolved_expected": None if not expected_present else resolved_expected,
                    "observed_present": observed_present,
                    "expected_present": expected_present,
                    "evaluation_kind": (
                        "not_applicable"
                        if status == "NOT_APPLICABLE"
                        else (
                            "blocked"
                            if status == "BLOCKED"
                            else (
                                "action_failed"
                                if failed_reason is not None or missing_capability
                                else "operator"
                            )
                        )
                    ),
                    "observed": observed,
                    "evidence_ids": assertion_evidence,
                    "notes": notes,
                }
            )
        status = _case_status(assertion_results)
        result = {
            "case_id": case["case_id"],
            "status": status,
            "quality_status": (
                "PENDING_JUDGMENT"
                if any(packet["case_id"] == case["case_id"] for packet in self._judge_packets)
                else ("NOT_ELIGIBLE" if case.get("mode") == "qualitative" else "NOT_REQUESTED")
            ),
            "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            "assertion_results": assertion_results,
            "metric_ids": [metric["metric_id"] for metric in metrics],
            "evidence_ids": list(dict.fromkeys(evidence_ids)),
            "first_bad_boundary": case["likely_first_bad_boundary"] if status == "FAIL" else None,
            "root_cause_confidence": 1.0 if status == "FAIL" else 0.0,
            "blocked_reason": blocked_reason if status == "BLOCKED" else None,
        }
        return (
            result,
            metrics,
            list(dict.fromkeys(resources)),
            mutation_attempted,
        )

    async def _cleanup(
        self,
        run_id: str,
        created_resources: list[str],
        evidence: EvidenceStore,
        manifest: dict[str, Any],
        *,
        mutation_attempted: bool,
    ) -> tuple[dict[str, Any], list[str]]:
        created = list(dict.fromkeys(created_resources))
        if not created and not mutation_attempted:
            return (
                {
                    "status": "NOT_APPLICABLE",
                    "created_resources": [],
                    "removed_resources": [],
                    "remaining_resources": [],
                    "notes": "no mutating action was attempted",
                },
                [],
            )
        action = self._actions["cleanup.run_scope"]
        if action["effect"] not in self._config.allowed_effects:
            reason = "cleanup effect is not allowed by the run configuration"
            evidence_id = evidence.write("cleanup-blocked", {"reason": reason, "created": created})
            return (
                {
                    "status": "BLOCKED",
                    "created_resources": created,
                    "removed_resources": [],
                    "remaining_resources": created,
                    "notes": reason,
                },
                [evidence_id],
            )
        if not self._driver.supports("cleanup.run_scope"):
            reason = "no configured adapter supports cleanup.run_scope"
            evidence_id = evidence.write("cleanup-blocked", {"reason": reason, "created": created})
            return (
                {
                    "status": "BLOCKED",
                    "created_resources": created,
                    "removed_resources": [],
                    "remaining_resources": created,
                    "notes": reason,
                },
                [evidence_id],
            )
        request = ActionRequest(
            run_id=run_id,
            case_id=run_id,
            step_id="cleanup",
            action="cleanup.run_scope",
            isolation="ephemeral_stack",
            effect=action["effect"],
            inputs={"run_id": run_id, "created_resource_ids": created},
            observe=(),
            run_context=self._budgeted_run_context(
                {**self._config.run_context, "manifest": manifest}
            ),
        )
        try:
            response = await asyncio.wait_for(
                self._driver.execute(request), timeout=self._config.step_timeout_s
            )
        except Exception as exc:  # Boundary: cleanup must leave an explicit ledger.
            response = ActionResponse(
                status="BLOCKED", message=f"cleanup adapter failed: {type(exc).__name__}"
            )
        self._record_action_cost("cleanup.run_scope", response, required=True)
        inventory = response.observations.get("cleanup")
        required_inventory_fields = {
            "inventory_complete",
            "created_resource_ids",
            "removed_resource_ids",
            "remaining_resource_ids",
            "health_restored",
        }
        inventory_valid = (
            isinstance(inventory, dict)
            and required_inventory_fields <= set(inventory)
            and inventory.get("inventory_complete") is True
            and all(
                isinstance(inventory.get(field_name), list)
                and all(isinstance(item, str) for item in inventory[field_name])
                for field_name in (
                    "created_resource_ids",
                    "removed_resource_ids",
                    "remaining_resource_ids",
                )
            )
            and isinstance(inventory.get("health_restored"), bool)
        )
        if not inventory_valid:
            reason = "cleanup adapter did not return a complete run-scoped resource inventory"
            evidence_id = evidence.write(
                "cleanup-result",
                {
                    "status": "BLOCKED",
                    "message": response.message,
                    "reported_created": created,
                    "inventory": inventory,
                },
            )
            return (
                {
                    "status": "BLOCKED",
                    "created_resources": created,
                    "removed_resources": [],
                    "remaining_resources": created,
                    "notes": reason,
                },
                [evidence_id],
            )
        inventory_data = inventory if isinstance(inventory, dict) else {}
        discovered = list(dict.fromkeys(inventory_data["created_resource_ids"]))
        created = list(dict.fromkeys([*created, *discovered]))
        removed = list(dict.fromkeys(inventory_data["removed_resource_ids"]))
        reported_removed = set(response.removed_resources)
        remaining = sorted(
            set(inventory_data["remaining_resource_ids"]) | (set(created) - set(removed))
        )
        status = response.status
        inventory_mismatch = bool(reported_removed) and reported_removed != set(removed)
        if status == "PASS" and (
            remaining or inventory_mismatch or inventory_data["health_restored"] is not True
        ):
            status = "FAIL"
        if status == "NOT_APPLICABLE":
            status = "BLOCKED"
        evidence_id = evidence.write(
            "cleanup-result",
            {
                "status": status,
                "message": response.message,
                "created": created,
                "removed": removed,
                "remaining": remaining,
                "health_restored": inventory_data["health_restored"],
                "inventory_complete": inventory_data["inventory_complete"],
                "reported_removed": sorted(reported_removed),
            },
        )
        return (
            {
                "status": status,
                "created_resources": created,
                "removed_resources": removed,
                "remaining_resources": remaining,
                "notes": response.message
                or (
                    "cleanup reconciled from independent inventory"
                    if status == "PASS"
                    else "cleanup inventory did not reconcile"
                ),
            },
            [evidence_id],
        )

    async def _evaluate_checks(
        self,
        *,
        run_id: str,
        checks: list[dict[str, Any]],
        case_results: list[dict[str, Any]],
        cleanup: dict[str, Any],
        evidence: EvidenceStore,
        manifest: dict[str, Any],
        framework_evidence_id: str,
        preflight: dict[str, Any],
        artifact_safety: dict[str, Any],
    ) -> list[dict[str, Any]]:
        results_by_case = {item["case_id"]: item for item in case_results}
        selected_case_ids = set(results_by_case)
        check_results: list[dict[str, Any]] = []
        for check in checks:
            mapped = [
                results_by_case[case_id]
                for case_id in check["scenario_ids"]
                if case_id in selected_case_ids
            ]
            evidence_ids = list(
                dict.fromkeys(
                    evidence_id for result in mapped for evidence_id in result["evidence_ids"]
                )
            )
            blocked_reason: str | None = None
            if check["id"] in _FRAMEWORK_CHECKS:
                status, notes = self._framework_check(
                    check["id"], cleanup, preflight, artifact_safety
                )
                if check["id"] == "PRE-003":
                    evidence_ids = preflight["evidence_ids"]
                elif check["id"] == "OBS-004":
                    evidence_ids = artifact_safety["evidence_ids"]
                else:
                    evidence_ids = [framework_evidence_id]
            elif mapped:
                statuses: list[str] = []
                for result in mapped:
                    case = next(
                        item for item in self._cases if item["case_id"] == result["case_id"]
                    )
                    assertions = {item["id"]: item for item in case["assertions"]}
                    gating_statuses = [
                        item["status"]
                        for item in result["assertion_results"]
                        if assertions[item["assertion_id"]]["gate"] is True
                    ]
                    statuses.extend(
                        gating_statuses or [item["status"] for item in result["assertion_results"]]
                    )
                if "FAIL" in statuses:
                    status = "FAIL"
                elif "BLOCKED" in statuses:
                    status = "BLOCKED"
                    blocked_reason = "; ".join(
                        sorted(
                            {
                                str(item["blocked_reason"])
                                for item in mapped
                                if item["blocked_reason"]
                            }
                        )
                    )
                elif statuses and all(item == "NOT_APPLICABLE" for item in statuses):
                    status = "NOT_APPLICABLE"
                else:
                    status = "PASS"
                notes = f"derived from scenarios: {', '.join(item['case_id'] for item in mapped)}"
            elif self._budget_reason is not None:
                status = "BLOCKED"
                blocked_reason = self._budget_reason
                notes = blocked_reason
                evidence_ids = []
            elif (admission_reason := self._cost_admission_reason("__check__")) is not None:
                status = "BLOCKED"
                blocked_reason = admission_reason
                notes = admission_reason
                evidence_ids = []
            elif self._driver.supports("__check__"):
                request = ActionRequest(
                    run_id=run_id,
                    case_id=check["id"],
                    step_id="check",
                    action="__check__",
                    isolation="none",
                    effect="read",
                    inputs={"check": check},
                    observe=(),
                    run_context=self._budgeted_run_context(
                        {**self._config.run_context, "manifest": manifest}
                    ),
                    evidence_labels=tuple(check["evidence"]),
                )
                check_executed = False
                try:
                    timeout_s = self._bounded_timeout(self._config.step_timeout_s)
                    check_executed = True
                    response = await asyncio.wait_for(
                        self._driver.execute(request),
                        timeout=timeout_s,
                    )
                    cost_reason = self._record_action_cost("__check__", response, required=True)
                    check_observation = response.observations.get("check")
                    if response.status == "PASS" and (
                        not isinstance(check_observation, dict)
                        or not isinstance(check_observation.get("passed"), bool)
                    ):
                        status = "BLOCKED"
                        notes = "check adapter omitted check.passed"
                    elif response.status == "PASS":
                        status = "PASS" if check_observation["passed"] else "FAIL"
                        notes = response.message or "derived from check.passed"
                    else:
                        status = response.status
                        notes = response.message or "check inspection did not complete"
                    if status == "BLOCKED":
                        blocked_reason = notes
                    evidence_ids = [
                        evidence.write(
                            f"{check['id']}-inspection",
                            {
                                "check": check,
                                "status": response.status,
                                "observations": response.observations,
                                "message": response.message,
                            },
                        )
                    ]
                    for index, descriptor in enumerate(response.evidence, start=1):
                        label = descriptor.get("label")
                        if not isinstance(label, str) or label not in check["evidence"]:
                            status = "BLOCKED"
                            blocked_reason = (
                                "check adapter returned evidence for an undeclared label: "
                                f"{label!r}"
                            )
                            notes = blocked_reason
                            continue
                        evidence_ids.append(
                            evidence.write(
                                f"{check['id']}-adapter-{index}",
                                {key: value for key, value in descriptor.items() if key != "label"},
                                kind=str(descriptor.get("kind", "file")),
                                labels=[label],
                            )
                        )
                    if cost_reason is not None:
                        status = "BLOCKED"
                        blocked_reason = cost_reason
                        notes = cost_reason
                except Exception as exc:
                    if check_executed:
                        self._record_action_cost(
                            "__check__", ActionResponse(status="BLOCKED"), required=True
                        )
                    status = "BLOCKED"
                    blocked_reason = (
                        self._budget_reason
                        if isinstance(exc, TimeoutError) and self._budget_reason is not None
                        else f"check adapter failed: {type(exc).__name__}"
                    )
                    notes = blocked_reason
                    evidence_ids = []
            else:
                status = "BLOCKED"
                blocked_reason = f"check {check['id']} needs inspection evidence"
                notes = blocked_reason
            labels_by_evidence_id = {
                item["evidence_id"]: set(item["labels"]) for item in evidence.records
            }
            provided_labels = {
                label
                for evidence_id in evidence_ids
                for label in labels_by_evidence_id.get(evidence_id, set())
            }
            missing_evidence = sorted(set(check["evidence"]) - provided_labels)
            if status in {"PASS", "FAIL"} and missing_evidence:
                status = "BLOCKED"
                blocked_reason = "missing required check evidence: " + ", ".join(missing_evidence)
                notes = blocked_reason
            if check["priority"] == "P0" and status == "NOT_APPLICABLE":
                status = "BLOCKED"
                blocked_reason = f"P0 check {check['id']} cannot be not applicable"
                notes = blocked_reason
            if status == "BLOCKED" and blocked_reason is None:
                blocked_reason = notes
            check_results.append(
                {
                    "check_id": check["id"],
                    "status": status,
                    "evidence_ids": evidence_ids,
                    "notes": notes,
                    "blocked_reason": blocked_reason,
                }
            )
        return check_results

    def _framework_check(
        self,
        check_id: str,
        cleanup: dict[str, Any],
        preflight: dict[str, Any],
        artifact_safety: dict[str, Any],
    ) -> tuple[str, str]:
        if check_id == "PRE-002" and self._config.baseline_path is None:
            return "PASS", "run is explicitly observation-only with no accepted baseline"
        if check_id == "PRE-002":
            return "PASS", "accepted baseline passed schema and compatibility validation"
        if check_id == "PRE-003":
            return preflight["status"], preflight["reason"]
        if check_id == "OBS-004":
            return artifact_safety["status"], artifact_safety["reason"]
        if check_id == "REP-003":
            if cleanup["status"] in {"PASS", "NOT_APPLICABLE"}:
                return "PASS", "cleanup ledger reconciles or no resources were created"
            return cleanup["status"], "cleanup result determines this check"
        messages = {
            "PRE-001": "runner froze every required manifest and selection field",
            "PRE-004": "contract validation and dataset fingerprinting passed before execution",
            "OBS-004": (
                "runner redacted retained action evidence and never serialized adapter "
                "environment variables"
            ),
            "REP-001": "runner produced exactly one result for every selected check and scenario",
            "REP-002": "runner generates a finding for every failed case and check",
        }
        return "PASS", messages[check_id]

    def _build_findings(
        self,
        *,
        case_results: list[dict[str, Any]],
        check_results: list[dict[str, Any]],
        selected_checks: list[dict[str, Any]],
        framework_evidence_id: str,
    ) -> list[dict[str, Any]]:
        failed_checks = {item["check_id"] for item in check_results if item["status"] == "FAIL"}
        checks_by_id = {item["id"]: item for item in selected_checks}
        findings: list[dict[str, Any]] = []
        covered_checks: set[str] = set()
        for result in case_results:
            if result["status"] != "FAIL":
                continue
            failed_assertions = [
                item for item in result["assertion_results"] if item["status"] == "FAIL"
            ]
            linked_checks = sorted(
                check_id
                for check_id in failed_checks
                if result["case_id"] in checks_by_id[check_id]["scenario_ids"]
            )
            covered_checks.update(linked_checks)
            severity = min(
                (
                    next(
                        assertion["severity"]
                        for assertion in next(
                            case for case in self._cases if case["case_id"] == result["case_id"]
                        )["assertions"]
                        if assertion["id"] == failed["assertion_id"]
                    )
                    for failed in failed_assertions
                ),
                key=lambda item: _SEVERITY_ORDER[item],
                default="HIGH",
            )
            first = failed_assertions[0]
            findings.append(
                {
                    "finding_id": f"F-{result['case_id']}",
                    "severity": severity,
                    "title": f"{result['case_id']} failed at {result['first_bad_boundary']}",
                    "first_bad_boundary": result["first_bad_boundary"],
                    "case_ids": [result["case_id"]],
                    "check_ids": linked_checks,
                    "expected": json.dumps(first["expected"], ensure_ascii=True, sort_keys=True),
                    "observed": json.dumps(first["observed"], ensure_ascii=True, sort_keys=True),
                    "impact": "The evaluated behavior diverged from its versioned contract.",
                    "evidence_ids": result["evidence_ids"] or [framework_evidence_id],
                    "reproduced_count": 1,
                    "root_cause_hypothesis": (
                        "Inspect the first failing action and boundary evidence."
                    ),
                    "confidence": result["root_cause_confidence"],
                    "recommended_action": (
                        "Minimize the failing scenario and fix the first bad boundary."
                    ),
                    "verification_check_ids": linked_checks,
                }
            )
        for check_id in sorted(failed_checks - covered_checks):
            check = checks_by_id[check_id]
            result = next(item for item in check_results if item["check_id"] == check_id)
            findings.append(
                {
                    "finding_id": f"F-{check_id}",
                    "severity": "BLOCKER" if check["priority"] == "P0" else "HIGH",
                    "title": f"{check_id} failed",
                    "first_bad_boundary": check["layer"],
                    "case_ids": [],
                    "check_ids": [check_id],
                    "expected": check["pass_condition"],
                    "observed": result["notes"],
                    "impact": "The selected evaluation check did not meet its pass condition.",
                    "evidence_ids": result["evidence_ids"] or [framework_evidence_id],
                    "reproduced_count": 1,
                    "root_cause_hypothesis": "Inspect the attached inspection evidence.",
                    "confidence": 0.5,
                    "recommended_action": "Resolve the failed check and rerun the same profile.",
                    "verification_check_ids": [check_id],
                }
            )
        return findings

    @staticmethod
    def _gate_reason(
        status: str,
        check_results: list[dict[str, Any]],
        case_results: list[dict[str, Any]],
        cleanup: dict[str, Any],
    ) -> str:
        if status == "PASS":
            return "all gating assertions and P0 checks pass"
        failed = [item["check_id"] for item in check_results if item["status"] == "FAIL"] + [
            item["case_id"] for item in case_results if item["status"] == "FAIL"
        ]
        blocked = [item["check_id"] for item in check_results if item["status"] == "BLOCKED"] + [
            item["case_id"] for item in case_results if item["status"] == "BLOCKED"
        ]
        if cleanup["status"] in {"FAIL", "BLOCKED"}:
            (failed if cleanup["status"] == "FAIL" else blocked).append("cleanup")
        if status == "FAIL":
            return "gating failures: " + ", ".join(failed)
        return "blocked prerequisites: " + ", ".join(blocked)

    @staticmethod
    def _render_summary(report: dict[str, Any]) -> str:
        lines = [
            f"# Evaluation {report['run_id']}",
            "",
            "## Findings",
            "",
        ]
        if report["findings"]:
            for finding in report["findings"]:
                lines.append(
                    f"- **{finding['severity']} {finding['finding_id']}**: {finding['title']}"
                )
        else:
            lines.append("- No failed checks or scenarios.")
        lines.extend(
            [
                "",
                "## Gate",
                "",
                f"- Status: `{report['status']}`",
                f"- Quality status: `{report['quality_status']}`",
                f"- Reason: {report['gate']['reason']}",
                "- Check counts: `"
                f"{json.dumps(report['gate']['check_status_counts'], sort_keys=True)}`",
                "- Case counts: `"
                f"{json.dumps(report['gate']['case_status_counts'], sort_keys=True)}`",
                "",
                "## Coverage",
                "",
                f"- Checks: {len(report['selection']['check_ids'])}",
                f"- Cases: {len(report['selection']['case_ids'])}",
                f"- Evidence records: {len(report['evidence'])}",
                f"- Metrics: {len(report['metrics'])}",
                f"- Judge packets: {len(report['judge_packets'])}",
                "",
                "## Artifacts",
                "",
                "- `report.json`",
                "- `summary.md`",
                "- `evidence/`",
                "",
            ]
        )
        return "\n".join(lines)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="JSON run configuration")
    parser.add_argument(
        "--adapter-command",
        nargs=argparse.REMAINDER,
        help="trusted adapter executable and arguments; this option must be last",
    )
    parser.add_argument(
        "--capabilities",
        help="required comma-separated actions supported by the adapter",
    )
    parser.add_argument(
        "--adapter-timeout",
        type=float,
        help="subprocess deadline; defaults above the maximum runner action deadline",
    )
    return parser


async def _main_async(args: argparse.Namespace) -> int:
    config = RunConfig.from_path(args.config)
    if args.adapter_command:
        if not args.capabilities:
            raise ValueError("--capabilities is required with --adapter-command")
        capabilities = frozenset(item for item in args.capabilities.split(",") if item)
        known_actions = {
            item["name"] for item in load_json(ROOT / "action_catalog.json")["actions"]
        } | {"__check__"}
        unknown_capabilities = sorted(capabilities - known_actions)
        if unknown_capabilities:
            raise ValueError(f"unknown adapter capabilities: {unknown_capabilities}")
        adapter_timeout = _subprocess_timeout(config, args.adapter_timeout)
        driver: ActionDriver = SubprocessActionDriver(
            command=tuple(args.adapter_command),
            timeout_s=adapter_timeout,
            capabilities=capabilities,
        )
    else:
        driver = UnavailableActionDriver()
    outcome = await EvaluationRunner(config, driver).run()
    print(f"{outcome.status}: {outcome.report_path}")
    return {"PASS": 0, "FAIL": 1, "BLOCKED": 2}[outcome.status]


def main() -> int:
    try:
        return asyncio.run(_main_async(_parser().parse_args()))
    except (RunnerError, ValueError, OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
