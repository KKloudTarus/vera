from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import evals.runner as runner_module
import evals.validate as validate_module
from evals.adapters import ActionRequest, ActionResponse, AdapterProtocolError
from evals.judging.aggregate import _execution_report_errors, aggregate
from evals.runner import (
    EvaluationRunner,
    EvidenceStore,
    RunConfig,
    RunnerError,
    _assertion_passes,
    _candidate_output_present,
    _normalize_metric,
    _observation_contract_errors,
    _order_cases,
    _prepare_candidate_for_judging,
    _resolve_inputs,
    _strip_gold_labels,
)
from evals.validate import load_json, validate_report

EVAL_ROOT = Path(__file__).resolve().parents[1]
_PART = re.compile(r"([^\[\]]+)(?:\[(\d+)\])?")
_ALL_EFFECTS = [
    "read",
    "judge",
    "synthetic_write",
    "failure_injection",
    "external",
    "load",
    "cleanup",
]


def _set_path(root: dict[str, Any], path: str, value: Any) -> None:
    current: dict[str, Any] = root
    parts = path.split(".")
    for position, raw in enumerate(parts):
        match = _PART.fullmatch(raw)
        assert match is not None
        name, index_text = match.groups()
        last = position == len(parts) - 1
        if index_text is None:
            if last:
                current[name] = copy.deepcopy(value)
            else:
                if not isinstance(current.get(name), dict):
                    current[name] = {}
                current = current[name]
            continue
        index = int(index_text)
        if not isinstance(current.get(name), list):
            current[name] = []
        items: list[Any] = current[name]
        while len(items) <= index:
            items.append({})
        if last:
            items[index] = copy.deepcopy(value)
        else:
            if not isinstance(items[index], dict):
                items[index] = {}
            current = items[index]


def _delete_path(root: dict[str, Any], path: str) -> None:
    parts = path.split(".")
    current: Any = root
    for raw in parts[:-1]:
        match = _PART.fullmatch(raw)
        if match is None or not isinstance(current, dict):
            return
        name, index_text = match.groups()
        if name not in current:
            return
        current = current[name]
        if index_text is not None:
            index = int(index_text)
            if not isinstance(current, list) or index >= len(current):
                return
            current = current[index]
    match = _PART.fullmatch(parts[-1])
    if match is None or not isinstance(current, dict):
        return
    name, index_text = match.groups()
    if index_text is None:
        current.pop(name, None)
    elif isinstance(current.get(name), list):
        index = int(index_text)
        if index < len(current[name]):
            current[name][index] = None


def _get_path(root: dict[str, Any], path: str) -> Any:
    current: Any = root
    for raw in path.split("."):
        match = _PART.fullmatch(raw)
        if match is None or not isinstance(current, dict):
            return None
        name, index_text = match.groups()
        if name not in current:
            return None
        current = current[name]
        if index_text is not None:
            index = int(index_text)
            if not isinstance(current, list) or index >= len(current):
                return None
            current = current[index]
    return current


def _satisfy_assertions(observations: dict[str, Any], assertions: list[dict[str, Any]]) -> None:
    for assertion in assertions:
        expected = assertion["expected"]
        target = assertion["target"]
        operator = assertion["operator"]
        if isinstance(expected, dict) and set(expected) == {"observation_ref"}:
            reference = expected["observation_ref"]
            reference_value = "reference-value"
            _set_path(observations, reference, reference_value)
            value = "different-value" if operator == "neq" else reference_value
        elif operator == "exists":
            value = "present"
        elif operator == "absent":
            _delete_path(observations, target)
            continue
        elif operator == "contains":
            current = _get_path(observations, target)
            value = list(current) if isinstance(current, list) else []
            if expected not in value:
                value.append(copy.deepcopy(expected))
        elif operator in {"not_contains", "none"}:
            current = _get_path(observations, target)
            value = list(current) if isinstance(current, list) else []
            excluded = expected if isinstance(expected, list) else [expected]
            value = [item for item in value if item not in excluded]
        elif operator == "all":
            current = _get_path(observations, target)
            value = list(current) if isinstance(current, list) else []
            for item in expected:
                if item not in value:
                    value.append(copy.deepcopy(item))
        elif operator == "neq":
            value = "different-value"
        elif operator == "lt":
            value = expected - 1
        elif operator == "gt":
            value = expected + 1
        elif operator == "unchanged" and target.endswith(".after"):
            before_target = f"{target.removesuffix('.after')}.before"
            value = _get_path(observations, before_target)
            if value is None:
                value = "unchanged"
                _set_path(observations, before_target, value)
        elif operator in {"unchanged", "equivalent"}:
            value = True
        else:
            value = copy.deepcopy(expected)
        _set_path(observations, target, value)


class SyntheticDriver:
    def __init__(
        self,
        cases: list[dict[str, Any]],
        *,
        unsupported: set[str] | None = None,
        cleanup_complete: bool = True,
    ) -> None:
        self._cases = {case["case_id"]: case for case in cases}
        self._unsupported = unsupported or set()
        self._cleanup_complete = cleanup_complete
        self._created_cases: set[str] = set()
        self._emitted_case_data: set[str] = set()
        self.calls: list[str] = []

    def supports(self, action: str) -> bool:
        return action not in self._unsupported

    async def execute(self, request: ActionRequest) -> ActionResponse:
        self.calls.append(request.action)
        if request.action == "safety.preflight":
            return ActionResponse(
                status="PASS",
                observations={
                    "safety": {
                        "scope_run_owned": True,
                        "production_writable": False,
                        "cost_bounded": True,
                        "cleanup_supported": True,
                    }
                },
            )
        if request.action == "cleanup.run_scope":
            discovered = sorted(f"resource:{case_id}" for case_id in self._created_cases)
            removed = discovered if self._cleanup_complete else []
            remaining = [] if self._cleanup_complete else discovered
            return ActionResponse(
                status="PASS",
                observations={
                    "cleanup": {
                        "inventory_complete": True,
                        "created_resource_ids": discovered,
                        "removed_resource_ids": removed,
                        "remaining_resource_ids": remaining,
                        "health_restored": self._cleanup_complete,
                    }
                },
                removed_resources=tuple(removed),
            )
        if request.action == "__check__":
            return ActionResponse(
                status="PASS",
                message="synthetic inspection evidence",
                observations={"check": {"passed": True}},
                evidence=tuple(
                    {"label": label, "kind": "file", "synthetic": True}
                    for label in request.inputs["check"]["evidence"]
                ),
            )
        if request.action in {"parity.verify", "result.score"}:
            raise AssertionError(f"runner-owned action reached adapter: {request.action}")
        case = self._cases[request.case_id]
        observations: dict[str, Any] = {}
        for path in request.observe:
            _set_path(observations, path, "observed")
        if request.action == "agent.run":
            observations["agent"] = {
                "answer": (
                    "Synthetic candidate output grounded in the supplied evaluation documents."
                ),
                "tool_calls": [
                    {
                        "tool": "knowledge_get_context",
                        "arguments": {"query": "synthetic"},
                        "provider": "nested-candidate-provider",
                        "metadata": {"model_id": "nested-candidate-model"},
                    }
                ],
                "used_result_ids": ["synthetic-result-1"],
                "citations": [{"result_id": "synthetic-result-1"}],
                "model_id": "synthetic-candidate-model",
                "prompt_version": "synthetic-v1",
                "latency_ms": 1,
                "cost_usd": 0,
            }
        for step in [*case["steps"], *case.get("cleanup", [])]:
            for key, value in step["input"].items():
                if (
                    key.endswith("_ref")
                    and isinstance(value, str)
                    and not value.startswith("fixture.")
                ):
                    _set_path(observations, value, "referenced-value")
        _satisfy_assertions(observations, case["assertions"])
        fixture = validate_module.fixture_data(case)
        if request.case_id == "CUR-003" and request.action == "fixture.seed":
            claims = [
                triple for record in fixture["records"] for triple in record["expected_triples"]
            ]
            observations["expected_claims"] = claims
            observations["actual_claims"] = copy.deepcopy(claims)
        if request.case_id == "RET-001" and request.action == "search.http":
            observations["ranked_results"] = {
                query["query_id"]: list(query["relevance"]) for query in fixture["queries"]
            }
        if request.action == "agent.run":
            if request.case_id == "ANS-001":
                observations["answers"]["budget"]["answer"] = "abstain"
                observations["citations"] = [{"result_id": "synthetic-result-1"}]
                observations["result_ids"] = ["synthetic-result-1"]
            elif request.case_id == "OUT-001":
                observations["agent"] = {
                    **observations["agent"],
                    "answer": ("Platform Team owns Payment API. Payment API runs on prod-cluster."),
                }
            elif request.case_id == "PERF-003":
                observations["runs"] = [
                    {
                        "answer": (
                            "I cannot answer from the available context."
                            if query["expected"] == "abstain"
                            else " ".join(
                                query["expected"]
                                if isinstance(query["expected"], list)
                                else [query["expected"]]
                            )
                        ),
                        "abstained": query["expected"] == "abstain",
                        "citations": (
                            []
                            if query["expected"] == "abstain"
                            else [{"result_id": "synthetic-result-1"}]
                        ),
                        "used_result_ids": (
                            [] if query["expected"] == "abstain" else ["synthetic-result-1"]
                        ),
                        "unsupported_claim_count": 0,
                        "question_index": question_index,
                        "query_id": query["query_id"],
                        "repetition_index": repetition_index,
                        "token_usage": {"total_tokens": 10},
                    }
                    for repetition_index in range(10)
                    for question_index, query in enumerate(fixture["queries"])
                ]
                observations["mcp_token_usage"] = {"total_tokens": 120}
        if request.action == "load.ingestion":
            observations["profiles"] = {
                **observations["profiles"],
                "expected_fixture": fixture,
                "final_state": fixture,
            }
        if request.case_id == "RES-001" and request.action == "projection.wait":
            observations["recovery"] = {
                **observations["recovery"],
                "graph": fixture,
            }
        if request.case_id == "TEMP-007" and request.action == "projection.rebuild":
            observations["rebuild"] = {"duration_ms": 1.0}
        if request.case_id == "TEMP-007" and request.action == "state.snapshot":
            snapshot = {
                "current_search": ["current"],
                "as_of_search": ["historical"],
                "intervals": ["interval"],
                "provenance": ["source"],
            }
            observations[request.observe[0]] = snapshot
        declared_observations: dict[str, Any] = {}
        for path in request.observe:
            root = re.split(r"[.[]", path, maxsplit=1)[0]
            value = _get_path(observations, root)
            if value is not None:
                _set_path(declared_observations, root, value)
        first_response = request.case_id not in self._emitted_case_data
        self._emitted_case_data.add(request.case_id)
        observed_roots = {re.split(r"[.[]", path, maxsplit=1)[0] for path in request.observe}
        step_labels = {
            label
            for assertion in case["assertions"]
            if re.split(r"[.[]", assertion["target"], maxsplit=1)[0] in observed_roots
            for label in assertion["evidence"]
        } | set(request.evidence_labels)
        adapter_evidence = tuple(
            {"label": label, "kind": "file", "synthetic": True} for label in sorted(step_labels)
        )
        adapter_metrics = (
            tuple(
                {
                    "name": name,
                    "unit": "count",
                    "value": 1,
                    "sample_size": 1000,
                }
                for name in case.get("metrics", [])
                if name not in runner_module._runner_owned_metric_names(case)
            )
            if first_response
            else ()
        )
        created: tuple[str, ...] = ()
        if request.case_id not in self._created_cases and request.effect != "read":
            self._created_cases.add(request.case_id)
            created = (f"resource:{request.case_id}",)
        return ActionResponse(
            status="PASS",
            observations=declared_observations,
            metrics=adapter_metrics,
            evidence=adapter_evidence,
            created_resources=created,
        )


class FailingActionDriver(SyntheticDriver):
    async def execute(self, request: ActionRequest) -> ActionResponse:
        response = await super().execute(request)
        if request.action == "record.ingest":
            return ActionResponse(
                status="FAIL",
                observations=response.observations,
                message="synthetic action failure",
                metrics=response.metrics,
                evidence=response.evidence,
                created_resources=response.created_resources,
            )
        return response


class BlockedResilienceDriver(SyntheticDriver):
    def __init__(self, cases: list[dict[str, Any]]) -> None:
        super().__init__(cases)
        self.requests: list[tuple[str, str, str, str | None]] = []

    async def execute(self, request: ActionRequest) -> ActionResponse:
        state = request.inputs.get("state")
        self.requests.append(
            (
                request.case_id,
                request.step_id,
                request.action,
                str(state) if state is not None else None,
            )
        )
        response = await super().execute(request)
        if request.case_id == "RES-001" and request.action == "fixture.seed":
            return ActionResponse(status="BLOCKED", message="synthetic resilience timeout")
        return response


class MissingEvidenceDriver(SyntheticDriver):
    async def execute(self, request: ActionRequest) -> ActionResponse:
        response = await super().execute(request)
        return ActionResponse(
            status=response.status,
            observations=response.observations,
            message=response.message,
            metrics=response.metrics,
            created_resources=response.created_resources,
            removed_resources=response.removed_resources,
        )


class NoResourceDriver(SyntheticDriver):
    async def execute(self, request: ActionRequest) -> ActionResponse:
        response = await super().execute(request)
        return ActionResponse(
            status=response.status,
            observations=response.observations,
            message=response.message,
            metrics=response.metrics,
            evidence=response.evidence,
            removed_resources=response.removed_resources,
        )


class NoInventoryCleanupDriver(SyntheticDriver):
    async def execute(self, request: ActionRequest) -> ActionResponse:
        if request.action == "cleanup.run_scope":
            self.calls.append(request.action)
            return ActionResponse(status="PASS")
        return await super().execute(request)


class StaleObservationDriver(SyntheticDriver):
    def __init__(self, cases: list[dict[str, Any]]) -> None:
        super().__init__(cases)
        self._dependency_calls = 0

    async def execute(self, request: ActionRequest) -> ActionResponse:
        response = await super().execute(request)
        if request.action == "dependency.configure":
            self._dependency_calls += 1
            if self._dependency_calls == 2:
                return ActionResponse(
                    status="PASS",
                    metrics=response.metrics,
                    evidence=response.evidence,
                    created_resources=response.created_resources,
                )
        return response


class UnsafePreflightDriver(SyntheticDriver):
    async def execute(self, request: ActionRequest) -> ActionResponse:
        if request.action == "safety.preflight":
            self.calls.append(request.action)
            return ActionResponse(
                status="PASS",
                observations={
                    "safety": {
                        "scope_run_owned": True,
                        "production_writable": True,
                        "cost_bounded": True,
                        "cleanup_supported": True,
                    }
                },
            )
        return await super().execute(request)


class UnderSampledDriver(SyntheticDriver):
    async def execute(self, request: ActionRequest) -> ActionResponse:
        response = await super().execute(request)
        return ActionResponse(
            status=response.status,
            observations=response.observations,
            message=response.message,
            metrics=tuple({**metric, "sample_size": 1} for metric in response.metrics),
            evidence=response.evidence,
            created_resources=response.created_resources,
            removed_resources=response.removed_resources,
        )


class MissingMetricsDriver(SyntheticDriver):
    async def execute(self, request: ActionRequest) -> ActionResponse:
        response = await super().execute(request)
        return ActionResponse(
            status=response.status,
            observations=response.observations,
            message=response.message,
            evidence=response.evidence,
            created_resources=response.created_resources,
            removed_resources=response.removed_resources,
        )


class EmptyCandidateDriver(SyntheticDriver):
    async def execute(self, request: ActionRequest) -> ActionResponse:
        response = await super().execute(request)
        if request.action != "agent.run" or request.case_id != "REAL-001":
            return response
        observations = copy.deepcopy(response.observations)
        observations["agent"]["answer"] = "   "
        return ActionResponse(
            status=response.status,
            observations=observations,
            message=response.message,
            metrics=response.metrics,
            evidence=response.evidence,
            created_resources=response.created_resources,
            removed_resources=response.removed_resources,
        )


def _fill_nulls(value: Any) -> Any:
    if value is None:
        return 1
    if isinstance(value, dict):
        return {key: _fill_nulls(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_fill_nulls(item) for item in value]
    return value


def _contract_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, fill_production: bool
) -> Path:
    root = tmp_path / "evals"
    shutil.copytree(
        EVAL_ROOT,
        root,
        ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache", "runs", "tests"),
    )
    if fill_production:
        path = root / "fixtures" / "production.json"
        path.write_text(
            json.dumps(_fill_nulls(load_json(path)), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    monkeypatch.setattr(validate_module, "ROOT", root)
    monkeypatch.setattr(runner_module, "ROOT", root)
    return root


def _config(root: Path, *, profile: str, run_id: str, baseline: Path | None = None) -> RunConfig:
    return RunConfig.from_dict(
        {
            "profile": profile,
            "environment": "ephemeral-test",
            "service_version": "0.1.0",
            "git_sha": "0123456789abcdef0123456789abcdef01234567",
            "git_dirty": False,
            "graph_backend": "neo4j",
            "hardware_profile": "test",
            "cache_state": "disabled",
            "concurrency": 1,
            "random_seed": 20260829,
            "ontology_version": "2",
            "models": {},
            "pipeline_versions": {},
            "quality_config": {"fabric_write_mode": "fabric"},
            "run_context": {
                "principal": "synthetic",
                "evaluation_scope": {
                    "id": "eval-test-scope",
                    "kind": "ephemeral_stack",
                    "run_owned": True,
                    "production_writable": False,
                },
                "cost_budget": {"max_duration_s": 3600, "max_cost_usd": 10},
            },
            "allowed_effects": _ALL_EFFECTS,
            "output_root": str(root / "test-runs"),
            "baseline_path": str(baseline) if baseline else None,
            "run_id": run_id,
        },
        root=root,
    )


def _compatible_baseline(
    root: Path,
    cases: list[dict[str, Any]],
    *,
    metrics: list[dict[str, Any]],
) -> dict[str, Any]:
    config = _config(root, profile="weekly", run_id="weekly-manifest-template")
    runner = EvaluationRunner(config, SyntheticDriver(cases), root=root)
    checks = load_json(root / "checklist.json")["items"]
    selection = {
        "check_ids": [item["id"] for item in checks if "weekly" in item["profiles"]],
        "case_ids": [item["case_id"] for item in cases if "weekly" in item["profiles"]],
    }
    manifest = runner._build_manifest(datetime.now(UTC), selection)
    compatibility_fields = [
        "dataset_sha256",
        "selection_sha256",
        "checklist_sha256",
        "action_catalog_sha256",
        "execution_profile_sha256",
        "config_fingerprint",
        "service_version",
        "graph_backend",
        "cache_state",
        "concurrency",
        "hardware_profile",
        "models",
        "pipeline_versions",
        "ontology_version",
    ]
    return {
        "schema_version": "1.0",
        "baseline_id": "weekly-compatible",
        "promoted_at": "2026-08-29T00:00:00Z",
        "promoted_by": "test",
        "source_run_uri": "artifact://baseline/report.json",
        "source_report_sha256": "0" * 64,
        "compatibility": {
            "profile": "weekly",
            **{field: manifest[field] for field in compatibility_fields},
        },
        "metrics": metrics,
        "threshold_policies": [],
    }


def _external_panel_inputs(tmp_path: Path, packet_path: Path) -> tuple[Path, list[Path]]:
    packet = load_json(packet_path)
    packet_sha256 = hashlib.sha256(packet_path.read_bytes()).hexdigest()
    actors = [
        ("grounding", "provider-a", "family-a"),
        ("task_utility", "provider-b", "family-b"),
        ("adversarial_safety", "provider-a", "family-c"),
        ("synthesis_uncertainty", "provider-b", "family-d"),
    ]
    assigned = [
        {
            "actor_id": f"integration-judge-{index + 1}",
            "role": role,
            "provider": provider,
            "model_family": family,
            "model_id": f"{family}-model",
            "version": "test",
        }
        for index, (role, provider, family) in enumerate(actors)
    ]
    assignment_path = tmp_path / "panel-assignment.json"
    assignment_path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "panel_id": "integration-panel",
                "packet_id": packet["packet_id"],
                "packet_sha256": packet_sha256,
                "issued_by": "integration-test",
                "created_at": "2026-08-29T00:00:00Z",
                "judges": assigned,
            }
        ),
        encoding="utf-8",
    )
    judgment_paths: list[Path] = []
    for actor in assigned:
        judgment_path = tmp_path / f"{actor['actor_id']}.json"
        judgment_path.write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "packet_id": packet["packet_id"],
                    "packet_sha256": packet_sha256,
                    "judge": {**actor, "independence_attested": True},
                    "dimension_results": {
                        dimension["id"]: {
                            "status": "SCORED",
                            "score": 0.9,
                            "rationale": "Grounded in the packet output.",
                            "evidence_refs": ["candidate:output"],
                        }
                        for dimension in packet["rubric"]["dimensions"]
                    },
                    "critical_failures": [],
                    "overall_score": 0.9,
                    "confidence": 0.9,
                    "overall_rationale": "The response satisfies the rubric.",
                    "strengths": ["Useful"],
                    "weaknesses": [],
                    "recommended_improvement": "Keep citations concise.",
                }
            ),
            encoding="utf-8",
        )
        judgment_paths.append(judgment_path)
    return assignment_path, judgment_paths


def test_all_declared_scenarios_execute_through_the_production_protocol(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=True)
    cases = validate_module.load_cases()
    driver = SyntheticDriver(cases)

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="release", run_id="release-all-actions"),
            driver,
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    catalog_actions = {item["name"] for item in load_json(root / "action_catalog.json")["actions"]}
    assert outcome.status == "PASS"
    assert report["schema_version"] == "1.1"
    assert len(report["case_results"]) == len(
        [case for case in cases if "release" in case["profiles"]]
    )
    assert all(result["status"] == "PASS" for result in report["case_results"])
    assert catalog_actions - runner_module._RUNNER_ACTIONS <= set(driver.calls)
    assert runner_module._RUNNER_ACTIONS.isdisjoint(driver.calls)
    assert report["cleanup"]["status"] == "PASS"
    assert report["quality_status"] == "PENDING_JUDGMENT"
    assert len(report["judge_packets"]) == 5
    result_ids = [item["case_id"] for item in report["case_results"]]
    assert result_ids.index("PERF-001") < result_ids.index("OPS-001")
    assert result_ids.index("PERF-002") < result_ids.index("OPS-001")
    assert result_ids.index("PERF-001") < result_ids.index("OPS-010")
    packet = load_json(Path(report["judge_packets"][0]["ref"]))
    serialized_packet = json.dumps(packet, sort_keys=True)
    assert packet["candidate"]["identity_blinded"] is True
    assert packet["candidate"]["alias"] == "candidate-A"
    assert packet["source_material"]["document_count"] == 25
    assert "synthetic-candidate-model" not in serialized_packet
    assert "synthetic-v1" not in serialized_packet
    assert "nested-candidate-provider" not in serialized_packet
    assert "nested-candidate-model" not in serialized_packet
    assert "execution_report_ref" not in serialized_packet
    assert (
        validate_report(
            outcome.report_path,
            load_json(root / "checklist.json")["items"],
            cases,
        )
        == []
    )
    assert _execution_report_errors(outcome.report_path) == []
    packet_path = Path(report["judge_packets"][0]["ref"])
    assignment_path, judgments = _external_panel_inputs(tmp_path, packet_path)
    panel_result, panel_errors = aggregate(
        packet_path, outcome.report_path, judgments, assignment_path
    )
    assert panel_errors == []
    assert panel_result is not None
    assert panel_result["status"] == "PASS"

    tampered = copy.deepcopy(report)
    assertion = next(
        item
        for case_result in tampered["case_results"]
        for item in case_result["assertion_results"]
        if item["status"] == "PASS" and item["operator"] == "eq"
    )
    assertion["observed"] = "tampered-observation"
    outcome.report_path.write_text(
        json.dumps(tampered, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert any(
        "assertion outcome contradicts observation" in error
        for error in validate_report(
            outcome.report_path,
            load_json(root / "checklist.json")["items"],
            cases,
        )
    )


def test_case_order_honors_dependencies_before_priority() -> None:
    cases = [
        {"case_id": "OPS-001", "priority": "P0"},
        {"case_id": "PERF-001", "priority": "P1"},
        {"case_id": "SEC-001", "priority": "P0"},
    ]

    ordered = _order_cases(cases, {"OPS-001": ["PERF-001"]})

    assert [case["case_id"] for case in ordered] == ["SEC-001", "PERF-001", "OPS-001"]


def test_case_order_rejects_dependency_cycle() -> None:
    cases = [
        {"case_id": "OPS-001", "priority": "P0"},
        {"case_id": "PERF-001", "priority": "P1"},
    ]

    with pytest.raises(RunnerError, match="case dependency cycle"):
        _order_cases(cases, {"OPS-001": ["PERF-001"], "PERF-001": ["OPS-001"]})


def test_empty_final_answer_does_not_create_a_judge_packet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-empty-candidate"),
            EmptyCandidateDriver(cases),
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    result = next(item for item in report["case_results"] if item["case_id"] == "REAL-001")
    assert outcome.status == "BLOCKED"
    assert result["status"] == "BLOCKED"
    assert result["quality_status"] == "NOT_ELIGIBLE"
    assert "candidate final output is empty" in result["blocked_reason"]
    assert all(item["case_id"] != "REAL-001" for item in report["judge_packets"])


def test_release_blocks_before_production_actions_when_targets_are_unresolved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    production_path = root / "fixtures" / "production.json"
    production = load_json(production_path)
    production["targets"]["search_p95_ms"] = None
    production_path.write_text(json.dumps(production), encoding="utf-8")
    cases = validate_module.load_cases()
    driver = SyntheticDriver(cases)

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="release", run_id="release-null-targets"),
            driver,
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    ops = {
        item["case_id"]: item
        for item in report["case_results"]
        if item["case_id"].startswith("OPS-")
    }
    assert outcome.status == "BLOCKED"
    assert ops["OPS-001"]["status"] == "BLOCKED"
    assert "unresolved null target values" in ops["OPS-001"]["blocked_reason"]


def test_unconfigured_driver_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-no-adapter"),
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    assert outcome.status == "BLOCKED"
    assert report["blocked_prerequisites"]
    assert all(result["status"] == "BLOCKED" for result in report["case_results"])


def test_existing_product_capability_without_adapter_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    driver = SyntheticDriver(
        cases,
        unsupported={"artifact.reextract", "source.tombstone", "search.transaction_as_of"},
    )

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="nightly", run_id="nightly-product-gaps"),
            driver,
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    results = {item["case_id"]: item for item in report["case_results"]}
    assert results["ING-003"]["status"] == "BLOCKED"
    assert results["ING-003"]["blocked_reason"] == (
        "no configured adapter supports action artifact.reextract"
    )
    assert (
        next(
            item for item in results["ING-003"]["assertion_results"] if item["assertion_id"] == "A1"
        )["status"]
        == "BLOCKED"
    )


def test_cleanup_residue_fails_the_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    driver = SyntheticDriver(cases, cleanup_complete=False)

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-cleanup-residue"),
            driver,
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    assert outcome.status == "FAIL"
    assert report["cleanup"]["status"] == "FAIL"
    assert report["cleanup"]["remaining_resources"]


def test_unsafe_preflight_stops_before_scenario_actions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    driver = UnsafePreflightDriver(cases)

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-unsafe-preflight"),
            driver,
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    assert outcome.status == "FAIL"
    assert driver.calls == ["safety.preflight"]
    assert (
        next(item for item in report["check_results"] if item["check_id"] == "PRE-003")["status"]
        == "FAIL"
    )


def test_action_failure_cannot_pass_from_satisfying_observations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    driver = FailingActionDriver(cases)

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-action-failure"),
            driver,
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    result = next(item for item in report["case_results"] if item["case_id"] == "E2E-001")
    assert outcome.status == "FAIL"
    assert result["status"] == "FAIL"
    assert "cleanup.run_scope" in driver.calls


def test_case_cleanup_restores_dependency_after_regular_step_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    driver = BlockedResilienceDriver(cases)

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="nightly", run_id="nightly-resilience-cleanup"),
            driver,
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    result = next(item for item in report["case_results"] if item["case_id"] == "RES-001")
    requests = [request for request in driver.requests if request[0] == "RES-001"]
    assert result["status"] == "BLOCKED"
    assert requests == [
        ("RES-001", "S1", "dependency.configure", "unavailable"),
        ("RES-001", "S2", "fixture.seed", None),
        ("RES-001", "S6", "dependency.configure", "available"),
    ]


def test_missing_declared_evidence_blocks_observed_assertions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-missing-evidence"),
            MissingEvidenceDriver(cases),
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    result = next(item for item in report["case_results"] if item["case_id"] == "E2E-001")
    assert outcome.status == "BLOCKED"
    assert result["status"] == "BLOCKED"
    assert "missing required evidence" in result["blocked_reason"]


def test_cleanup_runs_after_mutation_without_reported_resource_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    driver = NoResourceDriver(cases)

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-empty-ledger"),
            driver,
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    assert outcome.status == "PASS"
    assert report["cleanup"]["status"] == "PASS"
    assert "cleanup.run_scope" in driver.calls


def test_cleanup_cannot_pass_without_an_independent_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-missing-cleanup-inventory"),
            NoInventoryCleanupDriver(cases),
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    assert outcome.status == "BLOCKED"
    assert report["cleanup"]["status"] == "BLOCKED"
    assert "resource inventory" in report["cleanup"]["notes"]


def test_repeated_observation_must_be_returned_by_the_current_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()

    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="nightly", run_id="nightly-stale-observation"),
            StaleObservationDriver(cases),
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    result = next(item for item in report["case_results"] if item["case_id"] == "PROJ-001")
    assert outcome.status == "FAIL"
    assert result["status"] == "FAIL"
    assert "omitted observations" in result["assertion_results"][0]["notes"]


@pytest.mark.parametrize(
    ("operator", "observed", "expected", "passed"),
    [
        ("eq", 2, 2, True),
        ("neq", 2, 3, True),
        ("contains", ["a", "b"], "a", True),
        ("contains", ["Migration Team OWNS Legacy API"], "Migration Team", True),
        ("contains", ["checkout service DEPENDS_ON inventory api"], "Inventory API", True),
        ("contains", [{"fact": "Migration Team OWNS Legacy API"}], "Migration Team", True),
        ("contains", [{"citation": {"source": "cluster-a"}}], "cluster-a", True),
        ("not_contains", ["a"], "b", True),
        ("not_contains", ["Migration Team OWNS Legacy API"], "Migration Team", False),
        ("not_contains", [{"fact": "Migration Team OWNS Legacy API"}], "Legacy API", False),
        ("exists", "value", True, True),
        ("gte", 2, 2, True),
        ("lte", 2, 3, True),
        ("lt", 2, 3, True),
        ("gt", 3, 2, True),
        ("all", ["a", "b"], ["a", "b"], True),
        ("all", [{"fact": "alpha beta"}], ["alpha", "beta"], True),
        ("none", ["a"], ["b"], True),
        ("none", [{"fact": "alpha beta"}], ["gamma"], True),
        ("unchanged", True, True, True),
        ("equivalent", True, True, True),
    ],
)
def test_assertion_operators(operator: str, observed: Any, expected: Any, passed: bool) -> None:
    result, _, _ = _assertion_passes(
        {"target": "value", "operator": operator, "expected": expected},
        {"value": observed},
    )
    assert result is passed


@pytest.mark.parametrize("after", [{"artifacts": 1}, {"artifacts": 2}])
def test_unchanged_operator_compares_sibling_snapshots(after: dict[str, int]) -> None:
    before = {"artifacts": 1}
    result, observed, _ = _assertion_passes(
        {"target": "counts.after", "operator": "unchanged", "expected": True},
        {"counts": {"before": before, "after": after}},
    )

    assert result is (after == before)
    assert observed == {"before": before, "after": after}


def test_runner_computes_parity_from_raw_snapshots() -> None:
    response = runner_module._execute_runner_action(
        "parity.verify",
        {
            "before_ref": {
                "current_search": ["cluster-a"],
                "as_of_search": ["cluster-old"],
                "intervals": ["i1"],
                "provenance": ["source-a"],
            },
            "after_ref": {
                "current_search": ["cluster-b"],
                "as_of_search": ["cluster-old"],
                "intervals": ["i1"],
                "provenance": ["source-a"],
            },
            "comparison_policy": "temporal-v1",
        },
        fixture={},
        observations={},
        declared_metrics=set(),
        evidence_labels=["parity_diff"],
    )

    assert response.status == "PASS"
    assert response.observations["parity"] == {
        "current": False,
        "as_of": True,
        "intervals": True,
        "provenance": True,
    }


def test_runner_emits_declared_temporal_parity_metrics() -> None:
    response = runner_module._execute_runner_action(
        "parity.verify",
        {
            "before_ref": {
                "current_search": ["cluster-a"],
                "as_of_search": ["cluster-old"],
                "intervals": ["i1"],
                "provenance": ["source-a"],
            },
            "after_ref": {
                "current_search": ["cluster-a"],
                "as_of_search": ["cluster-old"],
                "intervals": ["i1"],
                "provenance": ["source-a"],
            },
            "comparison_policy": "temporal-v1",
        },
        fixture={},
        observations={"rebuild": {"duration_ms": 12.5}},
        declared_metrics={"projection_parity", "temporal_parity", "rebuild_duration_ms"},
        evidence_labels=["parity_diff"],
    )

    assert response.status == "PASS"
    assert {metric["name"]: metric["value"] for metric in response.metrics} == {
        "projection_parity": 1.0,
        "temporal_parity": 1.0,
        "rebuild_duration_ms": 12.5,
    }


def test_runner_scores_extraction_from_fixture_and_raw_claims() -> None:
    fixture = {
        "records": [
            {"expected_triples": [{"subject": "Platform", "predicate": "OWNS", "object": "API"}]}
        ]
    }
    response = runner_module._execute_runner_action(
        "result.score",
        {"rubric_version": "extraction-v1"},
        fixture=fixture,
        observations={
            "actual_claims": [
                {"subject": "Platform", "predicate": "OWNS", "object": "Wrong API"},
                {"subject": "", "predicate": "OWNS", "object": "API"},
            ]
        },
        declared_metrics={"claim_precision", "claim_recall", "schema_invalid_count"},
        evidence_labels=["score_report"],
    )

    assert response.status == "PASS"
    assert response.observations["score"]["schema_invalid_count"] == 1
    metrics = {metric["name"]: metric["value"] for metric in response.metrics}
    assert metrics == {
        "claim_precision": 0.0,
        "claim_recall": 0.0,
        "schema_invalid_count": 1,
    }


def test_absent_operator_accepts_a_missing_path() -> None:
    result, observed, _ = _assertion_passes(
        {"target": "missing", "operator": "absent", "expected": True}, {}
    )
    assert result
    assert observed is None


def test_answer_score_uses_structured_abstention() -> None:
    score, _metrics = runner_module._answer_score(
        {
            "questions": [
                {"expected": "cluster-current"},
                {"expected": "abstain"},
            ]
        },
        {
            "answers": {
                "current": "Payment API runs on cluster-current.",
                "budget": {
                    "answer": "The context does not include budget information.",
                    "abstained": True,
                },
            },
            "result_ids": ["result-1"],
            "citations": [{"result_id": "result-1"}],
        },
    )

    assert score["unsupported_claim_count"] == 0


def test_answer_score_maps_repeated_runs_and_counts_only_accepted_tokens() -> None:
    fixture = {
        "queries": [
            {"query_id": "q-owner", "expected": "Platform Team"},
            {"query_id": "q-negative", "expected": "abstain"},
        ]
    }
    runs = [
        {
            "query_id": "q-owner",
            "question_index": 0,
            "answer": "Platform Team owns it.",
            "abstained": False,
            "used_result_ids": ["r1"],
            "citations": [{"result_id": "r1"}],
            "unsupported_claim_count": 0,
            "token_usage": {"total_tokens": 20},
        },
        {
            "query_id": "q-negative",
            "question_index": 1,
            "answer": "Not available.",
            "abstained": True,
            "used_result_ids": [],
            "citations": [],
            "unsupported_claim_count": 0,
            "token_usage": {"prompt_tokens": 8, "completion_tokens": 2},
        },
        {
            "query_id": "q-owner",
            "question_index": 0,
            "answer": "Wrong Team owns it.",
            "abstained": False,
            "used_result_ids": ["r1"],
            "citations": [{"result_id": "r1"}],
            "unsupported_claim_count": 0,
            "token_usage": {"total_tokens": 100},
        },
    ]

    score, metrics = runner_module._answer_score(fixture, {"runs": runs})

    assert score["material_error_count"] == 1
    by_name = {metric["name"]: metric for metric in metrics}
    assert by_name["task_success"]["value"] == pytest.approx(2 / 3)
    assert by_name["task_success"]["sample_size"] == 3
    assert by_name["tokens_per_accepted_answer"]["value"] == 15
    assert by_name["tokens_per_accepted_answer"]["sample_size"] == 2


def test_input_resolution_uses_fixture_context_and_prior_observations() -> None:
    resolved = _resolve_inputs(
        {
            "fixture": "records[0]",
            "group_ref": "scope.group_id",
            "queries_ref": "fixture.queries",
        },
        fixture={"records": [{"id": "r1"}], "queries": ["q1"]},
        observations={"scope": {"group_id": "g1"}},
        run_context={},
    )
    assert resolved == {
        "fixture": {"id": "r1"},
        "group_ref": "g1",
        "queries_ref": ["q1"],
    }


def test_gold_labels_are_removed_before_adapter_execution() -> None:
    blinded = _strip_gold_labels(
        {
            "questions_ref": [
                {"text": "Where does it run?", "expected": "cluster-a"},
            ],
            "fixture_file": {
                "data": {
                    "records": [
                        {
                            "body": "Platform Team owns Payment API.",
                            "expected_triples": [{"subject": "Platform Team"}],
                        }
                    ],
                    "queries": [{"text": "owner", "relevance": {"fact-1": 3}}],
                    "expected_sha256": "secret-gold-digest",
                    "corpus_sha256": "operational-corpus-digest",
                }
            },
        }
    )

    assert blinded["questions_ref"] == [{"text": "Where does it run?"}]
    assert blinded["fixture_file"]["data"]["records"] == [
        {"body": "Platform Team owns Payment API."}
    ]
    assert blinded["fixture_file"]["data"]["queries"] == [{"text": "owner"}]
    assert "expected_sha256" not in blinded["fixture_file"]["data"]
    assert blinded["fixture_file"]["data"]["corpus_sha256"] == "operational-corpus-digest"


def test_adapter_observations_cannot_escape_declared_boundary_roots() -> None:
    errors = _observation_contract_errors(
        {
            "search": {"http": {"results": []}},
            "score": {"material_error_count": 0},
        },
        ("search.http",),
    )

    assert errors == ["undeclared observation paths: ['score.material_error_count']"]


def test_adapter_metric_cannot_declare_runner_owned_fields() -> None:
    with pytest.raises(AdapterProtocolError, match="runner-owned or unknown fields"):
        _normalize_metric(
            {
                "name": "hit_at_5",
                "unit": "ratio",
                "value": 1,
                "sample_size": 10,
                "status": "PASS",
            },
            owner_id="RET-001",
            metric_id="metric-1",
            declared_names={"hit_at_5"},
        )


def test_fixture_input_cannot_be_shadowed_by_observations() -> None:
    resolved = _resolve_inputs(
        {"fixture": "record"},
        fixture={"record": {"id": "frozen"}},
        observations={"record": {"id": "adapter-controlled"}},
        run_context={},
    )
    assert resolved == {"fixture": {"id": "frozen"}}


def test_evidence_store_redacts_secrets_and_payloads(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path, retain_payloads=False)
    evidence_id = store.write(
        "redaction",
        {"authorization": "Bearer secret", "body": "private body", "count": 2},
    )
    record = next(item for item in store.records if item["evidence_id"] == evidence_id)
    saved = load_json(Path(record["ref"]))
    assert saved["authorization"] == "[REDACTED]"
    assert saved["body"]["length"] == len("private body")
    assert saved["count"] == 2


def test_evidence_store_redacts_structured_payloads_and_key_variants(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path, retain_payloads=False)
    evidence_id = store.write(
        "redaction-variants",
        {
            "client-secret": "secret",
            "x-api-key": "secret",
            "email": "person@example.com",
            "body": {"nested": "private"},
        },
    )
    record = next(item for item in store.records if item["evidence_id"] == evidence_id)
    saved = load_json(Path(record["ref"]))
    assert saved["client-secret"] == "[REDACTED]"
    assert saved["x-api-key"] == "[REDACTED]"
    assert saved["email"] == "[REDACTED]"
    assert set(saved["body"]) == {"length", "sha256"}
    assert store.audit() == []


def test_retain_payloads_rejects_string_booleans() -> None:
    value = load_json(EVAL_ROOT / "run.example.json")
    value["retain_synthetic_payloads"] = "false"
    with pytest.raises(ValueError, match="must be a boolean"):
        RunConfig.from_dict(value, root=EVAL_ROOT)


@pytest.mark.parametrize("value", [None, False, True, 0, 1, "", "   ", [], {}, ()])
def test_empty_candidate_outputs_are_not_judgeable(value: Any) -> None:
    assert _candidate_output_present(value) is False


def test_structured_candidate_answer_is_preserved_while_metadata_is_blinded() -> None:
    output, tool_trace, citations, result_ids = _prepare_candidate_for_judging(
        {
            "answer": {
                "provider": "source-provider",
                "model_id": "domain-model",
                "recommendation": "Proceed conditionally.",
            },
            "provider": "candidate-provider",
            "model_id": "candidate-model",
            "tool_calls": [
                {
                    "tool": "knowledge_get_context",
                    "provider": "candidate-provider",
                }
            ],
            "citations": [],
            "used_result_ids": [],
        }
    )

    assert output["provider"] == "source-provider"
    assert output["model_id"] == "domain-model"
    assert tool_trace == [{"tool": "knowledge_get_context"}]
    assert citations == []
    assert result_ids == []


def test_candidate_envelope_requires_an_explicit_final_output() -> None:
    with pytest.raises(RunnerError, match="lacks a final-output field"):
        _prepare_candidate_for_judging({"latency_ms": 1, "cost_usd": 0, "tool_calls": []})


def test_report_validation_detects_tampered_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-tamper"),
            SyntheticDriver(cases),
            root=root,
        ).run()
    )
    report = load_json(outcome.report_path)
    Path(report["evidence"][0]["ref"]).write_text("tampered", encoding="utf-8")

    errors = validate_report(
        outcome.report_path, load_json(root / "checklist.json")["items"], cases
    )

    assert any("evidence SHA-256 mismatch" in error for error in errors)


def test_report_validation_detects_tampered_judge_packet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-packet-tamper"),
            SyntheticDriver(cases),
            root=root,
        ).run()
    )
    report = load_json(outcome.report_path)
    Path(report["judge_packets"][0]["ref"]).write_text("tampered", encoding="utf-8")

    errors = validate_report(
        outcome.report_path, load_json(root / "checklist.json")["items"], cases
    )

    assert any("judge packet SHA-256 mismatch" in error for error in errors)


def test_report_validation_rejects_non_object_json(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    report_path.write_text("[]", encoding="utf-8")

    errors = validate_report(
        report_path,
        load_json(EVAL_ROOT / "checklist.json")["items"],
        validate_module.load_cases(),
    )

    assert any("schema" in error for error in errors)


def test_missing_declared_metrics_blocks_the_case(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="daily", run_id="daily-missing-metrics"),
            MissingMetricsDriver(cases),
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    result = next(item for item in report["case_results"] if item["case_id"] == "E2E-001")
    assert outcome.status == "BLOCKED"
    assert "omitted declared metrics" in result["blocked_reason"]


def test_minimum_sample_size_is_enforced(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    outcome = asyncio.run(
        EvaluationRunner(
            _config(root, profile="weekly", run_id="weekly-under-sampled"),
            UnderSampledDriver(cases),
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    result = next(item for item in report["case_results"] if item["case_id"] == "PERF-001")
    assert outcome.status == "BLOCKED"
    assert result["status"] == "BLOCKED"
    assert "fewer than 200 samples" in result["blocked_reason"]


def test_non_gating_baseline_regression_does_not_fail_the_run_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    baseline = _compatible_baseline(
        root,
        cases,
        metrics=[
            {
                "name": "hit_at_5",
                "dimensions": {},
                "unit": "count",
                "value": 2,
                "sample_size": 1000,
            },
            {
                "name": "time_to_searchable_p95_ms",
                "dimensions": {},
                "unit": "count",
                "value": 1,
                "sample_size": 1000,
            },
            {
                "name": "agent_p95_ms",
                "dimensions": {},
                "unit": "count",
                "value": 1,
                "sample_size": 1000,
            },
            {
                "name": "tokens_per_accepted_answer",
                "dimensions": {},
                "unit": "count",
                "value": 1,
                "sample_size": 1000,
            },
        ],
    )
    baseline_path = tmp_path / "weekly-baseline.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")

    outcome = asyncio.run(
        EvaluationRunner(
            _config(
                root,
                profile="weekly",
                run_id="weekly-non-gating-regression",
                baseline=baseline_path,
            ),
            SyntheticDriver(cases),
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    result = next(item for item in report["case_results"] if item["case_id"] == "PERF-001")
    baseline_assertion = next(
        item for item in result["assertion_results"] if item["assertion_id"] == "A3"
    )
    assert outcome.status == "PASS"
    assert result["status"] == "FAIL"
    assert baseline_assertion["status"] == "FAIL"


def test_empty_compatible_baseline_keeps_comparisons_not_applicable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    cases = validate_module.load_cases()
    baseline_path = tmp_path / "weekly-empty-baseline.json"
    baseline_path.write_text(
        json.dumps(_compatible_baseline(root, cases, metrics=[])), encoding="utf-8"
    )

    outcome = asyncio.run(
        EvaluationRunner(
            _config(
                root,
                profile="weekly",
                run_id="weekly-empty-baseline",
                baseline=baseline_path,
            ),
            SyntheticDriver(cases),
            root=root,
        ).run()
    )

    report = load_json(outcome.report_path)
    baseline_assertions = [
        assertion
        for result in report["case_results"]
        for assertion in result["assertion_results"]
        if next(item for item in cases if item["case_id"] == result["case_id"])["assertions"][
            int(assertion["assertion_id"][1:]) - 1
        ]["activation"]
        == "baseline_required"
    ]
    assert outcome.status == "PASS"
    assert baseline_assertions
    assert all(item["status"] == "NOT_APPLICABLE" for item in baseline_assertions)


def test_incompatible_baseline_is_rejected_before_run_directory_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _contract_copy(tmp_path, monkeypatch, fill_production=False)
    digest = "0" * 64
    baseline = {
        "schema_version": "1.0",
        "baseline_id": "wrong-baseline",
        "promoted_at": "2026-08-29T00:00:00Z",
        "promoted_by": "test",
        "source_run_uri": "artifact://baseline/report.json",
        "source_report_sha256": digest,
        "compatibility": {
            "dataset_sha256": digest,
            "profile": "daily",
            "selection_sha256": digest,
            "checklist_sha256": digest,
            "action_catalog_sha256": digest,
            "execution_profile_sha256": digest,
            "config_fingerprint": digest,
            "service_version": "wrong",
            "graph_backend": "wrong",
            "cache_state": "disabled",
            "concurrency": 1,
            "hardware_profile": "wrong",
            "models": {},
            "pipeline_versions": {},
            "ontology_version": "wrong",
        },
        "metrics": [],
        "threshold_policies": [],
    }
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    config = _config(
        root,
        profile="daily",
        run_id="daily-incompatible-baseline",
        baseline=baseline_path,
    )

    with pytest.raises(RunnerError, match="baseline is incompatible"):
        asyncio.run(EvaluationRunner(config, root=root).run())
    assert not (config.output_root / config.run_id).exists()
