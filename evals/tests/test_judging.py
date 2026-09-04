from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

import evals.judging.aggregate as aggregate_module
import evals.judging.finalize as finalize_module
from evals.judging.aggregate import _combine_status, aggregate
from evals.judging.finalize import finalize
from evals.validate import sha256_json

EVAL_ROOT = Path(__file__).resolve().parents[1]
_EXECUTION_REPORT_ERRORS = aggregate_module._execution_report_errors
_JUDGES = [
    ("grounding", "provider-a", "family-a"),
    ("task_utility", "provider-b", "family-b"),
    ("adversarial_safety", "provider-a", "family-c"),
    ("synthesis_uncertainty", "provider-b", "family-d"),
]


@pytest.fixture(autouse=True)
def _accept_minimal_unit_report(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(aggregate_module, "_execution_report_errors", lambda _: [])


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _panel_files(
    tmp_path: Path,
    *,
    scores: list[float] | None = None,
    critical_failure_at: set[int] | None = None,
) -> tuple[Path, Path, Path, list[Path]]:
    rubric = json.loads(
        (EVAL_ROOT / "judging" / "rubrics" / "daily-usefulness-v1.json").read_text(encoding="utf-8")
    )
    policy = json.loads((EVAL_ROOT / "judging" / "panel-policy.json").read_text(encoding="utf-8"))
    documents = [{"document_id": "doc-1", "content": "Evidence"}]
    packet = {
        "schema_version": "1.0",
        "packet_id": "run-1:REAL-001",
        "run": {
            "run_id": "run-1",
            "case_id": "REAL-001",
            "profile": "daily",
            "dataset_sha256": "0" * 64,
            "service_version": "test",
            "git_sha": "0123456789abcdef",
            "generated_at": "2026-08-29T00:00:00Z",
        },
        "scenario": {
            "title": "Open output",
            "objective": "Judge usefulness without a canonical answer.",
            "persona": "Synthetic user",
            "tags": ["real_world"],
            "answer_key_policy": "rubric_only_no_canonical_answer",
        },
        "task": {"prompt": "Synthesize the supplied evidence."},
        "source_material": {
            "synthetic_only": True,
            "documents": documents,
            "document_count": len(documents),
            "sha256": sha256_json(documents),
        },
        "candidate": {
            "alias": "candidate-A",
            "identity_blinded": True,
            "output": "Grounded candidate output.",
        },
        "system_evidence": {
            "boundary_observations": {},
            "tool_trace": [],
            "citations": [],
            "result_ids": [],
        },
        "hard_constraints": {
            "policy": "Panel quality cannot override deterministic failures.",
            "assertions": [],
        },
        "reference_catalog": [
            {"ref": "task", "kind": "task", "pointer": "/task"},
            {
                "ref": "candidate:output",
                "kind": "candidate",
                "pointer": "/candidate/output",
            },
            {
                "ref": "document:doc-1",
                "kind": "document",
                "pointer": "/source_material/documents/0",
            },
        ],
        "rubric": rubric,
        "panel_policy": policy,
        "judge_contract": {
            "instructions": "Judge independently.",
            "judgment_schema": "judging/schemas/judgment.schema.json",
            "packet_sha256_excluded": True,
        },
    }
    packet_path = tmp_path / "packet.json"
    _write_json(packet_path, packet)
    packet_sha256 = hashlib.sha256(packet_path.read_bytes()).hexdigest()

    assigned_judges = [
        {
            "actor_id": f"judge-{index + 1}",
            "role": role,
            "provider": provider,
            "model_family": family,
            "model_id": f"{family}-model",
            "version": "test",
        }
        for index, (role, provider, family) in enumerate(_JUDGES)
    ]
    assignment_path = tmp_path / "assignment.json"
    _write_json(
        assignment_path,
        {
            "schema_version": "1.0",
            "panel_id": "panel-1",
            "packet_id": packet["packet_id"],
            "packet_sha256": packet_sha256,
            "issued_by": "test-orchestrator",
            "created_at": "2026-08-29T00:01:00Z",
            "judges": assigned_judges,
        },
    )

    report_path = tmp_path / "report.json"
    _write_json(
        report_path,
        {
            "run_id": "run-1",
            "profile": "daily",
            "status": "PASS",
            "quality_status": "PENDING_JUDGMENT",
            "judge_packets": [
                {
                    "packet_id": packet["packet_id"],
                    "case_id": "REAL-001",
                    "ref": str(packet_path),
                    "sha256": packet_sha256,
                    "status": "PENDING_JUDGMENT",
                }
            ],
            "case_results": [{"case_id": "REAL-001", "status": "PASS"}],
        },
    )

    scores = scores or [0.9] * len(_JUDGES)
    critical_failure_at = critical_failure_at or set()
    judgment_paths: list[Path] = []
    for index, (assigned, score) in enumerate(zip(assigned_judges, scores, strict=True)):
        critical_failures = (
            [
                {
                    "rule_id": "material_fabrication",
                    "rationale": "The output invents a material claim.",
                    "evidence_refs": ["document:doc-1"],
                }
            ]
            if index in critical_failure_at
            else []
        )
        judgment = {
            "schema_version": "1.0",
            "packet_id": packet["packet_id"],
            "packet_sha256": packet_sha256,
            "judge": {**assigned, "independence_attested": True},
            "dimension_results": {
                dimension["id"]: {
                    "status": "SCORED",
                    "score": score,
                    "rationale": "The score follows the rubric anchors.",
                    "evidence_refs": ["document:doc-1"],
                }
                for dimension in rubric["dimensions"]
            },
            "critical_failures": critical_failures,
            "overall_score": score,
            "confidence": 0.9,
            "overall_rationale": "Independent rubric assessment.",
            "strengths": ["Grounded"],
            "weaknesses": [],
            "recommended_improvement": "Preserve source references.",
        }
        path = tmp_path / f"judgment-{index + 1}.json"
        _write_json(path, judgment)
        judgment_paths.append(path)
    return packet_path, report_path, assignment_path, judgment_paths


@pytest.mark.parametrize(
    ("hard_gate_status", "quality_status", "expected"),
    [
        ("PASS", "PASS", "PASS"),
        ("FAIL", "PASS", "FAIL"),
        ("PASS", "FAIL", "FAIL"),
        ("BLOCKED", "PASS", "BLOCKED"),
        ("PASS", "DISPUTED", "BLOCKED"),
    ],
)
def test_panel_quality_and_hard_gates_use_and_semantics(
    hard_gate_status: str, quality_status: str, expected: str
) -> None:
    assert _combine_status(hard_gate_status, quality_status) == expected


def test_panel_passes_with_trusted_diverse_assignment(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path)

    result, errors = aggregate(packet, report, judgments, assignment)

    assert errors == []
    assert result is not None
    assert result["quality_status"] == "PASS"
    assert result["status"] == "PASS"
    assert result["inputs"]["packet_sha256"] == hashlib.sha256(packet.read_bytes()).hexdigest()
    assert len(result["inputs"]["judgments"]) == 4


def test_panel_requires_declared_role_provider_and_model_diversity(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path)

    result, errors = aggregate(packet, report, judgments[:3], assignment)

    assert errors == []
    assert result is not None
    assert result["quality_status"] == "INSUFFICIENT_PANEL"
    assert result["status"] == "BLOCKED"


def test_panel_disagreement_requires_human_review(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path, scores=[0.9, 0.9, 0.9, 0.6])

    result, errors = aggregate(packet, report, judgments, assignment)

    assert errors == []
    assert result is not None
    assert result["quality_status"] == "DISPUTED"
    assert result["status"] == "BLOCKED"
    assert result["human_review_required"] is True


def test_single_critical_failure_allegation_is_disputed(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path, critical_failure_at={0})

    result, errors = aggregate(packet, report, judgments, assignment)

    assert errors == []
    assert result is not None
    assert result["quality_status"] == "DISPUTED"
    assert result["status"] == "BLOCKED"
    assert result["human_review_required"] is True


def test_confirmed_critical_failure_fails_quality_review(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path, critical_failure_at={0, 1})

    result, errors = aggregate(packet, report, judgments, assignment)

    assert errors == []
    assert result is not None
    assert result["quality_status"] == "FAIL"
    assert result["status"] == "FAIL"
    assert all(item["confirmed"] for item in result["critical_failures"])


def test_critical_failure_aggregation_is_input_order_independent(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path, critical_failure_at={0, 1})

    forward, forward_errors = aggregate(packet, report, judgments, assignment)
    reverse, reverse_errors = aggregate(packet, report, list(reversed(judgments)), assignment)

    assert forward_errors == reverse_errors == []
    assert forward == reverse


def test_always_applicable_dimension_cannot_be_omitted(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path)
    judgment = json.loads(judgments[0].read_text(encoding="utf-8"))
    judgment["dimension_results"]["grounding"].update({"status": "NOT_APPLICABLE", "score": None})
    _write_json(judgments[0], judgment)

    result, errors = aggregate(packet, report, judgments, assignment)

    assert result is None
    assert any("always-applicable dimension is N/A" in error for error in errors)


def test_judgment_must_bind_to_exact_packet_bytes(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path)
    judgment = json.loads(judgments[0].read_text(encoding="utf-8"))
    judgment["packet_sha256"] = "f" * 64
    _write_json(judgments[0], judgment)

    result, errors = aggregate(packet, report, judgments, assignment)

    assert result is None
    assert any("packet SHA-256 mismatch" in error for error in errors)


def test_judge_identity_must_match_trusted_assignment(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path)
    judgment = json.loads(judgments[0].read_text(encoding="utf-8"))
    judgment["judge"]["provider"] = "unassigned-provider"
    _write_json(judgments[0], judgment)

    result, errors = aggregate(packet, report, judgments, assignment)

    assert result is None
    assert any("differs from trusted assignment" in error for error in errors)


def test_judgment_evidence_refs_must_exist_in_packet(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path)
    judgment = json.loads(judgments[0].read_text(encoding="utf-8"))
    judgment["dimension_results"]["grounding"]["evidence_refs"] = ["document:missing"]
    _write_json(judgments[0], judgment)

    result, errors = aggregate(packet, report, judgments, assignment)

    assert result is None
    assert any("unknown evidence refs" in error for error in errors)


def test_schema_invalid_judgment_returns_errors_without_crashing(tmp_path: Path) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path)
    judgment = json.loads(judgments[0].read_text(encoding="utf-8"))
    judgment["judge"] = []
    _write_json(judgments[0], judgment)

    result, errors = aggregate(packet, report, judgments, assignment)

    assert result is None
    assert any("schema" in error for error in errors)


def _finalization_files(
    tmp_path: Path, *, profile: str = "weekly", git_dirty: bool = False
) -> tuple[Path, list[Path], frozenset[str]]:
    packet, report, assignment, judgments = _panel_files(tmp_path)
    report_value = json.loads(report.read_text(encoding="utf-8"))
    report_value["profile"] = profile
    report_value["manifest"] = {"git_dirty": git_dirty}
    _write_json(report, report_value)
    panel_result, errors = aggregate(packet, report, judgments, assignment)
    assert errors == []
    assert panel_result is not None
    result_path = tmp_path / "panel-result.json"
    _write_json(result_path, panel_result)
    trusted = frozenset({hashlib.sha256(result_path.read_bytes()).hexdigest()})
    return report, [result_path], trusted


def test_final_quality_gate_binds_every_panel_result(tmp_path: Path) -> None:
    report, panel_results, trusted = _finalization_files(tmp_path)

    result, errors = finalize(report, panel_results, trusted_panel_result_sha256=trusted)

    assert errors == []
    assert result is not None
    assert result["status"] == "PASS"
    assert result["quality_status"] == "PASS"
    assert len(result["panel_results"]) == 1
    assert result["execution_report"]["sha256"] == hashlib.sha256(report.read_bytes()).hexdigest()


def test_final_quality_gate_requires_every_packet(tmp_path: Path) -> None:
    report, _panel_results, trusted = _finalization_files(tmp_path)

    result, errors = finalize(report, [], trusted_panel_result_sha256=trusted)

    assert result is None
    assert any("missing panel results" in error for error in errors)


def test_final_quality_gate_rejects_report_hash_mismatch(tmp_path: Path) -> None:
    report, panel_results, _trusted = _finalization_files(tmp_path)
    panel_result = json.loads(panel_results[0].read_text(encoding="utf-8"))
    panel_result["inputs"]["report_sha256"] = "f" * 64
    _write_json(panel_results[0], panel_result)
    trusted = frozenset({hashlib.sha256(panel_results[0].read_bytes()).hexdigest()})

    result, errors = finalize(report, panel_results, trusted_panel_result_sha256=trusted)

    assert result is None
    assert any("execution report SHA-256 mismatch" in error for error in errors)


def test_final_quality_gate_reaggregates_panel_inputs(tmp_path: Path) -> None:
    report, panel_results, _trusted = _finalization_files(tmp_path)
    panel_result = json.loads(panel_results[0].read_text(encoding="utf-8"))
    panel_result["overall_score"] = 0.99
    _write_json(panel_results[0], panel_result)
    trusted = frozenset({hashlib.sha256(panel_results[0].read_bytes()).hexdigest()})

    result, errors = finalize(report, panel_results, trusted_panel_result_sha256=trusted)

    assert result is None
    assert any("differs from deterministic aggregation" in error for error in errors)


def test_final_quality_gate_requires_external_result_trust(tmp_path: Path) -> None:
    report, panel_results, _trusted = _finalization_files(tmp_path)

    result, errors = finalize(report, panel_results, trusted_panel_result_sha256=frozenset())

    assert result is None
    assert any("panel result SHA-256 is not externally trusted" in error for error in errors)


def test_release_finalization_requires_clean_source(tmp_path: Path) -> None:
    report, panel_results, trusted = _finalization_files(
        tmp_path, profile="release", git_dirty=True
    )

    result, errors = finalize(report, panel_results, trusted_panel_result_sha256=trusted)

    assert result is None
    assert any("dirty source tree" in error for error in errors)


def test_finalization_rejects_inputs_changed_during_aggregation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, panel_results, trusted = _finalization_files(tmp_path)
    original_aggregate = aggregate_module.aggregate

    def mutating_aggregate(
        *args: object, **kwargs: object
    ) -> tuple[dict[str, Any] | None, list[str]]:
        result = original_aggregate(*args, **kwargs)  # type: ignore[arg-type]
        panel_results[0].write_text("{}\n", encoding="utf-8")
        return result

    monkeypatch.setattr(aggregate_module, "aggregate", mutating_aggregate)

    result, errors = finalize(report, panel_results, trusted_panel_result_sha256=trusted)

    assert result is None
    assert any("inputs changed" in error for error in errors)


def test_finalizer_refuses_to_overwrite_quality_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, panel_results, trusted = _finalization_files(tmp_path)
    output = tmp_path / "quality-gate.json"
    output.write_text("immutable\n", encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "finalize",
            str(report),
            str(panel_results[0]),
            "--trusted-panel-result-sha256",
            next(iter(trusted)),
            "--output",
            str(output),
        ],
    )

    assert finalize_module.main() == 1
    assert output.read_text(encoding="utf-8") == "immutable\n"


def test_execution_report_validation_is_mandatory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packet, report, assignment, judgments = _panel_files(tmp_path)
    monkeypatch.setattr(
        aggregate_module,
        "_execution_report_errors",
        lambda _: ["cleanup status contradicts PASS"],
    )

    result, errors = aggregate(packet, report, judgments, assignment)

    assert result is None
    assert any("cleanup status contradicts PASS" in error for error in errors)


def test_slice_report_validation_uses_its_known_profile_subset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report_path = tmp_path / "report.json"
    _write_json(
        report_path,
        {
            "run_id": "run-1-weekly-real001-slice",
            "profile": "weekly",
            "selection": {"check_ids": ["CHECK-001"], "case_ids": ["REAL-001"]},
        },
    )
    monkeypatch.setattr(
        aggregate_module,
        "load_json",
        lambda path: (
            {"items": [{"id": "CHECK-001", "profiles": ["weekly"]}]}
            if path.name == "checklist.json"
            else json.loads(path.read_text(encoding="utf-8"))
        ),
    )
    monkeypatch.setattr(
        aggregate_module,
        "load_cases",
        lambda: [{"case_id": "REAL-001", "profiles": ["weekly"]}],
    )
    validated: list[tuple[list[dict[str, Any]], list[dict[str, Any]]]] = []

    def validate(
        _path: Path, checks: list[dict[str, Any]], cases: list[dict[str, Any]]
    ) -> list[str]:
        validated.append((checks, cases))
        return []

    monkeypatch.setattr(aggregate_module, "validate_report", validate)

    assert _EXECUTION_REPORT_ERRORS(report_path) == []
    assert validated == [
        (
            [{"id": "CHECK-001", "profiles": ["weekly"]}],
            [{"case_id": "REAL-001", "profiles": ["weekly"]}],
        )
    ]


def test_slice_report_rejects_unknown_contracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report_path = tmp_path / "report.json"
    _write_json(
        report_path,
        {
            "run_id": "run-1-weekly-real001-slice",
            "profile": "weekly",
            "selection": {"check_ids": ["UNKNOWN-001"], "case_ids": ["REAL-001"]},
        },
    )
    monkeypatch.setattr(
        aggregate_module,
        "load_json",
        lambda path: (
            {"items": [{"id": "CHECK-001", "profiles": ["weekly"]}]}
            if path.name == "checklist.json"
            else json.loads(path.read_text(encoding="utf-8"))
        ),
    )
    monkeypatch.setattr(
        aggregate_module,
        "load_cases",
        lambda: [{"case_id": "REAL-001", "profiles": ["weekly"]}],
    )

    errors = _EXECUTION_REPORT_ERRORS(report_path)

    assert errors == ["slice report selects unknown contracts: checks=['UNKNOWN-001'], cases=[]"]
