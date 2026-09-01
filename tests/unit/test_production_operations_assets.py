from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

ROOT = Path(__file__).parents[2]
ALERTS_PATH = ROOT / "deploy" / "observability" / "v1" / "prometheus-alerts.yaml"
DASHBOARD_PATH = ROOT / "deploy" / "observability" / "v1" / "dashboard.json"
PRODUCTION_CONTRACT_PATH = ROOT / "evals" / "fixtures" / "production.json"
PROMETHEUS_PATH = ROOT / "deploy" / "observability" / "v1" / "prometheus.yml"
RECOVERY_HARNESS_PATH = ROOT / "deploy" / "recovery" / "postgres-harness.sh"
RUNTIME_PROVISION_PATH = ROOT / "deploy" / "postgres" / "provision-runtime.sh"
EVAL_COMPOSE_PATH = ROOT / "evals" / "docker-compose.eval.yml"
RUNBOOK_PATH = ROOT / "docs" / "runbooks.md"
DR_RUNBOOK_PATH = ROOT / "docs" / "dr-runbook.md"

EXPRESSIONS = {
    "write_failure": "increase(vera_write_failures_total[5m]) > 0",
    "projection_drift": "max(vera_projection_drift_items) > 0",
    "queue_lag": "max(vera_queue_lag_seconds) > 300",
    "freshness": (
        "histogram_quantile(0.95, sum by (le) "
        "(rate(vera_time_to_searchable_seconds_bucket[5m]))) > 900"
    ),
    "extraction_failure": "increase(vera_extraction_failures_total[5m]) > 0",
    "retrieval_latency": (
        "histogram_quantile(0.95, sum by (le) (rate(vera_search_duration_seconds_bucket[5m]))) > 2"
    ),
}
OWNERS = {"application-owner", "operations"}


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _section(markdown: str, heading: str) -> str:
    marker = f"### {heading}\n"
    start = markdown.index(marker) + len(marker)
    end = markdown.find("\n### ", start)
    return markdown[start:] if end == -1 else markdown[start:end]


def test_alerts_and_dashboard_cover_the_observability_contract() -> None:
    contract = _load_json(PRODUCTION_CONTRACT_PATH)
    expected_signals = set(contract["observability_matrix"]["signals"])
    assert expected_signals == set(EXPRESSIONS)

    alerts = yaml.safe_load(ALERTS_PATH.read_text(encoding="utf-8"))
    assert [group["name"] for group in alerts["groups"]] == ["vera-production-signals-v1"]
    rules = alerts["groups"][0]["rules"]
    alerts_by_signal = {rule["labels"]["signal_id"]: rule for rule in rules}
    assert len(rules) == len(alerts_by_signal) == len(expected_signals)
    assert set(alerts_by_signal) == expected_signals

    dashboard = _load_json(DASHBOARD_PATH)
    assert dashboard["uid"] == "vera-production-signals-v1"
    assert dashboard["version"] == 1
    panels = dashboard["panels"]
    panels_by_signal = {panel["signal_id"]: panel for panel in panels}
    assert len(panels) == len(panels_by_signal) == len(expected_signals)
    assert set(panels_by_signal) == expected_signals

    runbook = RUNBOOK_PATH.read_text(encoding="utf-8")
    for signal in expected_signals:
        rule = alerts_by_signal[signal]
        assert rule["alert"].startswith("Vera")
        assert rule["labels"]["contract"] == "OPS-007"
        assert rule["labels"]["owner"] in OWNERS
        assert rule["labels"]["severity"] == "critical"
        assert rule["for"] == "0s"
        assert rule["expr"].strip() == EXPRESSIONS[signal]

        runbook_url = str(rule["annotations"]["runbook_url"])
        assert urlsplit(runbook_url).path.endswith("/docs/runbooks.md")
        assert urlsplit(runbook_url).fragment == signal
        assert f"### {signal}\n" in runbook
        assert "**Owner:**" in _section(runbook, signal)

        panel = panels_by_signal[signal]
        assert panel["type"] == "timeseries"
        assert len(panel["targets"]) == 1
        assert panel["targets"][0]["expr"] == rule["expr"].strip()
        assert panel["targets"][0]["datasource"]["uid"] == "${DS_PROMETHEUS}"
        assert panel["links"][0]["url"] == runbook_url

    prometheus = yaml.safe_load(PROMETHEUS_PATH.read_text(encoding="utf-8"))
    assert prometheus["global"]["scrape_interval"] == "1s"
    assert prometheus["global"]["evaluation_interval"] == "1s"
    assert prometheus["scrape_configs"] == [
        {
            "job_name": "vera-api",
            "static_configs": [{"targets": ["api:8000"]}],
        },
        {
            "job_name": "vera-worker",
            "static_configs": [{"targets": ["worker:9100"]}],
        },
        {
            "job_name": "vera-eval-product-exercises",
            "static_configs": [{"targets": ["evaluator:9200"]}],
        },
    ]


def test_incident_and_transition_runbooks_cover_the_contract() -> None:
    contract = _load_json(PRODUCTION_CONTRACT_PATH)
    runbook = RUNBOOK_PATH.read_text(encoding="utf-8")
    dr_runbook = DR_RUNBOOK_PATH.read_text(encoding="utf-8")

    for role in contract["incident_matrix"]["required_roles"]:
        assert role.replace("_", " ") in dr_runbook.lower()

    for incident in contract["incident_matrix"]["incidents"]:
        section = _section(dr_runbook, incident)
        assert "**Owner:**" in section

    for transition in contract["rollout_matrix"]["transitions"]:
        section = _section(runbook, transition)
        assert "**Owner:**" in section
        assert "**Rollout:**" in section
        assert "**Rollback:**" in section


def test_recovery_harness_exposes_the_shared_request_directory() -> None:
    script = RECOVERY_HARNESS_PATH.read_text(encoding="utf-8")
    assert 'chmod 1777 "$root"' in script


def test_rollout_control_plane_isolated_from_application_children() -> None:
    compose = EVAL_COMPOSE_PATH.read_text(encoding="utf-8")
    provision = RUNTIME_PROVISION_PATH.read_text(encoding="utf-8")
    assert "chmod 1777 /rollout" not in compose
    assert "rollout-desired:/rollout/desired:ro" in compose
    assert "rollout-status:/rollout/status:ro" in compose
    assert 'user: "10001:10001"' in compose
    assert "VERA_EVAL_ROLLOUT_CONTROLLER_TOKEN" in compose
    assert "postgresql+asyncpg://vera_legacy:" in compose
    assert "VERA_MEMORY__FABRIC_ENABLED" in compose
    assert "GRANT vera_app TO vera_legacy;" in provision
