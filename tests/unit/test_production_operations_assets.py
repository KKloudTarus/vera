from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from textwrap import dedent
from typing import Any
from urllib.parse import urlsplit

import pytest
import yaml

ROOT = Path(__file__).parents[2]
ALERTS_PATH = ROOT / "deploy" / "observability" / "v1" / "prometheus-alerts.yaml"
DASHBOARD_PATH = ROOT / "deploy" / "observability" / "v1" / "dashboard.json"
PRODUCTION_CONTRACT_PATH = ROOT / "evals" / "fixtures" / "production.json"
PROMETHEUS_PATH = ROOT / "deploy" / "observability" / "v1" / "prometheus.yml"
RECOVERY_HARNESS_PATH = ROOT / "deploy" / "recovery" / "postgres-harness.sh"
RELEASE_WORKFLOW_PATH = ROOT / ".github" / "workflows" / "release.yml"
RELEASE_CANDIDATE_WORKFLOW_PATH = ROOT / ".github" / "workflows" / "release-candidate.yml"
RUNTIME_PROVISION_PATH = ROOT / "deploy" / "postgres" / "provision-runtime.sh"
EVAL_COMPOSE_PATH = ROOT / "evals" / "docker-compose.eval.yml"
K8S_BASE_PATH = ROOT / "deploy" / "k8s" / "base.yaml"
K8S_CALIBRATE_PATH = ROOT / "deploy" / "k8s" / "calibrate-cronjob.yaml"
K8S_MCP_PATH = ROOT / "deploy" / "k8s" / "mcp.yaml"
K8S_WORKER_PATH = ROOT / "deploy" / "k8s" / "worker.yaml"
HELM_TEMPLATE_ROOT = ROOT / "deploy" / "helm" / "vera" / "templates"
USAGE_MIGRATION_PATH = (
    ROOT / "migrations" / "versions" / "d4e5f6a7b8c9_llm_usage_cost_completeness.py"
)
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
    assert "backup|restore|cleanup|purge" in script
    assert "purge_all || return" in script
    assert 'for running in "$root"/*/running' in script
    assert "ALTER DATABASE vera WITH ALLOW_CONNECTIONS false" in script
    assert "ALTER DATABASE vera WITH ALLOW_CONNECTIONS true" in script
    assert 'staging_marker="$root/$token/staging.sha256"' in script
    assert "trap 'restore_access >/dev/null 2>&1 || true; exit 1' HUP INT TERM" in script
    assert "restore cutover failed and database access could not be restored" in script
    failed_first_cutover = script.split(
        '"ALTER DATABASE vera RENAME TO $previous"; then', maxsplit=1
    )[1].split("fi", maxsplit=1)[0]
    assert "fail_cutover" in failed_first_cutover


@pytest.mark.parametrize(
    ("failure", "failed_command"),
    [
        ("disable", "ALTER DATABASE vera WITH ALLOW_CONNECTIONS false"),
        ("rename", "ALTER DATABASE vera RENAME TO vera_restore_previous_abc"),
    ],
)
def test_recovery_harness_fails_closed_on_ambiguous_cutover_errors(
    tmp_path: Path, failure: str, failed_command: str
) -> None:
    root = tmp_path / "recovery"
    dump = root / "abc" / "vera.dump"
    dump.parent.mkdir(parents=True)
    dump.write_bytes(b"backup")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    psql_log = tmp_path / "psql.log"
    psql = bin_dir / "psql"
    psql.write_text(
        dedent(
            """\
            #!/bin/sh
            printf '%s\n' "$*" >> "$PSQL_LOG"
            case "$FAILURE:$*" in
              disable:*"ALTER DATABASE vera WITH ALLOW_CONNECTIONS false"*) exit 23 ;;
              rename:*"ALTER DATABASE vera RENAME TO vera_restore_previous_"*) exit 23 ;;
              *"SELECT count(*)"*"datname='vera'"*) printf '1\n' ;;
              *"SELECT count(*)"*) printf '0\n' ;;
              *) exit 0 ;;
            esac
            """
        ),
        encoding="utf-8",
    )
    psql.chmod(0o755)
    for command in ("dropdb", "createdb", "pg_restore"):
        executable = bin_dir / command
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o755)

    completed = subprocess.run(  # noqa: S603
        ["/bin/sh", str(RECOVERY_HARNESS_PATH), "run", "restore", "abc"],
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "PSQL_LOG": str(psql_log),
            "FAILURE": failure,
            "VERA_RECOVERY_ROOT": str(root),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    commands = psql_log.read_text(encoding="utf-8")
    assert failed_command in commands
    assert "ALTER DATABASE vera WITH ALLOW_CONNECTIONS true" in commands


def test_release_promotes_commit_bound_evaluated_digest_before_registry_mutation() -> None:
    workflow = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    candidate = RELEASE_CANDIDATE_WORKFLOW_PATH.read_text(encoding="utf-8")
    verification = 'evidence_ref="refs/tags/release-evidence-${GITHUB_SHA}"'

    assert "environment: release-evidence" in workflow
    assert "secrets.VERA_TRUSTED_PANEL_RESULT_SHA256" in workflow
    assert verification in workflow
    assert 'image_digest="$(python -m evals.verify_release_gate' in workflow
    assert workflow.index(verification) < workflow.index("docker/login-action")
    assert "docker/build-push-action" not in workflow
    assert "docker buildx imagetools create" in workflow
    assert '"${repository}@${digest}"' in workflow
    assert "docker/build-push-action@10e90e3645eae34f1e60eeb005ba3a3d33f178e8" in candidate
    assert "candidate-${{ steps.source.outputs.sha }}" in candidate
    assert "VERA_BUILD_GIT_SHA=${{ steps.source.outputs.sha }}" in candidate
    assert not re.search(r"(?:actions|docker)/[^@\s]+@v\d", workflow + candidate)
    release_stack = (ROOT / "evals" / "release_stack.sh").read_text(encoding="utf-8")
    assert "release stack rejects Docker Compose global options" in release_stack


def test_release_stack_rejects_compose_global_overrides_before_docker(
    tmp_path: Path,
) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    git = bin_dir / "git"
    git.write_text(
        dedent(
            """\
            #!/bin/sh
            case "$*" in
              *"rev-parse --show-toplevel"*) printf '%s\n' "$REPO_ROOT" ;;
              *"status --porcelain"*) exit 0 ;;
              *"rev-parse --verify HEAD"*) printf '%040d\n' 1 ;;
              *) exit 1 ;;
            esac
            """
        ),
        encoding="utf-8",
    )
    git.chmod(0o755)
    docker_log = tmp_path / "docker.log"
    docker = bin_dir / "docker"
    docker.write_text(
        '#!/bin/sh\nprintf \'%s\\n\' "$*" >> "$DOCKER_LOG"\n',
        encoding="utf-8",
    )
    docker.chmod(0o755)
    override = tmp_path / "override.yml"

    completed = subprocess.run(  # noqa: S603
        ["/bin/sh", str(ROOT / "evals" / "release_stack.sh"), "-f", str(override), "up"],
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "REPO_ROOT": str(ROOT),
            "DOCKER_LOG": str(docker_log),
            "COMPOSE_PROJECT_NAME": "vera-release-test",
            "VERA_EVAL_SCOPE_ID": "release-test",
            "VERA_RELEASE_APP_IMAGE": "ghcr.io/kkloudtarus/vera@sha256:" + "a" * 64,
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2
    assert "rejects Docker Compose global options" in completed.stderr
    assert not docker_log.exists()


def test_production_application_images_are_immutable_and_migrations_are_revisioned() -> None:
    digest = "sha256:" + "0" * 64
    for path in (
        K8S_BASE_PATH,
        K8S_CALIBRATE_PATH,
        K8S_MCP_PATH,
        K8S_WORKER_PATH,
        ROOT / "deploy/k8s/api.yaml",
    ):
        manifest = path.read_text(encoding="utf-8")
        app_images = re.findall(r"image:\s+(ghcr\.io/kkloudtarus/vera\S+)", manifest)
        assert app_images
        assert all(image == f"ghcr.io/kkloudtarus/vera@{digest}" for image in app_images)

    base = K8S_BASE_PATH.read_text(encoding="utf-8")
    helm_migration = (HELM_TEMPLATE_ROOT / "migrate-job.yaml").read_text(encoding="utf-8")
    helm_helpers = (HELM_TEMPLATE_ROOT / "_helpers.tpl").read_text(encoding="utf-8")
    assert "name: vera-migrate-d4e5f6a7b8c9" in base
    assert "activeDeadlineSeconds: 600" in base
    assert "backoffLimit: 3" in base
    assert "-migrate-{{ .Release.Revision }}" in helm_migration
    assert 'include "vera.image"' in helm_migration
    assert "^sha256:[a-f0-9]{64}$" in helm_helpers
    assert "image.digest must replace the fail-closed placeholder" in helm_helpers


def test_all_external_workflow_actions_are_pinned_to_commits() -> None:
    for workflow_path in (ROOT / ".github" / "workflows").glob("*.yml"):
        workflow = workflow_path.read_text(encoding="utf-8")
        for action, revision in re.findall(r"uses:\s+([^@\s]+)@([^\s#]+)", workflow):
            if action.startswith("./"):
                continue
            assert re.fullmatch(r"[a-f0-9]{40}", revision), (
                f"{workflow_path.name} uses mutable action revision {action}@{revision}"
            )


def test_usage_migration_hardens_roles_and_validates_constraints_online() -> None:
    migration = USAGE_MIGRATION_PATH.read_text(encoding="utf-8")

    assert '"vera_app": "NOBYPASSRLS"' in migration
    assert '"vera_trusted": "BYPASSRLS"' in migration
    assert '"vera_worker": "BYPASSRLS"' in migration
    assert "owns database objects" in migration
    assert "CHECK ({expression}) NOT VALID" in migration
    assert "VALIDATE CONSTRAINT {name}" in migration
    assert "ADD COLUMN IF NOT EXISTS" in migration
    assert "provider_retry_fenced" in migration
    assert "trg_enforce_ingestion_claim_fence" in migration
    assert "LOCK TABLE ingestion_jobs IN ACCESS EXCLUSIVE MODE" in migration
    assert "REVOKE UPDATE, DELETE, TRUNCATE ON llm_usage" in migration


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
    assert "VERA_MCP__TOOL_PROFILE: advanced" in compose
    assert "GRANT vera_app TO vera_legacy;" in provision


def test_application_login_cannot_assume_the_worker_role() -> None:
    compose = EVAL_COMPOSE_PATH.read_text(encoding="utf-8")
    provision = RUNTIME_PROVISION_PATH.read_text(encoding="utf-8")

    assert "WHERE granted.rolname = 'vera_runtime'" in provision
    assert "WHERE granted.rolname = 'vera_worker_runtime'" in provision
    assert "GRANT vera_app, vera_trusted TO vera_runtime;" in provision
    assert "GRANT vera_app, vera_trusted, vera_worker TO vera_worker_runtime;" in provision
    assert "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vera_runtime;" in provision
    assert "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vera_worker_runtime;" in provision
    assert "owns database objects" in provision
    assert "VERA_EVAL_WORKER_DSN: postgresql+asyncpg://vera_worker_runtime:" in compose
    assert "vera_scaler_runtime" in provision
    assert "vera_scaler_runtime owns database objects" in provision
    assert "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vera_scaler_runtime;" in provision
    assert "GRANT SELECT ON ingestion_jobs TO vera_scaler_runtime;" in provision
    assert "has_table_privilege('vera_app', relation.oid, candidate.privilege)" in provision
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES" not in provision
    evaluator = compose.split("  evaluator:", maxsplit=1)[1]
    assert "VERA_DB__DSN: postgresql+asyncpg://vera_worker_runtime:" in evaluator
    assert "VERA_EVAL_ADMIN_DSN" not in evaluator
    assert 'VERA_DB__ROLE_ENFORCEMENT: "true"' in evaluator


def test_production_deployments_isolate_database_credentials() -> None:
    base_documents = list(yaml.safe_load_all(K8S_BASE_PATH.read_text(encoding="utf-8")))
    secrets = {
        document["metadata"]["name"]: document["stringData"]
        for document in base_documents
        if document["kind"] == "Secret"
    }
    assert secrets["vera-secrets"]["VERA_DB__DSN"].startswith("postgresql+asyncpg://vera_runtime:")
    assert secrets["vera-worker-database"]["VERA_DB__DSN"].startswith(
        "postgresql+asyncpg://vera_worker_runtime:"
    )
    assert secrets["vera-admin-database"]["VERA_DB__DSN"].startswith("postgresql+asyncpg://vera:")
    assert secrets["vera-scaler-database"]["VERA_KEDA_DB_DSN"].startswith(
        "postgresql://vera_scaler_runtime:"
    )
    assert "VERA_MCP__JWT_SECRET" not in secrets["vera-secrets"]
    assert "VERA_MCP__JWT_SECRET" in secrets["vera-mcp-secret"]
    migration = next(document for document in base_documents if document["kind"] == "Job")
    migration_secret_refs = migration["spec"]["template"]["spec"]["containers"][0]["envFrom"]
    assert migration_secret_refs == [
        {"configMapRef": {"name": "vera-config"}},
        {"secretRef": {"name": "vera-admin-database"}},
    ]

    worker = next(
        document
        for document in yaml.safe_load_all(K8S_WORKER_PATH.read_text(encoding="utf-8"))
        if document["kind"] == "Deployment"
    )
    worker_env = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    assert worker_env[0]["valueFrom"]["secretKeyRef"]["name"] == "vera-worker-database"
    assert worker_env[1]["valueFrom"]["secretKeyRef"]["name"] == "vera-scaler-database"

    api_template = (HELM_TEMPLATE_ROOT / "api.yaml").read_text(encoding="utf-8")
    bootstrap_template = (HELM_TEMPLATE_ROOT / "bootstrap-job.yaml").read_text(encoding="utf-8")
    graph_template = (HELM_TEMPLATE_ROOT / "graph.yaml").read_text(encoding="utf-8")
    config_template = (HELM_TEMPLATE_ROOT / "config.yaml").read_text(encoding="utf-8")
    helpers_template = (HELM_TEMPLATE_ROOT / "_helpers.tpl").read_text(encoding="utf-8")
    mcp_template = (HELM_TEMPLATE_ROOT / "mcp.yaml").read_text(encoding="utf-8")
    worker_template = (HELM_TEMPLATE_ROOT / "worker.yaml").read_text(encoding="utf-8")
    migrate_template = (HELM_TEMPLATE_ROOT / "migrate-job.yaml").read_text(encoding="utf-8")
    minio_template = (HELM_TEMPLATE_ROOT / "minio.yaml").read_text(encoding="utf-8")
    provision_template = (HELM_TEMPLATE_ROOT / "database-provision-job.yaml").read_text(
        encoding="utf-8"
    )
    assert 'include "vera.runtimeDatabaseEnv"' in api_template
    assert 'include "vera.runtimeDatabaseEnv"' in mcp_template
    assert 'include "vera.workerDatabaseEnv"' in worker_template
    assert 'include "vera.waitForRuntimeDatabase"' in api_template
    assert 'include "vera.waitForRuntimeDatabase"' in mcp_template
    assert 'include "vera.waitForRuntimeDatabase"' in worker_template
    assert ".Values.postgres.runtimeUser" not in helpers_template
    assert ".Values.postgres.workerUser" not in helpers_template
    assert 'include "vera.mcpSecretEnv"' not in api_template
    assert 'include "vera.mcpSecretEnv"' not in worker_template
    assert 'include "vera.mcpSecretEnv"' in mcp_template
    assert 'include "vera.bootstrapSecretEnv"' in bootstrap_template
    assert 'include "vera.bootstrapSecretEnv"' not in worker_template
    assert "- secretRef:" not in helpers_template
    assert "key: NEO4J_AUTH" in graph_template
    assert 'include "vera.adminDatabaseEnv"' in migrate_template
    assert 'value: {{ include "vera.adminDsn" . | quote }}' not in migrate_template
    assert "WHERE granted.rolname = 'vera_runtime'" in provision_template
    assert "WHERE granted.rolname = 'vera_worker_runtime'" in provision_template
    assert '"helm.sh/hook"' not in provision_template
    assert "ttlSecondsAfterFinished: 300" in provision_template
    assert "activeDeadlineSeconds: 600" in provision_template
    assert "value: {{ .Values.postgres.password | quote }}" not in provision_template
    assert "to_regrole('vera_app') IS NOT NULL" in provision_template
    assert "WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vera_runtime')" in (
        provision_template
    )
    assert "PASSWORD :'runtime_password'" in provision_template
    assert "value: {{ .Values.postgres.runtimePassword | quote }}" not in provision_template
    assert "value: {{ .Values.postgres.workerPassword | quote }}" not in provision_template
    assert 'replace "+" "%20"' in helpers_template
    assert 'name: {{ include "vera.fullname" . }}-objectstore-admin' in minio_template
    assert '"helm.sh/hook": post-install,pre-upgrade' in minio_template
    assert "activeDeadlineSeconds: 600" in minio_template
    assert "value: {{ .Values.minio.rootPassword | quote }}" not in minio_template
    assert 'mc admin user info local "$APP_ACCESS_KEY"' in minio_template
    assert "mc admin policy attach local vera-bucket" in minio_template
    assert "chart-managed PostgreSQL runtime credentials are immutable" in config_template
    assert "chart-managed MinIO application credentials are immutable" in config_template
    assert 'define "vera.schemaRevision" -}}d4e5f6a7b8c9' in helpers_template
    assert "vera.provisioned_revision" in helpers_template
    assert "vera.provisioned_revision" in provision_template
    embedded_python = (
        "      import asyncio"
        + helpers_template.split("      import asyncio", maxsplit=1)[1].split(
            "      asyncio.run(main())", maxsplit=1
        )[0]
    )
    compile(dedent(embedded_python), "wait-for-runtime-database", "exec")

    k8s_mcp = K8S_MCP_PATH.read_text(encoding="utf-8")
    assert "name: vera-mcp-secret" in k8s_mcp
