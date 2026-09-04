# VERA Evaluation Framework

`evals/` tests the complete VERA knowledge path, from source data to the final agent output. The
framework separates product correctness, retrieval quality, candidate-agent behavior, and
production readiness.

## Evaluation Scope

The evaluated path is:

```text
source -> ingestion -> artifact/chunk -> assertion/evidence -> fact lifecycle
       -> graph/search projection -> retrieval -> agent output -> user outcome
```

The main evaluation areas are:

- Ingestion, idempotency, versioning, cursor recovery, and deletion.
- Claim extraction, curation, trust policy, evidence, and exact provenance.
- Valid time, transaction time, supersession, conflict, and retraction.
- Projection parity, retrieval quality, no-answer behavior, and tenant isolation.
- Agent tool use, grounding, citation, abstention, and task completion.
- Latency, throughput, cost, resilience, backup, restore, and observability.
- Authentication, authorization, RLS, poisoning, and prompt injection.

## Contracts

| Component | Purpose |
|---|---|
| `checklist.json` | Claims that each run must prove |
| `scenarios/*.jsonl` | Stimuli, actions, assertions, and expected behavior |
| `fixtures/*.json` | Synthetic datasets and production-readiness targets |
| `action_catalog.json` | Typed action allowlist and capability ownership |
| `schemas/` | Case, checklist, report, and baseline contracts |
| `judging/` | Rubrics, roles, panel policy, aggregation, and finalization |

Validate contracts with:

```bash
python -m evals.validate
python -m evals.validate evals/runs/<run-id>/report.json
```

## Execution Profiles

| Profile | Purpose |
|---|---|
| `daily` | Correctness smoke tests and real-world workflows |
| `nightly` | Full ingestion, temporal, retrieval, and resilience regression |
| `weekly` | Load, latency, throughput, cost, and independent quality review |
| `release` | Production-readiness hard gates and operational drills |

Invoke the runner with:

```bash
python -m evals.runner evals/run.local.json \
  --capabilities <comma-separated-actions> \
  --adapter-command python -m evals.vera_local_adapter
```

`--adapter-command` must be the final option because every following argument is forwarded to the
adapter subprocess.
When `--adapter-timeout` is omitted, the runner sets it above the largest action deadline in the
selected profile. An explicit adapter timeout must also exceed that deadline.

Start the disposable full-stack evaluator with:

```bash
export VERA_EVALUATOR_USER="$(id -u):$(id -g)"
docker compose \
  -f docker-compose.yml \
  -f evals/docker-compose.eval.yml \
  --profile app \
  --profile eval \
  up -d --build
```

`evals/docker-compose.eval.yml` is an override for the root topology, not a standalone Compose
file. The evaluator image is defined in `evals/Dockerfile.eval`.

For a release run, set a unique Compose project and scope, then use the release wrapper:

```bash
export COMPOSE_PROJECT_NAME=vera-release-<unique-id>
export VERA_EVAL_SCOPE_ID=release-<unique-id>
# Copy the immutable reference emitted by the Build release candidate workflow.
export VERA_RELEASE_APP_IMAGE=ghcr.io/kkloudtarus/vera@sha256:<candidate-digest>
# Optional; defaults to the checkout's ignored .env file.
export VERA_RELEASE_ENV_FILE=/absolute/path/to/release.env
./evals/release_stack.sh build
./evals/release_stack.sh up -d
./evals/release_stack.sh exec -T evaluator \
  python -m evals.prepare_release \
  evals/run.release.local.json \
  /output/release-configs/${VERA_EVAL_SCOPE_ID}.json
```

`release_stack.sh` refuses a dirty Git worktree, verifies that the candidate image was built from
the exact clean `HEAD`, and runs every application process from that digest. It builds only the
evaluation support images from a Git archive. Later Compose commands read their definitions from
a fresh archive and cannot rebuild from the checkout.
The wrapper passes the external runtime env file into the archived Compose project, force-recreates
the complete stack, and records the verified image IDs for every release service.
`prepare_release` reads the immutable evaluator image metadata, rejects dirty metadata, and writes
a new run config with the matching revision, application image digest, and configured scope. Run
`evals.runner` with the generated config path. Generated configs are local artifacts under
`evals/runs/` and cannot overwrite an existing file.

The release env file must provide distinct `VERA_EVAL_DB_RUNTIME_PASSWORD`,
`VERA_EVAL_DB_WORKER_PASSWORD`, and `VERA_EVAL_DB_LEGACY_PASSWORD` values. Provide the matching
`*_URLENCODED` value when a password contains characters that must be escaped inside a DSN.

## Safety And Cleanup

Every mutating run requires:

- A dedicated synthetic scope or run-owned ephemeral stack.
- `production_writable=false`.
- Positive duration, total-cost, and per-action cost-reservation budgets.
- Adapter support for `safety.preflight` and `cleanup.run_scope`.
- A complete resource ledger for every created and removed object.

The runner does not start mutation when preflight is missing or invalid. Cleanup always runs in a
`finally` block. Residual resources fail the final gate.

## Result Statuses

| Status | Meaning |
|---|---|
| `PASS` | The action or assertion ran and met its contract |
| `FAIL` | Observed behavior violated the contract |
| `BLOCKED` | A capability, permission, observation, or prerequisite was missing |
| `NOT_APPLICABLE` | The condition does not apply to the current selection |

Every attempt is immutable. After a `FAIL` or `BLOCKED` result, fix the infrastructure, adapter, or
product and use a new `run_id`. Never edit an existing report to manufacture a passing result.

## Run Artifacts

```text
evals/runs/<run-id>/
  report.json
  summary.md
  evidence/
  judge-packets/
  panel/
  quality-gate.json
```

`report.json` is the authoritative execution result. It records the manifest, source revision,
selection, metrics, findings, evidence hashes, and cleanup ledger. Generated run directories are
local artifacts and are excluded from Git.

## Independent Judging Protocol

Qualitative scenarios emit one blinded packet for each candidate output. Judges do not use the
environment adapter and cannot see another judge's result.

The panel has four roles:

| Role | Judge |
|---|---|
| `grounding` | Claude Code Opus |
| `task_utility` | OpenCode GPT-5.6-sol |
| `adversarial_safety` | OpenCode GPT-5.6-terra |
| `synthesis_uncertainty` | Claude Code Sonnet |

For every packet:

1. Verify that the packet SHA-256 matches the trusted assignment.
2. Run every judge in a fresh session without resuming or sharing judgments.
3. Provide only the blinded packet, rubric, role instructions, and judgment schema.
4. Store exactly one JSON object that conforms to `judging/schemas/judgment.schema.json`.
5. Validate all four judgments before aggregation.
6. Run `judging/aggregate.py` with the packet, report, assignment, and judgment paths.
7. After every packet has a panel result, run `judging/finalize.py` with the immutable report and
   every panel result to create append-only `quality-gate.json`.

Claude Code uses the `opus` and `sonnet` model aliases. OpenCode uses
`openai/gpt-5.6-sol` and `openai/gpt-5.6-terra`. Judge sessions have no write, shell,
network-fetch, or MCP tools.

Panel policy requires at least four judges, three model families, two providers, and all four
roles. Median dimension scores and critical-failure votes are combined with deterministic hard
gates:

```text
deterministic hard gates AND panel quality
```

Panel scores cannot override security, cleanup, temporal, lineage, performance, or correctness
failures. `report.json` retains `PENDING_JUDGMENT` to preserve the execution artifact. The final
quality decision is stored in `quality-gate.json`, which is SHA-256-bound to the report and every
panel result.

## Image Publication Gate

Release image publication requires the complete finalized run directory in a separate evidence
commit tagged `release-evidence-<source-git-sha>`. Keep the generated paths beneath
`evals/runs/<run-id>/` in that commit. Protect `release-evidence-*` tags from updates and deletion.

The `release-evidence` GitHub environment must require independent reviewers and expose a protected
`VERA_TRUSTED_PANEL_RESULT_SHA256` secret containing the comma-separated approved panel-result
digests. The image workflow fetches the evidence tag before registry login. It validates the full
execution report, every transitive panel input, deterministic panel regeneration, the protected
digest set, and the clean source commit targeted by the version tag. It then promotes the exact
evaluated registry digest to the version and `latest` aliases without rebuilding it. The workflow
summary records the evidence commit and promoted digest.

## Current Coverage

- 46 scenarios.
- 78 checklist checks.
- 35 allowlisted actions.
- 31 core regression cases.
- 5 qualitative real-world workflows.
- 10 production-readiness cases.
