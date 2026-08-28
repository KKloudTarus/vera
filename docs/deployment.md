# Deployment

VERA is one container image that runs three processes, differing only by command: the API,
the MCP server, and the ingestion worker. PostgreSQL and S3 are authoritative; Neo4j is a
rebuildable projection; Valkey is cache and rate limiter.

## The image

```bash
docker build -t vera:local .
```

Multi-stage, non-root, runtime dependencies only. The base is pinned to
`python:3.11-slim-bookworm`; the builder patches pip/setuptools/wheel and the runtime applies
OS security updates, so no known-vulnerable package ships (verified by `pip-audit`).

Commands:

- API (default): `uvicorn vera.entrypoints.api.main:app --host 0.0.0.0 --port 8000`
- Worker: `python -m vera.entrypoints.worker.main`
- MCP: `python -m vera.entrypoints.mcp.main`

## Docker Compose (single host)

```bash
docker compose --profile app up --build
```

This runs migrate, then the API (`:8000`), worker, and MCP (`:8080`), alongside the infra
(postgres, neo4j, valkey, minio). The default `docker compose up` (no profile) starts only
the infrastructure, which is what local development uses.

## Kubernetes

Manifests are in `deploy/k8s/`:

| File | Contents |
|------|----------|
| `base.yaml` | namespace `vera`, `vera-config` ConfigMap, `vera-secrets` Secret, and the `vera-migrate` Job (`alembic upgrade head`) |
| `api.yaml` | API Deployment + Service, CPU HPA, liveness/readiness probes |
| `mcp.yaml` | MCP Deployment + Service |
| `worker.yaml` | worker Deployment, KEDA `ScaledObject` on ingestion queue depth (CPU HPA fallback) |
| `calibrate-cronjob.yaml` | nightly rerank calibration (`calibrate --apply`) |

Apply:

```bash
# 1. Put real values in vera-config (non-secret) and vera-secrets (DSN, OpenAI key, JWT
#    secret, Neo4j password) in base.yaml, or manage them with your secrets tooling.
kubectl apply -f deploy/k8s/base.yaml        # namespace, config, secret, migrate Job
kubectl -n vera wait --for=condition=complete job/vera-migrate
kubectl apply -f deploy/k8s/api.yaml -f deploy/k8s/mcp.yaml -f deploy/k8s/worker.yaml
kubectl apply -f deploy/k8s/calibrate-cronjob.yaml
```

Config and secrets are injected via `envFrom` (the ConfigMap and Secret). The worker scales
on pending queue depth with KEDA; delete the `ScaledObject` and add a CPU HPA if KEDA is not
installed. Per-group serialization is enforced in-process, so worker replicas are safe.

## Configuration

Every setting is an environment variable `VERA_<SECTION>__<FIELD>` (see
`src/vera/config/settings.py`). The ones that matter most in production:

| Variable | Purpose |
|----------|---------|
| `VERA_DB__DSN` | PostgreSQL DSN (source of truth) |
| `VERA_MEMORY__PROVIDER` | `graphiti` to enable the graph |
| `VERA_NEO4J__URI` / `USER` / `PASSWORD` | graph backend |
| `VERA_MEMORY__OPENAI_API_KEY`, `VERA_MEMORY__EMBEDDER` | LLM extraction and embeddings |
| `VERA_OBJECTSTORE__*` | S3-compatible object store |
| `VERA_RESILIENCE__VALKEY_URL` | shared cache and rate limiter |
| `VERA_MCP__JWT_SECRET`, `VERA_MCP__AUTH_ISSUER`, `VERA_MCP__AUTH_AUDIENCE` | MCP auth |
| `VERA_CONNECTORS__SPECS` | scheduled connectors (JSON) |

Keep secrets out of the ConfigMap and out of `VERA_CONNECTORS__SPECS`; connectors read their
tokens from environment variables named by `token_env`.

## Operational commands

Run these as one-off Jobs (same image) or locally against the same config:

```bash
python -m vera.entrypoints.create_source ...     # create a knowledge source
python -m vera.entrypoints.reprocess <group>     # rebuild a group's graph from Postgres, then verify
python -m vera.entrypoints.backfill_embeddings <group>  # embed canonical names for existing entities
python -m vera.entrypoints.calibrate --apply     # calibrate rerank weights from feedback
python -m vera.entrypoints.retrieval_eval golden.json   # score retrieval quality (CI gate)
python -m vera.entrypoints.dedup_eval pairs.json # measure the dedup threshold and judge
```

## Disaster recovery

PostgreSQL and S3 are backed up; Neo4j is rebuilt from them. See
[../docs/dr-runbook.md](dr-runbook.md) for the full procedure, including the automated
post-rebuild verification and the embedding-model change process.

## Health and observability

- Liveness `GET /health/live`, readiness `GET /health/ready` (checks db, graph, object
  store). The worker exposes Prometheus metrics on `:9100`; the API exposes `/metrics`.
- Set `VERA_OBSERVABILITY__OTLP_ENDPOINT` to export OpenTelemetry traces.
