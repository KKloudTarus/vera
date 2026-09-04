# Kubernetes deployment

Manifests for the three VERA processes. Postgres, Neo4j, Valkey, and the object store
are expected as managed or separately-deployed services reachable at the hostnames in
`base.yaml` (adjust to your cluster).

## Apply order

```bash
# 1. Fill in the five Secrets and replace every all-zero VERA image digest with the
#    digest approved by the release gate (or use an external secret manager).
kubectl apply -f deploy/k8s/base.yaml     # namespace, config, secrets, migration job
kubectl -n vera wait --for=condition=complete job/vera-migrate-d4e5f6a7b8c9 --timeout=600s
# 2. Provision the runtime logins with deploy/postgres/provision-runtime.sh. Run it from a
#    trusted host that resolves PostgreSQL and supplies VERA_RUNTIME_PASSWORD,
#    VERA_WORKER_PASSWORD, and VERA_SCALER_PASSWORD.
kubectl apply -f deploy/k8s/api.yaml
kubectl apply -f deploy/k8s/mcp.yaml
kubectl apply -f deploy/k8s/worker.yaml
```

## Scaling

- **api**, **mcp**: HorizontalPodAutoscaler on CPU (they are stateless).
- **worker**: a KEDA `ScaledObject` scales on ingestion queue depth
  (`ingestion_jobs` pending count). Per-group serialization is enforced in-process, so
  multiple worker replicas are safe. KEDA uses a standard PostgreSQL DSN for a dedicated login
  with direct `SELECT` access only to `ingestion_jobs`. If KEDA is not installed, replace the
  `ScaledObject` with a CPU HPA.

## Images

Built from the repository `Dockerfile` (one image, command per process). Push to your registry and
set every `image:` field to the same immutable digest. The all-zero digest is a fail-closed
placeholder and cannot pull an image. Give the migration Job a new name
for each schema revision, wait for it to complete, then apply the workload manifests.

## Notes

- Containers run as a non-root user (enforced by `runAsNonRoot`).
- `vera-secrets` carries the API/MCP runtime DSN and shared application secrets;
  `vera-mcp-secret` is exposed only to MCP.
- `vera-worker-database` carries only the worker DSN. `vera-admin-database` is exposed only to
  the migration Job. `vera-scaler-database` carries KEDA's read-only standard PostgreSQL DSN.
  The application runtime login cannot assume `vera_worker`.
- `VERA_API__AUTH_REQUIRED=true` and database role enforcement are enabled in prod.
- Point `VERA_OBSERVABILITY__OTLP_ENDPOINT` at your OpenTelemetry collector for traces.
