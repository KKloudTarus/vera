# Kubernetes deployment

Manifests for the three VERA processes. Postgres, Neo4j, Valkey, and the object store
are expected as managed or separately-deployed services reachable at the hostnames in
`base.yaml` (adjust to your cluster).

## Apply order

```bash
# 1. Fill in deploy/k8s/base.yaml Secret (or use an external secret manager), then:
kubectl apply -f deploy/k8s/base.yaml     # namespace, config, secret, migration job
kubectl -n vera wait --for=condition=complete job/vera-migrate --timeout=300s
kubectl apply -f deploy/k8s/api.yaml
kubectl apply -f deploy/k8s/mcp.yaml
kubectl apply -f deploy/k8s/worker.yaml
```

## Scaling

- **api**, **mcp**: HorizontalPodAutoscaler on CPU (they are stateless).
- **worker**: a KEDA `ScaledObject` scales on ingestion queue depth
  (`ingestion_jobs` pending count). Per-group serialization is enforced in-process, so
  multiple worker replicas are safe. If KEDA is not installed, replace the `ScaledObject`
  with a CPU HPA.

## Images

Built from the repository `Dockerfile` (one image, command per process). Push to your
registry and update the `image:` fields (placeholder: `ghcr.io/kkloudtarus/vera:latest`).

## Notes

- Containers run as a non-root user (enforced by `runAsNonRoot`).
- `VERA_API__AUTH_REQUIRED=true` in prod; secrets come from the `vera-secrets` Secret.
- Point `VERA_OBSERVABILITY__OTLP_ENDPOINT` at your OpenTelemetry collector for traces.
