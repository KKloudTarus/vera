# Deploying VERA with Helm

This chart installs the full VERA stack into one Kubernetes namespace: the API, MCP
server, and ingestion worker, plus their datastores (PostgreSQL, a graph backend, Valkey,
and MinIO). It is sized for a single-cluster pilot (k3s, kind, or a small managed cluster).

PostgreSQL and MinIO are authoritative; the graph is a rebuildable projection. The defaults
boot with no external credentials: a deterministic embedder, an offline LLM stub, and one
unauthenticated local MCP principal. Add an LLM key and turn on auth for real use.

## Prerequisites

- A Kubernetes cluster and `kubectl` context, with a default StorageClass (k3s ships
  `local-path`).
- Helm 3+ (Helm 4 works too).
- The VERA image reachable by the cluster. On a single-node cluster you can build it locally
  and import it into the node's container runtime (see below); otherwise push it to a
  registry and set `image.repository`/`image.tag`.

## 1. Make the image available

Build the image from the repo root and, for a single-node k3s cluster, import it into k3s's
containerd so no registry is needed:

```bash
docker build -t vera:0.1.0 .
docker save vera:0.1.0 | sudo k3s ctr images import -
```

For a multi-node or managed cluster, push instead and set the values:

```bash
docker tag vera:0.1.0 <registry>/vera:0.1.0 && docker push <registry>/vera:0.1.0
# then: --set image.repository=<registry>/vera --set image.tag=0.1.0
```

## 2. Install

```bash
helm install vera deploy/helm/vera -n vera --create-namespace
kubectl -n vera get pods -w
```

The datastores start first; the `vera-migrate` post-install hook waits for PostgreSQL and
runs `alembic upgrade head`; then the API, MCP, and worker become ready.

## 3. Verify

```bash
kubectl -n vera port-forward svc/vera-api 8000:8000 &
curl -s localhost:8000/health/ready        # {"status":"ready", ...}

kubectl -n vera port-forward svc/vera-mcp 8080:8080 &
# In the default local profile the MCP serves an unauthenticated local principal.
```

## 4. Turn on real semantic memory

The default embedder is deterministic and the LLM is an offline stub, so ingestion extracts
nothing. Provide an OpenAI key and switch the embedder:

```bash
helm upgrade vera deploy/helm/vera -n vera \
  --set memory.openaiApiKey=sk-... --set memory.embedder=openai
```

## 5. Harden for a real deployment

```bash
helm upgrade vera deploy/helm/vera -n vera \
  --set environment=prod \
  --set api.authRequired=true \
  --set mcp.jwtSecret=<a-long-random-secret> \
  --set postgres.password=<strong> --set minio.secretKey=<strong>
```

With `environment=prod` and a JWT secret, the MCP is an OAuth 2.1 resource server and no
longer serves the local principal. Manage secrets with your secret store rather than
`--set` in production.

## Choosing the graph backend

`graph.backend` defaults to `falkordb` (light, a Redis-module graph). Neo4j is heavier but a
drop-in alternative; it needs more memory:

```bash
helm install vera deploy/helm/vera -n vera --create-namespace --set graph.backend=neo4j
```

## Sizing

The defaults request roughly 1 vCPU / 2.5 GiB across the stack and are comfortable on a node
with 4 vCPU / 16 GiB. FalkorDB keeps the graph tier light; Neo4j roughly doubles its memory.

## Uninstall

```bash
helm uninstall vera -n vera
# PersistentVolumeClaims are kept by default; remove them to delete the data:
kubectl -n vera delete pvc --all
```
