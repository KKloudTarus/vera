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

## 1. The image

The image is published to GHCR by the release workflow as a public package, so the cluster
pulls `ghcr.io/kkloudtarus/vera:0.1.0` with no registry secret. Nothing to do here for a normal
cluster.

For an air-gapped single-node k3s cluster, build it locally and import it into containerd,
then point the chart at the local tag:

```bash
docker build -t vera:0.1.0 .
docker save vera:0.1.0 | sudo k3s ctr images import -
# then add: --set image.repository=vera
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

`graph.backend` defaults to `neo4j`. FalkorDB is lighter (a Redis-module graph) and its driver
ships in the published image, so it is a one-flag switch:

```bash
helm install vera deploy/helm/vera -n vera --create-namespace --set graph.backend=falkordb
```

## Sizing

The defaults request roughly 1.3 vCPU / 3 GiB across the stack and are comfortable on a node
with 4 vCPU / 16 GiB. Neo4j is the largest tier; FalkorDB roughly halves the graph memory once
its driver is in the image.

## Uninstall

```bash
helm uninstall vera -n vera
# PersistentVolumeClaims are kept by default; remove them to delete the data:
kubectl -n vera delete pvc --all
```
