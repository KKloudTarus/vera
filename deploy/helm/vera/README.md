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
- The VERA image reachable by the cluster through an immutable registry digest. Set
  `image.repository` and `image.digest` to the release-gated image.

## 1. The image

The release workflow promotes the exact candidate digest evaluated by the release gate. Obtain
that digest from the release workflow summary, then install with the immutable reference:

```bash
helm install vera deploy/helm/vera -n vera --create-namespace \
  --set image.digest=sha256:<approved-64-character-digest>
```

## 2. Install

```bash
helm install vera deploy/helm/vera -n vera --create-namespace \
  --set image.digest=sha256:<approved-64-character-digest>
kubectl -n vera get pods -w
```

On install, Kubernetes runs migration and role-provisioning Jobs alongside the datastores;
provisioning waits for migration to create the application roles. Application init containers
then wait for the chart's exact Alembic revision and both fixed runtime logins. On upgrades, the
pre-upgrade migration runs before resources change, then an ordinary revisioned Job provisions
roles from the newly applied admin Secret alongside the waiting workloads. Provisioning creates
missing login roles and reapplies grants,
but it never changes an existing password. Runtime pods wait for provisioning and never receive
the schema-owner credential. All setup Jobs have bounded retries and execution deadlines. This
ordering supports Helm's `--wait` and `--wait-for-jobs` flags.

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

Set datastore owner credentials on the first install. They initialize persistent data and a
later values change alone cannot rotate them.

```bash
helm install vera deploy/helm/vera -n vera --create-namespace \
  --set image.digest=sha256:<approved-64-character-digest> \
  --set environment=prod \
  --set api.authRequired=true \
  --set mcp.jwtSecret=<a-long-random-secret> \
  --set postgres.password=<strong> \
  --set postgres.runtimePassword=<strong> \
  --set postgres.workerPassword=<strong> \
  --set minio.rootPassword=<strong> \
  --set minio.secretKey=<strong>
```

With `environment=prod` and a JWT secret, the MCP is an OAuth 2.1 resource server and no
longer serves the local principal. This enables the intentionally non-expiring JWT fallback,
not browser
login. For browser OAuth, also set `mcp.authAudience`, `mcp.oauthIssuer`, and
`mcp.oauthJwksUrl` for an external OIDC provider. Keep the JWT secret until OAuth login and
refresh pass end to end. Manage secrets with your secret store rather than `--set` in
production.

With `secrets.create=false`, create five Secrets before installing the chart:

- `<release>-secrets` for application credentials. For Neo4j it must contain matching
  `VERA_NEO4J__PASSWORD` and `NEO4J_AUTH=neo4j/<password>` values. MCP and bootstrap keys in
  this Secret are injected only into their respective processes.
- `<release>-objectstore-admin` with `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`. Application
  pods receive a separate bucket-scoped identity from `<release>-secrets`.
- `<release>-database-runtime` with `VERA_DB__DSN` for `vera_runtime`.
- `<release>-database-worker` with `VERA_DB__DSN` for `vera_worker_runtime`.
- `<release>-database-admin` with `VERA_DB__DSN`, `POSTGRES_PASSWORD`,
  `VERA_RUNTIME_PASSWORD`, and `VERA_WORKER_PASSWORD` for migration and provisioning only.

All PostgreSQL, MinIO, and Neo4j credentials belong to persisted datastores. The provisioning
hooks create missing runtime identities but intentionally do not mutate passwords during an
upgrade. This keeps a failed or rolled-back upgrade from invalidating credentials still used by
the previous workloads. Keep `minio.accessKey` stable for the lifetime of the datastore.

Chart-generated datastore credentials are immutable after the first successful deployment of
this chart; Helm rejects an upgrade that changes them. Use `secrets.create=false` before a
production install when credential rotation is required.

For the first upgrade from a chart version that used only `<release>-secrets`, preserve every
installed datastore credential and pass `--set secrets.allowLegacyUpgrade=true`. That one-time
upgrade creates the split Secrets. Remove the flag on later upgrades so a missing split Secret
fails closed.

For externally managed Secrets, rotate credentials in an explicit maintenance operation:

1. Quiesce the affected application processes.
2. Change the password through PostgreSQL, MinIO, or Neo4j's administrative interface.
3. Update the corresponding external Kubernetes Secrets without changing `minio.accessKey`.
4. Bump `secrets.externalRevision` and run the Helm upgrade to restart consumers.

Because Helm does not own those Secret objects, a chart rollback does not revert their values.
Migration and provisioning hooks authenticate with the current external admin credentials.

## Choosing the graph backend

`graph.backend` defaults to `neo4j`. FalkorDB is lighter (a Redis-module graph) and its driver
ships in the published image, so it is a one-flag switch:

```bash
helm install vera deploy/helm/vera -n vera --create-namespace \
  --set image.digest=sha256:<approved-64-character-digest> \
  --set graph.backend=falkordb
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
