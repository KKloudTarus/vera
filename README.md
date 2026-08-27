# VERA

Verified Episodic Recall for Agents. A shared, verified agent-memory platform.
VERA owns trust, tenancy, provenance, curation, and lifecycle. The knowledge graph
and its temporal reconciliation are provided by Graphiti, reached only through the
`MemoryEngine` port so the engine can be replaced without touching business code.

## Architecture

Clean architecture. Imports point inward only, and CI enforces it with
import-linter:

```
entrypoints  ->  adapters  ->  application  ->  domain
```

`shared`, `config`, and `observability` are cross-cutting and may be used by any
layer.

Three deployables share one dependency graph (modular monolith):

- `vera-api`: FastAPI HTTP surface
- `vera-mcp`: stateless MCP server for AI clients
- `vera-worker`: async ingestion worker

### Layout

```
src/vera/
  config/          typed settings (pydantic-settings, per process)
  observability/   structured logging (structlog)
  shared/          cross-context kernel: ids, errors/Result, value objects, time
  domain/          pure business model + ports (no I/O)
    ports/         MemoryEngine, JobQueue, ObjectStore, UnitOfWork
    knowledge/ identity/ curation/ retrieval/
  application/     command and query handlers (depend on ports only)
  adapters/        infrastructure: persistence, graph, objectstore, queue
  entrypoints/     api, worker, mcp (each owns its composition root)
  bootstrap.py     shared composition helpers
migrations/        alembic (async)
tests/             unit + integration (testcontainers)
```

## Principles

Two rules shape almost every file:

- No vendor lock-in. Every dependency is open-source and cloud-portable, reached
  through a port. The queue is Postgres-native by default, the object store speaks
  the S3-compatible API, the cache and limiter use Valkey, and the graph is Neo4j (or
  any backend behind the `MemoryEngine` port).
- Postgres and S3 hold the source of truth. Neo4j is a projection that can be
  rebuilt from them at any time (`python -m vera.entrypoints.reprocess <group>`).

## Getting started

Requires the conda env `vera` (Python 3.11+) and Docker for local infrastructure.

```bash
conda activate vera
cp .env.example .env
make install          # pip install -e ".[all]" into the active env, + pre-commit
make up               # start postgres, neo4j, valkey, minio
make migrate          # apply database migrations
make run-api          # http://localhost:8000  (docs at /docs)
make run-worker       # ingestion worker
```

## Container images

One image runs all three processes; they differ only by the command.

```bash
docker build -t vera:local .                 # multi-stage, non-root, runtime deps only
docker compose --profile app up --build      # migrate, then api (:8000), worker, mcp (:8080)
```

The default command is the API; override it for the others:

- worker: `python -m vera.entrypoints.worker.main`
- mcp: `python -m vera.entrypoints.mcp.main`

## Development

```bash
make check            # lint + typecheck + architecture + tests (the CI gate)
make fmt              # format and autofix
make test             # unit tests
make test-int         # integration tests (needs Docker)
```

CI (`.github/workflows/ci.yml`) runs the check gate, the integration suite against
service containers, and an advisory dependency audit on every push and pull request.
See `SECURITY.md` for the security review.
