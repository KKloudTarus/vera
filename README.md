# VERA

[![CI](https://github.com/KKloudTarus/vera/actions/workflows/ci.yml/badge.svg)](https://github.com/KKloudTarus/vera/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)

Verified Episodic Recall for Agents. A shared, verified agent-memory platform. VERA owns
trust, tenancy, provenance, curation, and lifecycle. The knowledge graph and its temporal
reconciliation are provided by Graphiti, reached only through the `MemoryEngine` port so
the engine can be replaced without touching business code.

The problem VERA solves: agents need a shared long-term memory they can trust. Raw text
dumped into a vector store gives no provenance, no way to tell a verified fact from a
guess, no tenancy isolation, and no way to correct or forget a fact later. VERA adds a
verification and trust layer on top of a temporal knowledge graph, so a retrieved fact
carries where it came from, how trusted it is, when it was true, and who may see it.

## Documentation

The documentation site is at **https://kkloudtarus.github.io/vera/**. Source lives in
[`docs/`](docs/index.md): [getting started](docs/getting-started.md),
[loading knowledge](docs/loading-knowledge.md) (repositories, Markdown, Confluence),
[using the API](docs/usage.md), [connecting an AI agent over MCP](docs/mcp.md),
[deployment](docs/deployment.md), and [architecture and algorithms](docs/architecture.md).

## Contents

- [Architecture](#architecture)
- [Core concepts](#core-concepts)
- [Ingestion and curation](#ingestion-and-curation-how-data-becomes-memory)
- [Scoring methodology](#scoring-methodology-how-data-is-graded)
- [Retrieval and ranking](#retrieval-and-ranking)
- [Embeddings and entity resolution](#embeddings-and-entity-resolution)
- [Temporal model](#temporal-model-bi-temporal-truth)
- [Retraction and erasure](#retraction-and-erasure)
- [Feedback calibration](#feedback-calibration)
- [Retrieval quality evaluation](#retrieval-quality-evaluation)
- [Security and tenancy](#security-and-tenancy)
- [Reliability and observability](#reliability-and-observability)
- [Interfaces](#interfaces)
- [Operations](#operations)
- [Getting started](#getting-started)

## Architecture

Clean (hexagonal) architecture. Imports point inward only, and CI enforces it with
import-linter:

```
entrypoints  ->  adapters  ->  application  ->  domain
```

`domain` and `application` import no infrastructure. Ports are `typing.Protocol`; adapters
are the only place infrastructure libraries are imported. `shared`, `config`, and
`observability` are cross-cutting.

Three deployables share one dependency graph (modular monolith):

- `vera-api`: FastAPI HTTP surface
- `vera-mcp`: stateless MCP server for AI clients
- `vera-worker`: async ingestion worker

### Layout

```
src/vera/
  config/          typed settings (pydantic-settings, per process)
  observability/   structured logging, OpenTelemetry tracing, Prometheus metrics, cost
  shared/          cross-context kernel: ids, errors/Result, value objects, time, security
  domain/          pure business model + ports (no I/O)
    ports/         MemoryEngine, JobQueue, ObjectStore, UnitOfWork, Embedder, Reranker, ...
    knowledge/ identity/ curation/ retrieval/ ontology/
  application/     command and query handlers (depend on ports only)
  adapters/        infrastructure: persistence, graph, objectstore, queue, resilience, ...
  entrypoints/     api, worker, mcp, plus operational commands (reprocess, calibrate, ...)
  bootstrap.py     shared composition root
migrations/        alembic (async)
tests/             unit + integration + llm (testcontainers)
```

## Core concepts

**No vendor lock-in.** Every dependency is open-source and cloud-portable, reached through
a port. The queue is Postgres-native, the object store speaks the S3-compatible API, the
cache and rate limiter use Valkey, and the graph is Neo4j (or any backend behind the
`MemoryEngine` port).

**Source of truth.** PostgreSQL and S3 are authoritative. Neo4j is a projection that can
be rebuilt from them at any time. Writes commit to Postgres first; graph updates flow
through an outbox, so the graph is always reconstructable and never the system of record.

**Scopes and tenancy.** Every scope has an opaque `group_id`: `o:` organization, `w:`
workspace, `p:` project, `u:` personal. A client never chooses one; VERA assigns them. The
six knowledge tables enforce row-level security by `group_id`, so a tenant cannot read
another tenant's memory even through a bug in application code.

**Provenance on every fact.** A retrieved fact carries its source, verification status,
authority, confidence, the ontology and pipeline versions that produced it, and its
valid-time window. Nothing is anonymous.

## Ingestion and curation: how data becomes memory

Data never lands in the graph directly. It flows through a curation pipeline that decides
whether it is trustworthy enough to publish.

1. **Connect.** A source connector (Confluence, Jira, Slack, Git, CMDB, PDF, filesystem)
   pulls records incrementally by a cursor. Connector credentials are resolved from an
   environment variable named by `token_env`, never stored in the config blob. Re-ingesting
   an unchanged record is a no-op (content-hash idempotency); a changed record appends a
   version.

2. **Store the raw artifact.** The raw bytes go to S3, so the graph stays rebuildable from
   Postgres plus S3.

3. **Extract claims.** The extractor turns an artifact into `(subject, predicate, object)`
   claims. Structured metadata (CMDB triples) needs no model. Free text is extracted by an
   LLM (`gpt-4.1-nano`, temperature 0) that also normalizes entity names to a canonical
   English form and predicates to `UPPER_SNAKE`, so the same real-world fact expressed two
   ways collapses to one triple.

4. **Classify and route by trust tier.** The source's trust tier determines what happens
   to each claim (see below).

5. **Publish.** A verified claim becomes a published episode in Postgres and an outbox job.
   The worker consumes the job, writes the fact to the graph, and stitches each graph node
   and edge back to a canonical entity and the published episode (the durable index that
   makes retraction and rebuild possible). Per-group serialization (a hash-routed lane pool
   plus a Postgres advisory lock) keeps one group's writes ordered and exactly-once.

## Scoring methodology: how data is graded

Every fact is graded along independent axes, set at publish time and carried through
retrieval.

**Trust tier (of the source), 1 to 4** drives both the publish decision and the authority
score:

| Tier | Meaning        | Publish action     | Authority |
|------|----------------|--------------------|-----------|
| 1    | Authoritative  | auto-publish       | 1.00      |
| 2    | Curated        | auto-publish       | 0.85      |
| 3    | Informational  | review required    | 0.70      |
| 4    | Unverified     | proposal only      | 0.40      |

Tier 1 and 2 sources publish automatically. Tier 3 holds the claim for human review. Tier
4 (for example an agent proposal) only ever creates an unverified proposal in the proposer's
personal scope; it is never auto-published and never enters a shared scope.

**Verification status** records how a fact was confirmed: `human_verified`, `auto`, or
`pending`. It maps to a verification score (1.0 / 0.8 / 0.5) used in ranking.

**Confidence** (0 to 1) is the extractor's confidence in the claim, stored on the episode
and blended into ranking so a shakier extraction ranks lower, all else equal.

**Contamination guard.** A shared scope only accepts verified knowledge. An unverified or
disputed claim cannot be published into a workspace or organization scope, so one agent's
guess cannot silently become another agent's "fact".

## Retrieval and ranking

Retrieval is a three-stage pipeline. Stages 1 and 2 always run; stage 3 is optional.

### Stage 1: candidate generation (graph hybrid search)

The `MemoryEngine` runs Graphiti's edge hybrid search with reciprocal rank fusion (RRF).
RRF merges the semantic (vector) and lexical (full-text) rankings without needing their
scores to be comparable: an item at rank `r` contributes `1 / (k + r)` from each list, with
`k = 1`. Search runs once per `group_id` and merges by edge, because Graphiti's multi-group
full-text filter is order-dependent; per-group search is order-independent and correct. A
valid-time filter is applied here (see the temporal model).

### Stage 2: weighted blend (VERA's rerank)

VERA re-ranks the candidates by blending signals that RRF does not know about. For each
candidate the blended score is a weighted sum of six normalized signals in [0, 1]:

```
score = w_relevance   * relevance      # stage-1 score, min-max normalized over the batch
      + w_authority    * authority      # source trust tier -> [0.4 .. 1.0]
      + w_verification * verification   # human_verified 1.0 / auto 0.8 / pending 0.5
      + w_recency      * recency        # exp(-ln2 * age / half_life); invalidated -> 0
      + w_feedback     * feedback       # Laplace-smoothed up/down votes: (up+1)/(up+down+2)
      + w_confidence   * confidence     # the claim's extraction confidence
```

Defaults: relevance 0.40, authority 0.18, verification 0.12, recency 0.12, feedback 0.08,
confidence 0.10, with a 30-day recency half-life. The weights are normalized, so they need
not sum to one, and they are tunable (see feedback calibration). Recency decays
exponentially: a fact loses half its recency score every half-life. Feedback uses Laplace
smoothing so a single downvote does not zero a fact and an unvoted fact sits at a neutral
0.5. The exact signal vector shown for each hit is returned with the result, so it can be
logged with any feedback for later calibration.

### Stage 3: cross-encoder (optional)

When enabled, a cross-encoder reads the query and each candidate fact together and scores
their direct relevance in [0, 1]. It runs only on the top `cross_encoder_top_n` candidates
(bounded cost) and its score is blended with the stage-2 score:
`final = (1 - w) * normalized_blend + w * cross_encoder`. This catches head cases the
bag-of-signals blend cannot. Off by default, since it adds a model call per search.

## Embeddings and entity resolution

**Embedding model.** OpenAI `text-embedding-3-small` at 1536 dimensions by default (a
deterministic offline embedder is used in tests). Embeddings are cached in-process (LRU
with TTL) and optionally in Valkey (L2), keyed by `model:dim` plus a content hash, so a
repeated text never pays for a second call. Every provider call is metered for cost, inside
the cache, so cache hits cost nothing.

**One dimension per group.** A group's vectors must share one embedding dimension or
similarity is meaningless. Ingestion records the model and dimension a group was first
built with and refuses a later write under a different one, so dimensions never silently
mix. Changing the embedding model is applied by reprocessing the group, which re-embeds
every fact under the new configuration.

**Cross-lingual and synonym deduplication.** The same real-world entity can arrive under
different surface forms ("payment service", "paymentapi", a translation). Resolution runs in
order: exact normalized match, then `pg_trgm` fuzzy match, then a semantic step. The
semantic step is deliberately two-part, because benchmarking showed cosine over bare names
is a weak signal (it scores sibling services as close as true synonyms, and translations
far apart, so no single threshold separates them):

- Embedding cosine is used only as a **blocker**: a very close match (>= 0.86) links
  immediately; otherwise names above a low block threshold (0.55) become candidates.
- An **LLM equivalence judge** confirms which candidate, if any, is the same entity. It
  resolves synonyms, abbreviations, and cross-lingual names that the embedding step alone
  cannot. It runs on the larger model, which is measurably stable on the hard
  sibling-versus-same distinction where the small model occasionally makes the corrupting
  false merge.

Each resolution is counted by `vera_entity_resolution_total` (created / linked by cosine /
linked by judge), so precision can be watched on real data. Use `dedup_eval` to measure a
threshold sweep and the judge's agreement on a labeled set before trusting it.

## Temporal model: bi-temporal truth

Facts carry a valid-time window (`valid_at`, `invalid_at`). A search defaults to "as of
now", so a superseded or retracted fact is hidden from the current view; an explicit
`as_of` returns the memory as it stood at that instant. This is a first-class query on both
the HTTP API (`as_of` on `/memory/search`) and the MCP `memory_search` tool.

**Supersession.** When a new, sufficiently trusted fact contradicts an existing one, the
old fact is invalidated (its `invalid_at` is set) rather than deleted, so history stays
queryable. One `SupersedePolicy` governs both the structured and free-text publish paths,
so contradiction handling is uniform:

- A functional predicate (one value at a time, for example `RUNS_ON`) supersedes every
  earlier value.
- A multi-valued predicate (for example `DEPENDS_ON`) keeps coexisting values unless an
  LLM contradiction judge marks a specific one as truly replaced.

The graph adapter never decides contradictions on its own for curated writes; it only
closes the edges the policy names.

## Retraction and erasure

A published source can be withdrawn end to end: its edges leave the graph, its graph maps
are cleared, and the episode is marked retracted (hidden from search and skipped by a
rebuild). Erasure goes further for data-subject requests, deleting the episode row and its
raw artifact bytes from the object store. Every retraction writes an audit event. Postgres
commits first (the source of truth); the graph and object store are updated after.
Exposed as `DELETE /memory/sources/{source_id}` with an `erase` flag.

## Feedback calibration

The rerank weights are not guesses that stay fixed. Each returned hit's signal vector is
logged with the thumbs up/down a caller later gives it (the learning-to-rank feature-logging
pattern). Calibration reads those labeled vectors and derives weights by a transparent
mean-difference rule: a signal that is higher on helpful hits than on unhelpful ones earns
weight in proportion to how well it separates the two; a signal that does not separate them
earns none. It is deterministic, so an operator can read why a weight moved.

Calibrated weights can be persisted as the active set (guarded by a minimum sample count so
a few votes cannot swing ranking), which the API and MCP ranker load at startup in place of
the configured defaults. A nightly CronJob runs the calibration, closing the loop from
feedback to the weights the ranker uses.

## Retrieval quality evaluation

A golden set (queries plus the substrings a correct answer must contain) is scored with the
standard metrics: hit@k (was a correct fact in the top k) and MRR (mean reciprocal rank).
The evaluator runs each query through the real ranker and fails the build if the hit rate
falls below a threshold, so a regression in ranking, dedup, or the model is caught in CI
rather than in production. Run it with `python -m vera.entrypoints.retrieval_eval
golden.json`.

## Security and tenancy

- **Row-level security.** The six knowledge tables enable and force RLS by `group_id`. The
  application connects as a non-superuser role and sets the tenant per unit of work, so
  isolation holds even if a query forgets its `WHERE group_id`.
- **Authentication.** API keys (a 256-bit random secret, stored only as a SHA-256 hash) and
  OIDC (validated by signing key or live JWKS, with just-in-time provisioning). The MCP
  server is an OAuth 2.1 Resource Server (RFC 9728): an unauthenticated call gets a 401 that
  points to the protected-resource metadata, and a token missing the required scope is
  rejected before any tool runs.
- **Authorization.** Roles are totally ordered (VIEWER < MEMBER < ADMIN < OWNER), so a check
  is one comparison. A workspace admin can issue, rotate, and revoke API keys for principals
  in a workspace it administers; revocation authorizes the caller (self or admin on a shared
  workspace).
- **Service accounts** are principals of kind `service_account`, so authentication and scope
  resolution are uniform across humans and machines.

## Reliability and observability

- **Resilience.** A per-provider chain of circuit breaker, token-bucket rate limiter
  (in-process or Valkey-backed with an atomic Lua script), full-jitter retry, and per-call
  and per-episode timeouts wraps the LLM and embedding calls.
- **Backpressure.** The lane pool's bounded queues stall the dispatcher when full; the
  worker also warns and increments a metric when the pending backlog crosses a configurable
  threshold, turning silent backpressure into an alertable signal.
- **Observability.** OpenTelemetry tracing (FastAPI, SQLAlchemy, asyncpg, httpx), Prometheus
  metrics with bounded labels, and real token-cost accounting per request kind, model, and
  group, written to `llm_usage`.

## Interfaces

**HTTP API** (`vera-api`): identity and tenancy administration, `/memory/search` (with
`as_of`), `/memory/explore` (multi-hop), and `DELETE /memory/sources/{id}` (retraction).
Writing to memory goes through connectors and curation, not a raw ingest endpoint, so trust
and provenance are never bypassed.

**MCP server** (`vera-mcp`): the safe, minimal surface AI clients connect to. Tools:
`memory_search`, `memory_get_context`, `memory_explore`, `memory_explain`,
`memory_get_source`, `memory_recent_changes`, `memory_propose`, `memory_feedback`. Every
tool resolves the caller's scopes server-side; tools expose only reads and proposals, never
raw graph mutation.

**Multi-hop reasoning** (`memory_explore` / `/memory/explore`): from a named entity, return
the facts on paths within N hops (bounded), with provenance, to trace how entities connect
in ways a single-fact search misses.

## Operations

Operational commands, each a module under `vera.entrypoints`:

```bash
python -m vera.entrypoints.create_source ...               # create a knowledge source for a connector to ingest into
python -m vera.entrypoints.reprocess <group_id>            # rebuild a group's graph from Postgres, then verify
python -m vera.entrypoints.backfill_embeddings <group_id>  # embed canonical names for pre-existing entities
python -m vera.entrypoints.calibrate [--apply] [groups...] # calibrate rerank weights from feedback
python -m vera.entrypoints.dedup_eval <pairs.json>         # measure dedup threshold and judge on labeled pairs
python -m vera.entrypoints.retrieval_eval <golden.json>    # score retrieval quality (CI gate)
```

Disaster recovery: Postgres and S3 are authoritative and backed up; Neo4j is rebuilt with
`reprocess`, which runs an automated post-rebuild check. See `docs/dr-runbook.md`.
Kubernetes manifests (API, MCP, worker with KEDA queue-depth scaling, and the calibration
CronJob) are in `deploy/k8s/`.

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

### Container images

One image runs all three processes; they differ only by the command.

```bash
docker build -t vera:local .                 # multi-stage, non-root, runtime deps only
docker compose --profile app up --build      # migrate, then api (:8000), worker, mcp (:8080)
```

The default command is the API; override it for the others:

- worker: `python -m vera.entrypoints.worker.main`
- mcp: `python -m vera.entrypoints.mcp.main`

### Development

```bash
make check            # lint + typecheck + architecture + tests (the CI gate)
make fmt              # format and autofix
make test             # unit tests
make test-int         # integration tests (needs Docker)
```

Tests are split by marker: unit (default), `integration` (needs the compose stack), and
`llm` (needs a real OpenAI key; excluded from the default gate). CI runs the check gate, the
integration suite against service containers, and a required dependency audit on every push
and pull request. See `SECURITY.md` for the security policy and review.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, the local
gate, and pull-request expectations, and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for
community standards. For a security vulnerability, follow [SECURITY.md](SECURITY.md) rather
than opening a public issue.

## License

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attributions.
