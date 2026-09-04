# Knowledge Fabric Runbooks

Operational procedures for the Phase 8 cutover, production alerts, and controlled feature
transitions. These cover only implemented features.

## Migrating Existing Knowledge into the Fact Model

The backfill converts `published_episodes` into Facts, Assertions, and Evidence. It is
additive and idempotent: it never alters or deletes the legacy tables or the graph maps, and
re-running converges, so a partial run can be resumed.

1. Apply migrations (creates the fabric tables, indexes, and roles):
   ```
   alembic upgrade head
   ```
2. Backfill one group first to validate, then all groups, with verification:
   ```
   python -m vera.entrypoints.migrate_to_fabric <group_id> --verify
   python -m vera.entrypoints.migrate_to_fabric --all --verify
   ```
   Each group logs `fabric.backfill.done` with counts and `needs_review` (free-text episodes
   with no structured triple, which are counted rather than given invented provenance).
   Verification fails the run if a group with episodes produced no assertions.
3. Rebuild the projections from the authoritative rows:
   ```
   # graph projection per group (rebuildable, drift-checked)
   #   FactProjectionService.rebuild_group / verify_group
   # passage/fact full-text indexes are generated columns: no rebuild step needed.
   ```
4. Read through the new surface (`/v2/knowledge`, `knowledge_*` MCP tools) alongside the old
   `/memory` surface, which keeps working throughout.

Cutover to the fabric as the primary read path is a configuration decision made only after
verification passes for every group. Until then both models coexist.

## Rollback

Because the backfill is additive, rollback is simply not cutting over: keep reading through
`/memory`. To remove the fabric entirely, downgrade the migrations in reverse
(`alembic downgrade <rev>`); every fabric migration has a tested `downgrade`. No legacy data
is touched by the backfill, so there is nothing to restore.

## Production Database Roles

Migrations run as the schema owner (or a superuser). The application never needs a superuser
at runtime (invariant 12). Three non-superuser roles, created by migration `c9e2f3a4b5d6`:

- `vera_app` (NOBYPASSRLS): the tenant path. The API `SET ROLE`s to it per request and RLS is
  enforced by `vera.group_id`.
- `vera_trusted` (BYPASSRLS, read-only): the cross-scope read models (retrieval and the
  knowledge read model), which filter `group_id` explicitly across a principal's resolved
  scopes.
- `vera_worker` (BYPASSRLS, read/write): the worker and projection paths, which write across
  groups and always filter `group_id` explicitly.

Deployment uses `vera_runtime` for API, MCP, bootstrap, and calibration. It can assume
`vera_app` and `vera_trusted`. The worker uses `vera_worker_runtime`, which can also assume
`vera_worker`. Only migration and role-provisioning jobs receive the schema-owner credential.
The RLS boundary remains the enforcing control for the tenant path; BYPASSRLS roles are used
only by trusted server-side paths that pass explicit group filters.

## Benchmarking

Numbers must be measured, not assumed. The harness seeds a throwaway group and reports
context-pack latency percentiles in your environment, then cleans up:

```
python -m vera.entrypoints.benchmark_fabric --facts 10000 --queries 500
```

It logs `benchmark.context_pack_latency_ms` with p50/p95/p99. Run it against a
production-shaped database (data volume, hardware, concurrency) before quoting any figure.

## Production Alerts

Load `deploy/observability/v1/prometheus-alerts.yaml` into any Prometheus-compatible rule
evaluator and import `deploy/observability/v1/dashboard.json` with its Prometheus datasource
variable. The six headings below are stable alert links. Record the alert time, active release,
trace IDs, mitigation, and recovery evidence in the incident timeline.

### write_failure

**Owner:** application owner

1. Halt active rollouts. Use the failing request's trace to identify the write boundary and
   dependency. Do not log payloads or credentials.
2. If PostgreSQL is unavailable, follow
   [postgres_unavailable](dr-runbook.md#postgres_unavailable). If the raw artifact write failed,
   follow [object_store_unavailable](dr-runbook.md#object_store_unavailable).
3. Restore the dependency, retry the idempotent request or job, and confirm the authoritative
   PostgreSQL row and object-store key agree before resolving the alert.

### projection_drift

**Owner:** operations

1. Pause community builds and find affected groups from the drift check's traces and logs.
2. Confirm PostgreSQL facts are intact. Treat the graph as disposable and never copy graph state
   back into PostgreSQL.
3. Run `python -m vera.entrypoints.reprocess <group_id>` for each affected group. Resolve only
   after verification reports no missing or extra projected facts and a known-fact search passes.

### queue_lag

**Owner:** operations

1. Check worker availability, pending/in-flight/dead counts, the oldest pending age, and recent
   dependency or rate-limit failures. Halt rollouts while lag grows.
2. Restore the failed dependency or worker capacity. Let visibility-timeout reclamation and the
   normal idempotent retry path recover jobs. Do not edit queue rows by hand.
3. Resolve after lag stays below five minutes, pending work drains, and no new dead jobs appear.
   Escalate persistent dependency failures to the matching incident procedure.

### freshness

**Owner:** application owner

1. Identify the stale source and compare its connector watermark, latest artifact version,
   queue age, extraction result, and projection checkpoint.
2. Repair the earliest failing stage. Re-run the configured connector after its dependency is
   healthy; source positions and content hashes make the ingest path idempotent.
3. Confirm the latest source revision is searchable with correct provenance and the measured lag
   is below fifteen minutes before resolving.

### extraction_failure

**Owner:** application owner

1. Use traces to separate invalid content from provider timeout, rate limit, credential, or model
   failures. Keep source content and provider responses out of incident channels.
2. Correct the input or restore provider access. Re-ingest the same artifact, or use controlled
   re-extraction against its immutable stored version.
3. Confirm a successful extraction run, published evidence with the expected pipeline version,
   and no new failures before resolving.

### retrieval_latency

**Owner:** application owner

1. Compare database, graph, embedding, reranking, and packing spans for the affected operation.
   Check saturation and error rates before adding capacity.
2. Roll back the latest retrieval transition if it caused the increase. Otherwise restore the
   slow dependency or reduce load without bypassing authorization or provenance checks.
3. Resolve after p95 remains at or below two seconds for ten minutes and a fixed query sample
   shows no critical relevance regression.

## Rollout and Rollback Transitions

Every transition has one change owner and an independent verifier. Record before/after fact
counts, projection parity, queue state, fixed-query retrieval results, release ID, and timestamps.
Change one transition at a time. Stop on any production alert, authoritative count mismatch, or
critical retrieval regression. Rollback never deletes PostgreSQL facts or raw object-store data.

### legacy_to_dual

**Owner:** application owner

**Rollout:** Backfill and verify one canary group, capture legacy and Fabric counts, then set
`VERA_MEMORY__FABRIC_WRITE_MODE=dual` on canary workers. Ingest a fixed sample and require both
legacy episodes and Fabric facts, a drained queue, zero projection drift, and equivalent retrieval
before expanding.

**Rollback:** Restore `VERA_MEMORY__FABRIC_WRITE_MODE=legacy`, drain work already committed, and
verify the legacy query sample. Leave additive Fabric rows in place for diagnosis and a later
retry.

### dual_to_fabric

**Owner:** application owner

**Rollout:** Require verified backfill for every group, zero queue lag and projection drift, and a
signed retrieval comparison. Set `VERA_MEMORY__FABRIC_WRITE_MODE=fabric` on a canary, verify new
writes and retrieval, then expand while monitoring all six production signals.

**Rollback:** Restore `VERA_MEMORY__FABRIC_WRITE_MODE=dual`. Drain committed outbox work and repeat
the parity and fixed-query checks. Keep PostgreSQL facts and raw artifacts unchanged.

### role_enforcement_off_to_on

**Owner:** operations

**Rollout:** Run `deploy/postgres/provision-runtime.sh` after migrations. Confirm that
`vera_runtime` cannot assume `vera_worker` and that `vera_worker_runtime` can assume all three
runtime roles. Test tenant isolation and cross-scope worker/read operations with the canary
credentials, then set `VERA_DB__ROLE_ENFORCEMENT=true` one process class at a time.

**Rollback:** Set `VERA_DB__ROLE_ENFORCEMENT=false` and restart the affected process class. Keep
the least-privilege grants, verify readiness and tenant isolation, and investigate every denied
operation before retrying.

### vector_retrieval_off_to_on

**Owner:** application owner

**Rollout:** Pin the provider, model, version, and dimension. Run
`python -m vera.entrypoints.backfill_chunk_embeddings <group_id>` and
`python -m vera.entrypoints.backfill_fact_embeddings <group_id>` for canary groups. Compare a
fixed query set, then set `VERA_MEMORY__VECTOR_SEARCH_ENABLED=true` on the canary and expand only
if relevance and latency gates pass.

**Rollback:** Set `VERA_MEMORY__VECTOR_SEARCH_ENABLED=false`. Keep versioned embeddings for audit
and a later retry, confirm full-text retrieval serves the fixed query set, and record the quality
or latency regression that caused rollback.

### community_build_off_to_on

**Owner:** application owner

**Rollout:** Fix a fact snapshot and cost budget, then run
`python -m vera.entrypoints.build_communities <group_id>` for a canary. Verify the projection
checkpoint, lineage rows, derived labeling, and retrieval sample before enabling the scheduled
`--all` run.

**Rollback:** Disable the schedule and allow any active group build to finish. Existing community
summaries remain derived data and authoritative fact retrieval remains available. Record the last
successful projection checkpoint before retrying.

## Disaster Recovery

PostgreSQL and S3 are authoritative; the graph and all indexes are rebuildable projections
(ADR-0003). Recovery restores Postgres and S3 from backups, then rebuilds the projections
(`reprocess` for the graph; the fact projection's `rebuild_group`; the full-text indexes are
generated columns and need no rebuild). See `docs/dr-runbook.md` for the base DR procedure.
