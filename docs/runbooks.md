# Knowledge Fabric runbooks

Operational procedures for the Phase 8 cutover: migrating existing knowledge, the production
database roles, benchmarking, and rollback. These cover only implemented features.

## Migrating existing knowledge into the fact model

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

## Production database roles

Migrations run as the schema owner (or a superuser). The application never needs a superuser
at runtime (invariant 12). Three non-superuser roles, created by migration `c9e2f3a4b5d6`:

- `vera_app` (NOBYPASSRLS): the tenant path. The API `SET ROLE`s to it per request and RLS is
  enforced by `vera.group_id`.
- `vera_trusted` (BYPASSRLS, read-only): the cross-scope read models (retrieval and the
  knowledge read model), which filter `group_id` explicitly across a principal's resolved
  scopes.
- `vera_worker` (BYPASSRLS, read/write): the worker and projection paths, which write across
  groups and always filter `group_id` explicitly.

Deployment: give each process a login role that is a member of the matching role, or set the
role on connect. The RLS boundary remains the enforcing control for the tenant path; the
BYPASSRLS roles are used only by trusted server-side paths that pass explicit group filters.

## Benchmarking

Numbers must be measured, not assumed. The harness seeds a throwaway group and reports
context-pack latency percentiles in your environment, then cleans up:

```
python -m vera.entrypoints.benchmark_fabric --facts 10000 --queries 500
```

It logs `benchmark.context_pack_latency_ms` with p50/p95/p99. Run it against a
production-shaped database (data volume, hardware, concurrency) before quoting any figure.

## Disaster recovery

PostgreSQL and S3 are authoritative; the graph and all indexes are rebuildable projections
(ADR-0003). Recovery restores Postgres and S3 from backups, then rebuilds the projections
(`reprocess` for the graph; the fact projection's `rebuild_group`; the full-text indexes are
generated columns and need no rebuild). See `docs/dr-runbook.md` for the base DR procedure.
