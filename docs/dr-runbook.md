# Disaster Recovery Runbook

PostgreSQL and the S3-compatible object store are authoritative. Neo4j is a projection
that VERA can rebuild from them. This runbook covers rebuilding the graph after data
loss or corruption, and the routine backups that make it possible.

## What Is Authoritative

- **PostgreSQL**: tenancy, identity, curated claims, published episodes (with their
  ontology and pipeline versions), canonical entities, and the graph maps. This is the
  source of truth for what is in memory.
- **Object store (S3-compatible)**: the raw artifact bytes behind each ingested version.
- **Neo4j**: derived. Node and edge uuids are not stable across a rebuild; the
  `(group_id, canonical_entity_id)` mapping is the durable key.

## Incident ownership

Assign all four roles at declaration. The incident commander owns severity, timeline,
communications, and closure. Operations owns dependency isolation, restore, and runtime changes.
Security owns credential containment, evidence access, and exposure assessment. The application
owner owns VERA behavior, authoritative parity, and functional recovery. Every handoff and
follow-up receives a named owner and deadline.

### postgres_unavailable

**Owner:** operations

1. The incident commander declares a write outage, freezes rollouts, and assigns the four roles.
   Operations stops workers if retries are increasing load and confirms `/health/ready` reports
   the database down.
2. Operations restores connectivity or fails over using the database platform's tested process.
   If storage integrity is uncertain, restore the latest base backup plus WAL to the approved
   point in time. Never reconstruct PostgreSQL from the graph.
3. The application owner verifies readiness, migration version, authoritative counts and
   checksums, queue state, and a fixed read/write sample. Security preserves access evidence and
   checks whether the outage involved unauthorized access before the commander restores traffic.

### object_store_unavailable

**Owner:** operations

1. The incident commander freezes new ingestion and erasure work. Operations confirms the
   S3-compatible endpoint, bucket, and credential path without exposing secrets.
2. Restore service or fail over to a replica that has complete version history. If objects were
   lost, restore object versions from the last verified replica or backup. Do not publish a
   PostgreSQL artifact version whose raw object is absent.
3. The application owner compares PostgreSQL artifact keys with restored objects, retries the
   idempotent failed ingest, and verifies re-extraction of a sample. Security reviews object
   access logs before the incident commander resumes connectors.

### graph_corruption

**Owner:** application owner

1. The incident commander pauses graph-dependent rollout and community builds. Operations
   isolates the graph and records the affected groups; PostgreSQL and object-store writes remain
   the recovery authority.
2. The application owner runs `python -m vera.entrypoints.reprocess <group_id>` for isolated
   groups. For full loss, follow [Full Neo4j loss](#full-neo4j-loss).
3. Require zero projection drift, successful built-in reprocess verification, and known-fact
   searches for a fixed group sample. Security preserves graph access logs, then the incident
   commander restores graph-dependent traffic.

### queue_stall

**Owner:** operations

1. Freeze rollouts. Operations checks worker liveness, pending/in-flight/dead counts, oldest job
   age, visibility timeouts, database health, and provider limits. The application owner traces
   one oldest job to its first failing boundary.
2. Restore the dependency or worker capacity. Allow automatic stuck-job reclamation and
   idempotent retry to recover work. Do not skip per-group ordering or modify queue rows manually.
3. The application owner verifies the queue drains in order, no new dead jobs appear, projection
   drift is zero, and a queued canary becomes searchable. Security reviews access anomalies if
   the stall followed a credential change; the commander records recovery time and follow-ups.

### credential_compromise

**Owner:** security

1. The incident commander restricts incident evidence and freezes rollouts. Security revokes the
   exposed API key or token, disables the affected principal when needed, and identifies every
   system that accepted the credential.
2. Operations rotates affected database, object-store, graph, identity, and provider credentials,
   then restarts consumers through the normal secret-delivery path. Never place replacement
   values in logs, tickets, or command history.
3. Security reviews audit and dependency access logs from first possible exposure through
   revocation. The application owner verifies authorization, readiness, writes, retrieval, queue
   progress, and authoritative parity. The commander restores access only after containment and
   assigns remediation owners.

## Backups (routine)

- PostgreSQL: continuous WAL archiving plus a daily base backup (PITR). Verify restores
  monthly into a scratch database.
- Object store: bucket versioning on, cross-region replication for the artifact bucket.
- Neo4j: no backup required for recovery (it is rebuilt), but a periodic dump shortens
  RTO for large graphs.

## Rebuild the Graph for One Group

Used after a graph wipe, an ontology change, or corruption in a single tenant.

```bash
python -m vera.entrypoints.reprocess <group_id>
```

This clears the group's graph, its graph maps, and its embedding fingerprint, then
replays its published episodes in reference-time order through the normal ingestion path.
Canonical entities resolve by name and are kept. The result is an equivalent graph: the
same facts, retrievable the same way.

The command runs an automated check after the replay: it counts the group's published
episodes against the rebuilt node and edge maps and fails (non-zero exit, a
`reprocess.verify_failed` log) if a group that had facts came back with an empty graph
projection. On success it logs `reprocess.verified` with the counts. Still spot-check a
search for a known fact.

### Changing the Embedding Model

A group's vectors must share one embedding dimension. Ingestion records the model and
dimension a group was first built with and refuses a later write under a different one
(the job dead-letters with a clear message). To adopt a new embedding model, change
`VERA_MEMORY__EMBEDDING_MODEL` / `VERA_MEMORY__EMBEDDING_DIM` for OpenAI, or
`VERA_VOYAGE__EMBEDDING_MODEL` / `VERA_VOYAGE__EMBEDDING_DIM` for Voyage, and reprocess
each affected group: the rebuild drops the old fingerprint and re-embeds every episode
under the new model.

## Full Neo4j Loss

1. Stand up a fresh Neo4j and point `VERA_NEO4J__URI` at it.
2. Start one API pod so `ensure_schema` creates indexes and constraints (or run it once).
3. For each group, run the reprocess command above (script over the groups in
   `SELECT DISTINCT group_id FROM published_episodes`). Run them in parallel up to the
   worker's provider rate limits.
4. Spot-check retrieval on a sample of groups.

## Full PostgreSQL Loss

Restore from PITR (base backup + WAL). Because Postgres is authoritative, this restores
identity, tenancy, and all published knowledge. Then rebuild Neo4j as above. Raw artifact
bytes remain in the object store and are re-read on demand.

## Drill

Quarterly: in staging, wipe Neo4j, run the rebuild for all groups, and confirm a fixed
set of queries returns the expected facts. The rebuild path is covered by an automated
test (`test_rebuild_reconstructs_an_equivalent_graph`); the drill validates it at scale.
