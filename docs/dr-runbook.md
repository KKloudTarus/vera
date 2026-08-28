# Disaster recovery runbook

PostgreSQL and the S3-compatible object store are authoritative. Neo4j is a projection
that VERA can rebuild from them. This runbook covers rebuilding the graph after data
loss or corruption, and the routine backups that make it possible.

## What is authoritative

- **PostgreSQL**: tenancy, identity, curated claims, published episodes (with their
  ontology and pipeline versions), canonical entities, and the graph maps. This is the
  source of truth for what is in memory.
- **Object store (S3-compatible)**: the raw artifact bytes behind each ingested version.
- **Neo4j**: derived. Node and edge uuids are not stable across a rebuild; the
  `(group_id, canonical_entity_id)` mapping is the durable key.

## Backups (routine)

- PostgreSQL: continuous WAL archiving plus a daily base backup (PITR). Verify restores
  monthly into a scratch database.
- Object store: bucket versioning on, cross-region replication for the artifact bucket.
- Neo4j: no backup required for recovery (it is rebuilt), but a periodic dump shortens
  RTO for large graphs.

## Rebuild the graph for one group

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

### Changing the embedding model

A group's vectors must share one embedding dimension. Ingestion records the model and
dimension a group was first built with and refuses a later write under a different one
(the job dead-letters with a clear message). To adopt a new embedding model, change
`VERA_MEMORY__EMBEDDING_MODEL` / `VERA_MEMORY__EMBEDDING_DIM` for OpenAI, or
`VERA_VOYAGE__EMBEDDING_MODEL` / `VERA_VOYAGE__EMBEDDING_DIM` for Voyage, and reprocess
each affected group: the rebuild drops the old fingerprint and re-embeds every episode
under the new model.

## Full Neo4j loss

1. Stand up a fresh Neo4j and point `VERA_NEO4J__URI` at it.
2. Start one API pod so `ensure_schema` creates indexes and constraints (or run it once).
3. For each group, run the reprocess command above (script over the groups in
   `SELECT DISTINCT group_id FROM published_episodes`). Run them in parallel up to the
   worker's provider rate limits.
4. Spot-check retrieval on a sample of groups.

## Full PostgreSQL loss

Restore from PITR (base backup + WAL). Because Postgres is authoritative, this restores
identity, tenancy, and all published knowledge. Then rebuild Neo4j as above. Raw artifact
bytes remain in the object store and are re-read on demand.

## Drill

Quarterly: in staging, wipe Neo4j, run the rebuild for all groups, and confirm a fixed
set of queries returns the expected facts. The rebuild path is covered by an automated
test (`test_rebuild_reconstructs_an_equivalent_graph`); the drill validates it at scale.
