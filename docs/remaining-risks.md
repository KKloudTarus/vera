# Remaining-risk register

Honest status of the Knowledge Fabric after Phase 8. Phases 0 through 8 are implemented,
gated, and migration-verified; the items below are known gaps and risks to weigh before
making the fabric the primary production path.

## Cutover and wiring

- **The live ingestion worker still writes `published_episodes`.** The reconciliation and
  projection services are implemented and tested but are not yet wired into the worker, so in
  production the fabric is populated by the backfill and by proposals, not by live ingest.
  Wiring the worker to reconcile and project on ingest is the remaining cutover step; it
  should be done behind a flag and validated per group.
- **The database role split is created but not yet adopted by the app processes.** The roles
  and grants exist (migration `c9e2f3a4b5d6`) and the deployment model is documented, but the
  processes do not yet connect as or `SET ROLE` to `vera_trusted` / `vera_worker`. This is a
  deployment change, not a code change.

## Retrieval

- **Vector search is deferred.** Candidate generation is Postgres full-text over rebuildable
  `search_vector` columns behind swappable ports; a pgvector backend is not yet implemented.
  Semantic recall depends on adding it.
- **The evaluation-metric expansion is partial.** The combined retrieval is tested for
  correctness and cited output, but the golden-set metrics (nDCG, citation and temporal
  correctness datasets) from the Phase 4 plan are not yet built, so there is no CI regression
  gate on retrieval quality for the new path.

## Graph projection

- **Sagas and derived communities/summaries are not projected.** The rebuildable temporal
  fact projection and its drift check are delivered; Saga and community construction depend on
  Graphiti's LLM-driven community APIs and are deferred.

## Migration fidelity

- **Free-text episodes are not migrated.** Episodes with no structured triple are counted as
  `needs_review` rather than converted, to avoid inventing provenance. Re-extracting them into
  the fact model is future work.
- **Backfilled objects are stored as scalars.** The backfill records a triple's object as a
  scalar value, not as a resolved object entity, so entity-to-entity relationships from the
  legacy model are not reconstructed as entity objects. This is faithful for retrieval and
  citation but loses the object side of the graph edge until re-ingested.

## Contracts

- **`knowledge_feedback` reuses the legacy `memory_feedback` path** and `get_evidence` is
  served by `explain_fact`; both are slated to move onto the fact model at the same time as
  the worker cutover.

## Performance

- **No production-scale benchmark has been run here.** The harness
  (`benchmark_fabric`) is implemented and produces measured percentiles, but the target
  volumes (10k artifacts, 100k chunks, 1M assertions) must be run against a
  production-shaped database before any scalability claim.
