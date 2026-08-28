# Remaining-risk register

Honest status of the Knowledge Fabric after Phase 8. Phases 0 through 8 are implemented,
gated, and migration-verified; the items below are known gaps and risks to weigh before
making the fabric the primary production path.

## Cutover and wiring

- **The worker fabric cutover is available behind a flag, off by default.** With
  `VERA_MEMORY__FABRIC_ENABLED=true` the ingestion worker also reconciles each episode's
  triples into the fact store (idempotent on replay by the episode's extraction run id), so
  the `/v2` knowledge surface reflects live ingest; the legacy published-episode path is
  unchanged. It is off by default: turning it on in production, validated per group, is the
  rollout decision. The graph is still projected by the episode path; switching the graph to
  the fact-based projection is a further step.
- **The database role split is created but not yet adopted by the app processes.** The roles
  and grants exist (migration `c9e2f3a4b5d6`) and the deployment model is documented, but the
  processes do not yet connect as or `SET ROLE` to `vera_trusted` / `vera_worker`. This is a
  deployment change, not a code change.

## Retrieval

- **Vector search is deferred.** Candidate generation is Postgres full-text over rebuildable
  `search_vector` columns behind swappable ports; a pgvector backend is not yet implemented.
  Semantic recall depends on adding it.
- **A committed golden set gates retrieval quality in CI.** `datasets/retrieval/golden.json`
  seeds a fixed set of entities, facts, and passages; `tests/integration/test_retrieval_golden.py`
  runs the real ContextAssembler over it and fails the build if hit@k, nDCG@k, or the citation
  rate drop below the thresholds in the file (currently hit 1.0, nDCG 0.85, citation 1.0; the
  seeded set measures nDCG 0.97). The metrics live in `application/queries/retrieval_eval.py`
  (hit@k, MRR, nDCG@k, citation rate) and the `retrieval_eval` CLI reports them against a live
  database. A production-scale relevance benchmark at target volumes is still a run-time action.

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

- **`knowledge_feedback` and `get_evidence` are first-class on the fact model.** `get_evidence`
  is a dedicated read (`GET /v2/knowledge/facts/{fact_key}/evidence` and the
  `knowledge_get_evidence` MCP tool) returning a fact's evidence flattened across its active
  assertions, distinct from `explain_fact`. `knowledge_feedback`
  (`POST /v2/knowledge/feedback` and the MCP tool) records up/down feedback keyed to a result
  ref (a fact_key or context-pack id) into the caller's personal scope, so agent feedback
  never mutates shared truth.

## Performance

- **No production-scale benchmark has been run here.** The harness
  (`benchmark_fabric`) is implemented and produces measured percentiles, but the target
  volumes (10k artifacts, 100k chunks, 1M assertions) must be run against a
  production-shaped database before any scalability claim.
