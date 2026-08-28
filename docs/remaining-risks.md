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
  rollout decision. When fabric is on, reconciliation enqueues an outbox `project_facts` job
  (coalesced per group) and the worker projects the group's active facts into the graph as
  RELATES_TO edges downstream of the fact store, so graph mutation is outbox-driven and
  rebuildable (gap 8), alongside the existing episode projection.
- **The database role split is adopted by the read and worker paths when enabled.** The roles
  and grants exist (migration `c9e2f3a4b5d6`); with `VERA_DB__ROLE_ENFORCEMENT=true` the read
  path assumes `vera_trusted` and the worker path `vera_worker` via a per-transaction
  SET LOCAL ROLE (gap 16). It is off by default because the login role must be a member of
  those roles (or a superuser) for the SET to succeed; enabling it in production is a
  deployment decision.

## Retrieval

- **pgvector passage search is available, gated.** Full-text over rebuildable `search_vector`
  columns is the default; with `VERA_MEMORY__VECTOR_SEARCH_ENABLED=true` and an embedder, the
  passage and code candidate sources use approximate nearest-neighbor search over a
  `chunks.embedding vector(1024)` HNSW index behind the same PassageIndex/CodeIndex ports (gap
  11). The column and index are added conditionally (migration `e2b3c4d5f6a7`, a no-op on a
  stock postgres image; the `pgvector/pgvector:pg18` compose image ships the extension), and
  `backfill_chunk_embeddings` populates embeddings per group. The embedding dimension is frozen
  at 1024 and must match the embedder.
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
