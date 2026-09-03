# Remaining-Risk Register

Honest status after the knowledge-lifecycle work in issue #6. Exact provenance,
deterministic reconciliation, authoritative-first writes, projection cleanup, hybrid
retrieval, reproducible snapshots, the agent MCP contract, Confluence lifecycle handling,
ontology governance, and governed community lineage are implemented and tested. The items
below remain operational or migration risks; none is a substitute for a production readiness
review.

## Cutover and Deployment

- **Authoritative Fabric writes remain an explicit rollout decision.**
  `VERA_MEMORY__FABRIC_WRITE_MODE` supports `legacy`, `dual`, and `fabric`, and defaults to
  `legacy`. `fabric` makes PostgreSQL facts authoritative and drives graph projection only
  from committed outbox work. Production rollout still needs per-scope verification, queue
  monitoring, and a tested rollback plan.
- **Database role enforcement remains deployment-gated.** The read and worker paths assume
  `vera_trusted` and `vera_worker` with per-transaction `SET LOCAL ROLE` when
  `VERA_DB__ROLE_ENFORCEMENT=true`. The login role must be granted those roles before the
  setting is enabled. Cross-tenant security testing belongs to the production-readiness
  follow-up.

## Retrieval

- **Vector retrieval remains provider- and rollout-dependent.** When
  `VERA_MEMORY__VECTOR_SEARCH_ENABLED=true`, full-text and vector candidates are fused rather
  than selected as alternatives. Embeddings are versioned per chunk, provider, model, and
  model version, and new chunks are embedded on the live path. Existing chunks still require
  an operator backfill for the selected model, and production must supply provider credentials,
  validate dimensions, and monitor embedding failures.
- **Old vector snapshots require their pinned query embedder.** Snapshot rows retain exact
  chunk fields and vectors. Replaying vector ranking also requires the provider, model, and
  model version recorded by the snapshot to remain configured. Vera fails closed when the
  active retrieval implementation differs from that pin.
- **Assembler contracts require an explicit version bump.** A snapshot pins the assembler
  version, which covers its scoring algorithm, default weights, diversification, and packing.
  Changing any of those without bumping the version would make replay claims inaccurate.
- **The committed golden set covers deterministic fixtures at test scale.** It gates
  hit@k, MRR, nDCG@k, and citation rate against deterministic fixtures. Domain coverage,
  latency, and ranking quality still need measurement with production-shaped data and traffic.

## Graph and Communities

- **Community construction remains an operator-run, potentially expensive step.** Each run
  rebuilds the active fact projection from PostgreSQL, records normalized lineage in
  `community_fact_lineage`, and publishes summaries marked `derived`; authoritative fact
  search never presents those summaries as evidence. Scheduling, LLM cost controls, and
  freshness targets remain deployment concerns. Saga construction remains deferred.
- **The graph is a rebuildable projection.** PostgreSQL and S3 remain the recovery sources.
  Projection drift is
  detectable and stale facts are removed incrementally, but operations must alert on queue lag
  and drift and retain a tested rebuild procedure.

## Historical Migration

- **Free-text legacy episodes remain queued for controlled re-extraction.** Episodes without structured
  triples are counted as `needs_review` rather than assigned invented evidence. Recovering them
  requires controlled re-extraction from immutable source content.
- **Backfilled legacy objects remain scalar values.** The backfill does not reconstruct the
  object side of historical entity-to-entity graph edges. Re-ingestion is required where that
  relationship structure matters.

## Production Operations

- **No production-scale benchmark or recovery drill has been run here.** The
  `benchmark_fabric` harness reports context-pack latency percentiles, but target volumes,
  concurrency, backup/restore, retention, observability, and incident recovery require a
  production-shaped environment.
- **Production security is a separate gate.** MCP authorization, tenant-isolation adversarial
  tests, data-poisoning and prompt-injection defenses, and operational abuse controls are not
  claimed by issue #6.

These production concerns are tracked in the dependent
[production operations and security readiness EPIC](https://github.com/KKloudTarus/vera/issues/9).
