# ADR-0006: Incremental, backward-compatible migration (no big-bang rewrite)

Status: accepted

## Context

The repository is in production use with published knowledge and a live graph projection. A
rewrite would risk data loss and break consumers. Working rules forbid a big-bang rewrite and
require preserving existing tests, APIs, and user changes.

## Decision

Evolve in additive phases. Phase 1 adds new tables and code without altering any existing
table, column, API, or MCP tool, and without rewiring the ingestion pipeline. The new
repositories are exercised only by their own tests until Phase 2 begins consuming them.

The Phase 8 data migration then bridges the old model to the new one:

- `candidate_claims` become `assertions`.
- `published_episodes` become `facts` plus supporting `assertions`/`evidence` where the
  original payload allows; the old `source_id` and graph maps are preserved.
- Rows whose provenance cannot be reconstructed with confidence are flagged for review rather
  than given invented provenance.
- Graphiti and passage projections are rebuilt and verified for count, source-link, active
  state, and search equivalence before the old paths are retired.
- A documented rollback path is kept until migration verification succeeds.

MCP and REST changes in Phase 6 are versioned; existing contracts stay until an explicit
deprecation.

## Consequences

- Every phase ends with the full quality gate green and a reversible migration.
- Two models coexist during the transition, which is the cost of not losing data or breaking
  consumers.
