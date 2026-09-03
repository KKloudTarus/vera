# ADR-0003: Graphiti and Every Index Remain Non-Authoritative, Rebuildable Projections

Status: accepted (reaffirms an existing invariant)

## Context

The target architecture adds more projections (passage index, code index, communities,
summaries) and a fuller Graphiti projection. The risk is that convenience pushes truth into
a projection, for example relying on Graphiti's own LLM extraction or automatic invalidation.

## Decision

Postgres and the S3 object store stay the only authoritative stores. Graphiti, the passage
and code indexes, embeddings, communities, and summaries are projections that must be
rebuildable from Postgres and S3 alone. Concretely:

- Graphiti's LLM extraction and automatic edge invalidation are not used as sources of truth.
  VERA extracts and reconciles; the adapter projects approved Facts and Assertions.
- All lower-level Graphiti use stays inside the Graphiti adapter. Graphiti types never appear
  in the application or domain layers (enforced by import-linter).
- A `ProjectionVerifier` port checks rebuild equivalence: a rebuild from Postgres must
  reproduce the authoritative active Fact set and the indexed chunk set.
- Derived insights (communities, summaries) are marked `derived=true`, `verification=generated`,
  `authority=0` and may never satisfy a request for verified facts without evidence.

## Consequences

- A lost or corrupted graph or index is recovered by reprocess, never a data-loss event.
- Contract tests pin the Graphiti 0.29.x primitives VERA depends on, so an upgrade that
  changes them fails loudly in CI rather than silently corrupting the projection.
