# ADR-0001: Separate logical Fact from source Assertion and Evidence

Status: accepted (Phase 1)

## Context

Today a `published_episode` is simultaneously the logical fact, the single source claim, and
its provenance. This conflation makes several required behaviors impossible: one fact
supported by several independent sources, a source that refutes a fact, withdrawing one
source's support without retracting the fact, and citing exact evidence.

## Decision

Model three distinct concepts:

- **Fact**: a normalized atomic proposition (subject entity, predicate, object, qualifiers),
  with lifecycle state, authority and confidence aggregates, and bi-temporal intervals. The
  Fact is the unit of truth.
- **Assertion**: a source-specific statement that supports or refutes a Fact, with its own
  extractor confidence, source authority, verification state, and active/withdrawn state. A
  Fact may have many Assertions from independent sources.
- **Evidence**: the exact support for an Assertion, referencing a chunk or structured record
  with an excerpt, citation URI, content hash, and coordinates.

A Fact's lifecycle is a function of its active supporting and refuting Assertions and the
predicate's ontology policy, never of a single source.

## Consequences

- Multi-source corroboration, refutation, and per-source withdrawal become first-class.
- Retrieval can cite exact evidence and annotate conflicts.
- The existing `published_episodes` remains as the graph-projection bridge and is migrated to
  Facts + Assertions + Evidence in Phase 8 (ADR-0006), not deleted.
- More tables and joins; mitigated by indexes on `fact_key`, `slot_key`, and `fact_id`.
