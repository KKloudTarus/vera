# ADR-0004: Append-only KnowledgeEvent Ledger and Immutable Revisions

Status: accepted (Phase 1)

## Context

Invariant 6 forbids overwriting knowledge history. The current model marks an episode
`invalid_at` or `retracted_at` in place, which loses the sequence of semantic changes and
cannot answer "what changed, when, why, and by whom".

## Decision

Add an append-only `knowledge_events` ledger, range-partitioned by `occurred_at` monthly
(the same pattern as `audit_events`, so retention is a partition drop). Every reconciliation,
lifecycle transition, entity merge/split, ontology change, snapshot, and context-pack
creation appends an event carrying actor/process, source, subject ids, previous and next
state, reason, policy version, trace id, and timestamp.

Fact history is represented by immutable revisions plus relations, never in-place edits:
supersession creates a new Fact revision and links the old one with a `SUPERSEDES`
`fact_relation`, moving the old revision to `superseded`. Assertion withdrawal sets
`state=withdrawn` and appends `ASSERTION_WITHDRAWN`; it does not delete the row.

The ledger feeds the semantic change projection. The `facts`, `assertions`, and `evidence`
tables hold authoritative Fact state.

## Implementation Status

The event ledger is append-only. Current lifecycle and aggregate fields are updated in place,
so the ledger lacks enough row data to reconstruct arbitrary earlier system states. Knowledge
snapshots provide reproducibility from their creation boundary by copying fact, provenance,
chunk, and vector retrieval inputs. Full immutable Fact revisions remain future work.

## Consequences

- Full auditability and a semantic change feed for the Workbench.
- Reproducible retrieval from an explicitly created snapshot boundary.
- Write amplification on hot facts; bounded by monthly partitioning and by emitting one event
  per semantic transition, not per row touch.
