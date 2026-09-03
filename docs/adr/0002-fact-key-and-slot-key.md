# ADR-0002: Derive Logical Fact Identity from Content, Not Random Ids

Status: accepted (Phase 1)

## Context

The current `dedup_uuid` on a published episode is derived per source claim, so the same
proposition arriving from two sources, or the same fact re-stated in an edited document,
produces different ids. Logical identity must be stable and content-derived so that repeated
facts deduplicate and rebuilds converge (invariant 6, working rule "do not use random claim
ids as logical fact identity").

## Decision

Derive two keys, both `sha256` hex over unit-separated, canonicalized parts:

```
fact_key = sha256(scope, subject_entity_id, predicate, normalized_object, canonical_qualifiers)
slot_key = sha256(scope, subject_entity_id, predicate, canonical_qualifiers)
```

- Qualifiers are canonicalized to a sorted, normalized JSON form before hashing, so key order
  and whitespace never change the result.
- `normalized_object` is the entity id for an entity object, or the normalized scalar for a
  scalar object.
- `fact_key` deduplicates identical propositions into one Fact.
- `slot_key` groups the values of one predicate slot (fixed subject + predicate + qualifiers)
  so a single-valued predicate can detect that a new value replaces the old one, while a
  multi-valued predicate keeps coexisting values.

Both derivations are pure functions in the domain layer (`vera.domain.knowledge.fabric`), so
they are unit-testable and identical across the app and the migration.

## Consequences

- Repeated facts reaffirm rather than duplicate; rebuilds are deterministic.
- Qualifier-aware `slot_key` prevents false contradictions such as `RUNS_ON EKS
  [env=prod]` versus `RUNS_ON ECS [env=dev]`, because the qualifier sets differ.
- Changing the hashing scheme is a pipeline-version change requiring reprocess; the scheme is
  therefore version-pinned and covered by determinism tests.
