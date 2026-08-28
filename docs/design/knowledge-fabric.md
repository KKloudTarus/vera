# Knowledge Fabric evolution: gap analysis, model, and plan

This document is the Phase 0 deliverable for evolving VERA from a verified agent-memory
service into a governed, temporal Knowledge Fabric. It records where the current code
stands against the target architecture, the authoritative model being introduced, and the
phased plan. It is descriptive of decisions; the rationale for each major choice is an ADR
under `docs/adr/`.

## 1. Gap analysis (current vs target)

The current repository already satisfies most structural invariants. The gaps are in the
knowledge model's granularity, not in the architecture.

| Area | Current state | Target | Gap |
|------|---------------|--------|-----|
| Authority of storage | Postgres + S3 authoritative; graph rebuildable | same | none (invariant already held) |
| Clean architecture | enforced by import-linter (2 contracts) | same | none |
| Tenancy | `group_id` + RLS `tenant_isolation`; scopes server-resolved | same | none for existing tables; new tables must join RLS |
| Provenance | `published_episodes` carries authority, verification, ontology_version, pipeline, bi-temporal `invalid_at` | per-fact provenance split into Fact/Assertion/Evidence | logical fact not separated from source claim or evidence |
| Logical facts | a published episode is both the claim and the "fact"; identity is `dedup_uuid` | Fact with derived `fact_key`/`slot_key`, many Assertions, exact Evidence | **primary gap** |
| Multi-source support | one episode per (group, source); no notion of several sources supporting one fact | many Assertions per Fact; supports/refutes polarity | missing |
| Chunking | none; body stored whole in S3, extraction over full text | structure-aware Chunk with coordinates | missing |
| Change history | supersession invalidates an episode; retraction marks retracted | append-only KnowledgeEvent ledger; immutable revisions | ledger missing |
| Ontology | Pydantic entity/edge classes + `SINGLE_VALUED_PREDICATES` set; `ONTOLOGY_VERSION=1` | versioned predicate governance (cardinality, qualifiers, authoritative sources, conflict strategy, absence semantics, TTL) | governance fields missing (Phase 6/2) |
| Reconciliation | functional-predicate supersession + LLM contradiction judge | deterministic assertion diff, evidence withdrawal, ontology-driven fact transitions | assertion-level diff missing (Phase 2) |
| Retrieval | 3-stage edge search (RRF -> blend -> reranker); FalkorDB hybrid | parallel candidate generation over facts/entities/episodes/chunks/code/communities/events | passage/code candidates missing (Phase 4) |
| Snapshots / ContextPacks | none | immutable snapshots; token-bounded packs | missing (Phase 5) |
| MCP | 8 read/propose tools | generic `knowledge_*` tools, `knowledge_get_context` primary | rename/extend under version (Phase 6) |
| Graphiti use | edge hybrid search + triplet ingest + retract; ACL in adapter | full projection: episodes, typed nodes, temporal fact edges, sagas, communities; contract tests | projection breadth + contract tests (Phase 3) |
| DB roles | `vera_app` non-superuser + RLS | separate migration owner / app / trusted-read / worker roles | role split (Phase 7) |
| Resilience | outbox, per-group lanes, breaker/limiter/retry, durable erasure cleanup | same + fabric metrics | metrics additions (later phases) |

Nothing in the current code has to be discarded. `published_episodes`, `graph_edge_map`,
`graph_node_map`, and `candidate_claims` remain and are bridged to the new model by the
Phase 8 migration (ADR-0006).

## 2. Authoritative domain model (Phase 1 scope)

Phase 1 introduces six persisted concepts and their pure-domain counterparts. All are
tenant-scoped by `group_id` and protected by the same `tenant_isolation` RLS policy as the
existing knowledge tables.

### Identity derivation (pure, deterministic)

```
fact_key = sha256(scope | subject_entity_id | predicate | normalized_object | canonical_qualifiers)
slot_key = sha256(scope | subject_entity_id | predicate | canonical_qualifiers)
```

`fact_key` deduplicates identical propositions: the same proposition from two sources maps
to one Fact with two Assertions. `slot_key` groups the values of one predicate slot so a
single-valued predicate (`RUNS_ON` under a fixed qualifier set) can detect replacement.
Qualifiers are canonicalized (sorted keys, normalized values) before hashing so ordering
never changes the key. See ADR-0002.

### Tables

- **chunks** — a citable, retrieval-sized piece of an `artifact_version`. Carries
  `chunk_key` (deterministic: hash of artifact_version_id, ordinal, content_hash), ordinal,
  heading path, char offsets, page number, code `symbol_name`/`start_line`/`end_line`,
  content hash, token count, `parent_chunk_id`, and the chunk `text` (so the passage index
  is rebuildable from Postgres). Unique on `(artifact_version_id, ordinal)` and `chunk_key`.
- **facts** — one row per fact revision. Carries `fact_key`, `slot_key`, `subject_entity_id`,
  `predicate`, object as either `object_entity_id` or `object_scalar` (+ `object_type`
  discriminator and `normalized_object`), `qualifiers`, `lifecycle_state`
  (proposed/active/disputed/superseded/retracted/expired), `authority`, `confidence`,
  valid-time (`valid_from`/`valid_to`), system-time (`system_from`/`system_to`),
  `ontology_version_id`, and event references. A partial unique index enforces at most one
  **active** fact per `(group_id, fact_key)`.
- **assertions** — a source-specific statement supporting or refuting a Fact. Carries
  `fact_id`, source/artifact/version references, `polarity` (supports/refutes),
  `extractor_confidence`, `source_authority`, `verification_state`, valid-time, observed and
  recorded times, `extraction_run_id`, and `state` (active/withdrawn). Unique on
  `(fact_id, artifact_version_id, polarity)` so re-ingest reaffirms rather than duplicates.
- **evidence** — exact support for an Assertion: `chunk_id` and/or `structured_record`,
  `excerpt`, `citation_uri`, `content_hash`, `source_coordinates`, `confidentiality`. Unique
  on `(assertion_id, content_hash)`.
- **fact_relations** — typed edges between facts: SUPERSEDES, CONTRADICTS, REFINES,
  DUPLICATES, DERIVED_FROM, RELATED_TO. Unique on `(from_fact_id, to_fact_id, relation_type)`.
- **knowledge_events** — append-only ledger, range-partitioned by `occurred_at` (monthly),
  mirroring `audit_events`. Carries `event_type`, actor, source, subject ids (fact,
  assertion, artifact, entity), `previous_state`/`next_state` (JSONB), reason, policy
  version, trace id. No cross-partition FKs; ids are plain UUIDs by design.

See ADR-0001 (Fact/Assertion/Evidence separation), ADR-0004 (append-only ledger and
immutable revisions), ADR-0005 (chunking in VERA).

## 3. Phased plan, dependencies, and risks

Phases are sequential where they share schema; independent within a phase.

| Phase | Content | Depends on | Primary risk | Mitigation |
|-------|---------|-----------|--------------|-----------|
| 0 | audit, ADRs, plan, characterization tests | current gate green | missing current behavior | characterization tests pin ingest/search/RLS/retract |
| 1 | Chunk/Fact/Assertion/Evidence/FactRelation/KnowledgeEvent schema, domain, ports, adapters, migration | 0 | schema churn later | additive only; no change to existing tables; reversible migration |
| 2 | structure-aware chunking, assertion diff, ontology-driven reconciliation, events | 1 | false contradictions | qualifier-aware slot_key; ontology conflict strategy |
| 3 | full Graphiti projection + contract + rebuild-equivalence tests | 1 | Graphiti API drift | pin 0.29.x; contract tests; all use inside adapter |
| 4 | PassageIndex/CodeIndex ports, parallel candidate retrieval | 1,2 | retrieval regression | golden-set thresholds from measured baseline |
| 5 | KnowledgeSnapshot, ContextAssembler, ContextPack | 1-4 | snapshot reproducibility | as-of system-time filter; snapshot records revisions |
| 6 | generic MCP `knowledge_*` + REST, versioned | 1-5 | consumer breakage | keep old tools until deprecation; version the surface |
| 7 | review/conflict/timeline endpoints; DB role split; destructive authz | 1-6 | production role breakage | new roles behind migration; tests under real roles |
| 8 | data migration, benchmarks, docs, runbooks | 1-7 | data loss | uncertain rows flagged for review; documented rollback |

Delivered so far: Phase 0 (this document, the ADRs, characterization tests), Phase 1 (the
authoritative model, migration `f3a1b2c4d5e6`), Phase 2 (chunking, ontology-driven
reconciliation, change events), Phase 3 (fact projection into Graphiti, the Graphiti 0.29.x
compatibility contract tests, and rebuild verification), and Phase 4 (Postgres full-text
passage/code/fact candidate sources over rebuildable `search_vector` columns, migration
`a7c9e1f2b3d4`, and a `ContextAssembler` that fans out to them in parallel, dedups, scores
with an explainable signal vector, applies source-diversity, annotates conflicts, cites
every hit, and packs to a token budget with no LLM on the path), Phase 5 (immutable
`KnowledgeSnapshot`s that freeze the active fact revisions with the ontology/policy versions
and source boundaries, migration `b8d0f1a2c3e4`, snapshot-scoped retrieval that stays
reproducible after supersession, and persisted `ContextPack`s carrying the cited results and
the conflict/freshness counts, both appending to the change ledger), and Phase 6 (a
`KnowledgeService` behind a versioned REST surface `/v2/knowledge` and generic `knowledge_*`
MCP tools with `knowledge_get_context` as the primary entry point: context, search,
get_fact, explain_fact, changes, conflicts, snapshots, and propose, all with scopes resolved
server-side and proposals confined to the caller's personal scope; the existing `/memory`
endpoints and `memory_*` tools remain for backward compatibility). Within Phase 6,
`knowledge_feedback` reuses the existing `memory_feedback` path and `get_evidence` is served
by `explain_fact`; migrating them to the new model follows the Phase 8 cutover. Phase 7 adds
governance and the production role model: destructive retraction now requires an admin role
(a `role_for`/`can_administer` resolver, not mere read access); the review queue, fact
timeline, promote/reject (admin-gated), and ontology-policy endpoints back a future Knowledge
Workbench; and migration `c9e2f3a4b5d6` creates the non-superuser `vera_trusted` (read) and
`vera_worker` (read/write) BYPASSRLS roles so the cross-scope read and worker paths need no
superuser in production, with `vera_app` staying the RLS-enforced tenant path. Phase 8 adds
the cutover tooling: an idempotent backfill (`migrate_to_fabric`) that converts
`published_episodes` into Facts/Assertions/Evidence while preserving the legacy rows and
flagging free-text episodes for review, a benchmark harness (`benchmark_fabric`) that reports
measured context-pack latency percentiles, and the runbook and remaining-risk register
(`docs/runbooks.md`, `docs/remaining-risks.md`). The live worker cutover and the remaining
deferrals are tracked in the risk register. Within Phase 3, projecting
Sagas and building derived communities/summaries is deferred: those depend on Graphiti's
LLM-driven community APIs, whereas the invariant that matters here, a graph rebuildable from
Postgres, is delivered and verified by `FactProjectionService` and its drift check. Within
Phase 4, a vector backend (pgvector) and the full evaluation-metric expansion (nDCG, citation
and temporal correctness datasets) are deferred behind the swappable ports; the combined,
cited, diversity-aware retrieval and its scoring are delivered and tested. The reconciliation,
projection, and retrieval services are not yet wired into the live worker or the MCP/REST
surface; that wiring lands with Phase 6 (contracts) and the Phase 8 migration.

Cross-cutting risks: (a) Graphiti making itself authoritative is prevented by keeping it a
projection and adding rebuild-equivalence tests (invariant 10); (b) agents mutating shared
truth is prevented by keeping `knowledge_propose` proposal-only in personal scope
(invariant 5); (c) generated summaries masquerading as facts is prevented by `derived=true`,
`authority=0` marking (invariant 11).

## 4. Backward compatibility

Phase 1 is strictly additive: no existing table, column, API, or MCP tool changes. The new
tables coexist with `published_episodes` and the graph maps. The ingestion pipeline is not
rewired in Phase 1; the new repositories are exercised only by their own tests until Phase 2
begins using them. This keeps every existing test green and the migration reversible.
