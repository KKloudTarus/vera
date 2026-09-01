# Changelog

All notable changes to VERA are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-09-01

Multilingual knowledge: VERA now ingests and retrieves non-English content (for example
Vietnamese) alongside English.

### Changed

- **Full-text search** uses the `simple` text-search configuration instead of `english`, so
  tokens in any language index and match by their exact form (no English stemming or stopword
  filtering). Applies to the `chunks` and `facts` `search_vector` columns and every query site.
- **Entity alias normalization** is Unicode-aware and preserves diacritics: `alias_norm` is
  written by the app from `normalize_name` (not a database expression), so `Đội nền tảng`
  resolves correctly and accents stay significant (`má`, `ma`, and `mà` are distinct names).
- **Chunking** breaks sentences before an accented uppercase start, and the graph full-text
  tokenizer keeps Unicode letters, so non-English queries reach the lexical search half.

### Notes

- Cross-lingual *semantic* retrieval (query one language, find facts in another) needs a
  multilingual embedder (`openai` or `voyage`); the default deterministic embedder is a
  non-semantic hash for offline runs.
- `simple` full-text search trades English stemming for correct multilingual tokenization;
  vector retrieval covers morphological and cross-lingual matches when a real embedder is set.

## [0.1.0] - 2026-09-01

First tagged release. VERA (Verified Episodic Recall for Agents) is a shared,
verified agent-memory platform: PostgreSQL and S3 hold the source of truth, and the
Neo4j (or FalkorDB) knowledge graph is a rebuildable projection reached through the
`MemoryEngine` port.

### Added

- **Knowledge fabric**: the authoritative Fact / Assertion / Evidence model with
  bi-temporal reconciliation, conflict handling across trust tiers, deduplication, and
  rebuild-by-replay. Postgres is authoritative; graph updates flow through the outbox.
- **Ingestion worker**: idempotent, per-`group_id` serialized ingestion with a
  Postgres-native queue, structure-aware chunking, and scheduled source connectors.
- **REST API**: bounded knowledge retrieval, search, proposals, feedback, snapshots,
  community lineage, change feed, and conflict queues, with RFC 9457 problem responses.
- **MCP server** for coding agents: canonical `knowledge_*` tools plus legacy
  `memory_*` aliases, tool-level authorization classes, input bounds, per-principal
  quotas, stable structured errors, behavioral annotations, tool-visibility profiles,
  bootstrap and project discovery, and the personal-proposal lifecycle.
- **Transport hardening**: OAuth 2.1 Resource Server (RFC 9728) with required-claim
  verification, Streamable HTTP with DNS-rebinding and Host/Origin validation, stateless
  JSON transport, redacted telemetry, and a private metrics listener.
- **Agent integration GUIDE** (`docs/integrations/GUIDE.md`, v1.0.0) with Tier-1 adapter
  references (Claude Code, Cursor, OpenCode), portable skill and instruction artifacts,
  and a config-validation harness.
- **Multi-tenancy** with a shared schema plus row-level security, anchored on `group_id`.
- **Operations**: observability (tracing, metrics, cost tracking), a disaster-recovery
  runbook, deployment manifests, and a production evaluation harness.

### Notes

- Live-runtime adapter acceptance and the Phase 4 controlled rollout are tracked
  separately and are not part of this release.

[0.1.0]: https://github.com/KKloudTarus/vera/releases/tag/v0.1.0
