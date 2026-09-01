# Changelog

All notable changes to VERA are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
