# ADR-0007: Agent Integration Operational Records

## Status

Accepted.

## Context

Agent integration produces several kinds of activity: authentication and authorization,
bootstrap and retrieval, durable context packs and snapshots, personal proposals and
feedback, and client-side setup or hooks. Duplicating every event into `audit_events` would
create a high-volume shadow ledger, retain sensitive request data unnecessarily, and let
invalid-token traffic amplify database writes.

## Decision

Each action has one authoritative operational record:

| Action | Authoritative record |
|---|---|
| Invalid authentication or insufficient tool scope | Redacted transport log and bounded request/error telemetry. No database audit row because no trusted actor or tenant is available. |
| Bootstrap, ephemeral context, ordinary reads, proposal reports | `vera_mcp_tool_calls_total`, `vera_mcp_tool_duration_seconds`, and the `vera.mcp.tool` span when tracing is enabled. Query text, repository paths, prompts, and transcripts are not telemetry labels, span attributes, or audit payloads. |
| Persisted context pack and snapshot creation | Append-only `knowledge_events` entries created transactionally with the durable record. |
| Proposal create/deduplicate/conflict/skip/reject | `proposal_attempts`; created assertions also append `knowledge_events`. |
| Proposal self-retract | Append-only assertion-withdrawn and fact-retracted `knowledge_events`. |
| Exact-attribution feedback | The unique personal `retrieval_feedback` row. |
| Source retraction or erasure | `audit_events`, because it is an administrative operation outside the agent proposal lifecycle. |
| Setup, config update/uninstall, and hooks | Runtime-owned project configuration and local hook behavior. VERA cannot claim or audit an action it did not observe. |

`audit_events` remains the operational security log for server-observed administrative
actions that have no more specific append-only ledger. Knowledge lifecycle changes are not
duplicated there. If VERA later ships a setup helper or hook receiver, that component must
define bounded audit events before release.

All logs, telemetry, and records use stable action/outcome identifiers. They must not store
bearer tokens, credentials, raw prompts, transcripts, source content, or unsanitized local
paths.

## Consequences

- State transitions and their audit evidence commit together in their existing ledgers.
- Authentication floods cannot force an `audit_events` write per invalid request.
- Setup success remains a runtime acceptance claim, not a server inference from a config
  file or handshake.
- A future helper or hook implementation requires a follow-up audit decision for its newly
  observable actions.
