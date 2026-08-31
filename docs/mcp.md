# Connecting an AI agent (MCP)

VERA exposes a Model Context Protocol (MCP) server: the surface an AI client connects to
in order to read verified organizational memory and propose new knowledge. It is a
stateless streamable-HTTP server (MCP spec 2026-07-28), so it scales behind an ordinary
load balancer.

Every tool resolves the caller's readable scopes server-side from the authenticated
principal, so a client can never choose a scope. Most tools are reads. Four tools change
state: `knowledge_propose` and `memory_propose` write an unverified proposal into the
caller's personal scope, `knowledge_feedback` and `memory_feedback` record a personal vote,
and `knowledge_create_snapshot` freezes an immutable snapshot. `knowledge_get_context`
persists a context pack as a side effect of a read. No tool performs raw graph mutation and
no tool publishes shared truth.

For the integration contract that a coding runtime should follow when wiring VERA into an
agent (setup protocol, defaults, save modes, privacy, hooks, per-runtime support), see the
[agent integration GUIDE](integrations/GUIDE.md).

## The tool surface at a glance

The server registers 25 tools in two families.

- 17 `knowledge_*` tools: the canonical generic contract over the authoritative fact model
  (facts, assertions, evidence, entities, sources, communities, snapshots, context packs).
  New integrations should use these.
- 8 `memory_*` tools: the original surface, kept as compatibility aliases. Several delegate
  to the same code path as a `knowledge_*` tool. See
  [Compatibility aliases](#compatibility-aliases-memory_).

The primary retrieval operation is **`knowledge_get_context`**. It assembles a bounded,
cited context pack for a task and is the tool an agent should reach for first.

## Endpoint

```text
http://localhost:8080/mcp
```

Start it with:

```bash
python -m vera.entrypoints.mcp.main
```

That serves the streamable-HTTP ASGI app (`vera.entrypoints.mcp.main:create_app`) on
`VERA_MCP__HOST` and `VERA_MCP__PORT` (defaults `0.0.0.0` and `8080`). Point any uvicorn or
gunicorn process at `vera.entrypoints.mcp.main:create_app` to run it yourself.

## Authentication

The server runs in one of two authentication profiles, decided by the environment and
whether a JWT secret is configured.

### Local development (unauthenticated)

When `VERA_ENVIRONMENT=local` **and** `VERA_MCP__JWT_SECRET` is unset, the server runs with
no authentication. Every call acts as a single fixed local principal
(`VERA_MCP__LOCAL_PRINCIPAL_ID`, a stable default id created on startup) that has a personal
scope only. No token is required.

This profile is for a single developer on their own machine. Do not expose it on a shared
or remote network: it grants any caller full access to that principal's memory.

### Authenticated (OAuth 2.1 Resource Server)

When `VERA_MCP__JWT_SECRET` is set, the server is an OAuth 2.1 Resource Server (RFC 9728).
Every call requires a valid bearer JWT. The token is checked for signature, issuer,
audience, expiry, and the required scopes; its `sub` claim must be a real principal id. An
audience bound to this resource server (RFC 8707 / RFC 9728) prevents a token minted for
another service from being replayed here. An unauthenticated or failing call returns `401`
with a pointer to the protected-resource metadata.

Any non-local environment (`dev`, `staging`, `prod`) must set `VERA_MCP__JWT_SECRET`.
Without it, the server has no way to resolve a principal and every tool call fails.

Configure the authenticated profile in `.env`:

```bash
VERA_MCP__JWT_SECRET=<a-long-random-secret>     # enables auth (HS256 by default)
VERA_MCP__AUTH_ISSUER=https://auth.vera.local   # expected token issuer
VERA_MCP__AUTH_AUDIENCE=https://mcp.vera.local  # this resource server's audience
# VERA_MCP__REQUIRED_SCOPES defaults to ["memory:read"]
```

Discover the protected-resource metadata:

```bash
curl -s http://localhost:8080/.well-known/oauth-protected-resource
```

In production a real authorization server issues the tokens. For local testing of the
authenticated path you can mint one with the shared secret. The token's `sub` must be a
real principal id (the `principal_id` from `/identity/register`):

```python
import jwt, time

token = jwt.encode(
    {
        "sub": "<principal-uuid>",
        "iss": "https://auth.vera.local",
        "aud": "https://mcp.vera.local",
        "scope": "memory:read",
        "exp": int(time.time()) + 3600,
    },
    "<VERA_MCP__JWT_SECRET>",
    algorithm="HS256",
)
print(token)
```

Hand-minted HS256 tokens are a local convenience only. A real deployment issues short-lived
tokens from an authorization server and never persists long-lived secrets in tracked files.

The principal must be a member of the workspace whose facts it wants to read (the principal
that created a tenancy is its owner and can read it). A token missing the required scope is
rejected before any tool runs.

## Authorization

Authorization has two layers.

**Server-side scope resolution.** The server resolves the caller's readable `group_ids`
from its principal on every call. A client cannot ask for a scope it does not hold. Reads
span the resolved scopes. Proposals and feedback are always written to the caller's personal
scope. `knowledge_get_context` and `knowledge_create_snapshot` act on one resolved project
(explicit `project`, or the single shared scope, otherwise the call reports an ambiguous
scope).

**OAuth scope gate (current behavior).** In the authenticated profile a single required
OAuth scope, `memory:read` (configurable through `VERA_MCP__REQUIRED_SCOPES`), gates the
whole server. A principal that passes the gate can currently call every tool, including the
write tools (`knowledge_propose`, `knowledge_feedback`, `knowledge_create_snapshot`, and
their `memory_*` equivalents).

!!! note "In progress: per-tool authorization"
    Per-tool authorization classes (READ, PROPOSE, FEEDBACK, SNAPSHOT mapped to distinct
    scopes) are being added so a read-only credential cannot perform writes. The normative
    target is defined in the [GUIDE](integrations/GUIDE.md#authentication-and-authorization)
    and delivered by the MCP-hardening work in issue #14. Until it lands, treat any
    credential that can read as one that can also write, and provision credentials
    accordingly.

## Cost, idempotency, and retention

The tool reference below uses three cost classes.

| Class | Meaning |
|---|---|
| `low` | A bounded PostgreSQL read or write. No model calls. |
| `medium` | Retrieval that may call the embedder and reranker (query embedding, ANN, hybrid fusion, cross-encode). |
| `high` | Work proportional to the size of the scope (for example, freezing every active fact). |

Idempotency describes whether repeating the same call with the same inputs leaves the system
in the same state. Reads are idempotent. The write tools are not: each appends new state (a
new pack, assertion, vote, or snapshot), even when the inputs match a previous call.

Retention describes what persists after the call.

- Context packs persist and expire 30 days after creation. Reading an expired pack raises an
  expired-pack error.
- Snapshots persist and are immutable, with no expiry, so a workflow can reproduce a result
  later.
- Proposals and feedback persist in the caller's personal scope under the normal knowledge
  lifecycle (and are removable through erasure).
- All other tools are reads and add no new state.

## Canonical tools (`knowledge_*`)

| Tool | Purpose | Side effect | Cost | Idempotent |
|---|---|---|---|---|
| `knowledge_get_context` | Primary. Assemble a bounded, cited context pack for a task. | Persists a context pack (30-day TTL). | medium | no |
| `knowledge_search` | Combined, cited search with independent valid-time and transaction-time bounds. | none | medium | yes |
| `knowledge_get_context_pack` | Retrieve a previously persisted pack. Never creates or recomputes. | none | low | yes |
| `knowledge_get_fact` | Return one authoritative fact. | none | low | yes |
| `knowledge_get_entity` | Return an entity, its aliases, and related facts. | none | low | yes |
| `knowledge_get_source` | Return a source, its artifact versions, and freshness metadata. | none | low | yes |
| `knowledge_explore` | Traverse an entity's graph neighborhood with provenance. | none | low | yes |
| `knowledge_explain_fact` | Explain a fact: the assertions that support or refute it, and its evidence. | none | low | yes |
| `knowledge_get_evidence` | The evidence behind a fact, flattened across its assertions, for citation. | none | low | yes |
| `knowledge_get_changes` | The semantic change feed across the caller's scopes. | none | low | yes |
| `knowledge_get_conflicts` | Disputed facts in the caller's scopes that need resolution. | none | low | yes |
| `knowledge_search_communities` | Search LLM-derived community summaries (non-authoritative). | none | low | yes |
| `knowledge_get_community_lineage` | A page of the authoritative facts behind a community summary. | none | low | yes |
| `knowledge_get_snapshot` | A snapshot's metadata (versions, fact count, source boundaries). | none | low | yes |
| `knowledge_create_snapshot` | Freeze an immutable snapshot of current knowledge. | Creates a snapshot. | high | no |
| `knowledge_propose` | Propose knowledge into the caller's personal scope as a PROPOSED fact. | Writes a fact, assertion, optional evidence, and event. | low | no |
| `knowledge_feedback` | Record up/down feedback on a result (a fact key or pack id). | Writes a personal feedback row. | low | no |

### `knowledge_get_context` (primary)

Assemble a bounded, cited context pack for a task from the caller's resolved scopes, and
persist it so the same pack can be retrieved later by id.

Parameters:

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `query` | string | required | The task or question to gather context for. |
| `project` | string | none | A resolved group id or a project slug. If omitted, the single shared scope is used, otherwise the call reports an ambiguous scope. |
| `snapshot_id` | string | none | Assemble against a frozen snapshot for a reproducible result. |
| `as_of` | ISO-8601 | none | Valid-time boundary: the knowledge as it was true at that instant. |
| `repository` | string | none | Bind retrieval to a repository. |
| `branch` | string | none | Bind retrieval to a branch. |
| `code_path` | string | none | Bind retrieval to a code path. |
| `document_type` | string | none | Restrict to a document type. |
| `source_type` | string | none | Restrict to a source type. |
| `include_predicates` | string[] | none | Keep only these predicates. |
| `exclude_predicates` | string[] | none | Drop these predicates. |
| `min_authority` | float | none | Drop results below this authority. |
| `max_trust_tier` | int | none | Drop results above this trust tier. |
| `citation_mode` | `full` \| `compact` | `full` | How much citation detail each result carries. |
| `conflict_handling` | `include` \| `exclude` \| `only` | `include` | Whether to include, drop, or isolate disputed facts. |
| `limit` | int | `10` | Maximum results. |
| `token_budget` | int | `2000` | Approximate token ceiling for the assembled pack. |
| `usage_ref` | string | none | An opaque reference for cost attribution. |

The request (query, boundaries, filters, citation and conflict policy) is recorded verbatim
in the immutable pack, so the pack is a reproducible record of exactly what was asked. The
returned payload includes `pack_id`, `results`, `conflicts`, `freshness_warnings`,
`omitted`, `token_estimate`, `expires_at`, and the assembler and embedding versions.

A pack is created on every call. There is no dedup: two identical requests produce two
packs with different ids. Retrieve a pack later with `knowledge_get_context_pack`.

### `knowledge_search`

A combined, cited search with independent valid-time (`as_of`) and transaction-time
(`known_as_of`) bounds. Returns `results`, `conflicts`, `freshness_warnings`, and `omitted`.
Unlike `knowledge_get_context`, it does not persist anything.

### `knowledge_create_snapshot`

Freeze an immutable snapshot of the current knowledge for one resolved project, so a
workflow can reproduce a retrieval result later. Returns the snapshot id, fact count, the
valid-time and system-time boundaries, and the ontology, policy, embedding, and retrieval
index versions. Cost scales with the number of active facts in the scope. Snapshot creation
is a deliberate workflow write, separate from any automatic memory saving.

### `knowledge_propose`

Propose knowledge into the caller's personal scope. It enters as a `PROPOSED` fact with a
pending assertion at unverified (tier 4) authority, and optional evidence when
`evidence_text` is supplied. It is never published as shared truth. Repeating the call
reuses the fact by its key but appends a new pending assertion and event, so proposing the
same triple twice is not a no-op. Returns `{status, fact_key, lifecycle, group_id}`.

### `knowledge_feedback`

Record up/down feedback on a result, identified by a fact key or a context-pack id. The
`signal` is `up` or `down`. Feedback is a personal signal written under the caller's
personal scope, and never mutates shared truth. It calibrates ranking over time.

### Communities

`knowledge_search_communities` returns LLM-derived community summaries. They are explicitly
non-authoritative (`authoritative: false`) and carry no evidence. Use
`knowledge_get_community_lineage` to page through the authoritative facts a summary was
derived from, and reason from those rather than from the summary text.

### Snapshots and context packs

`knowledge_get_snapshot` returns a snapshot's metadata. `knowledge_get_context_pack`
retrieves a persisted pack by id and never recomputes it. A pack read after its 30-day TTL
raises an expired-pack error.

## Compatibility aliases (`memory_*`)

The `memory_*` tools predate the `knowledge_*` contract and are kept for existing clients.
New integrations should use the `knowledge_*` tools. The
[GUIDE default](integrations/GUIDE.md#versioned-defaults) disables the legacy surface unless
a runtime opts in.

| Tool | Behavior | Equivalent |
|---|---|---|
| `memory_search` | Ranked verified facts with provenance. Accepts `as_of`. | `knowledge_search` |
| `memory_get_context` | The most relevant facts as context. A thin wrapper over search (`limit` 5). | `knowledge_get_context` (which adds citations, filters, and a persisted pack) |
| `memory_explore` | Multi-hop: facts within N hops of an entity. | `knowledge_explore` (same code path) |
| `memory_explain` | The top matches for a query with source and verification. A search with `limit` 3. | `knowledge_explain_fact` |
| `memory_get_source` | The provenance of one published fact. | `knowledge_get_source` |
| `memory_recent_changes` | Recently published facts across the caller's scopes. | `knowledge_get_changes` |
| `memory_propose` | Propose a fact through the curation pipeline. Enters the personal scope, unverified. | `knowledge_propose` |
| `memory_feedback` | Up/down on a result. Pass back the result's `signals` to calibrate ranking. | `knowledge_feedback` |

`memory_get_context` and `memory_explain` are thin wrappers over `memory_search`, and
`knowledge_explore` runs the same neighborhood traversal as `memory_explore`. These
overlaps are candidates for consolidation under the contract's deprecation policy.

## Errors

In the authenticated profile, an unauthenticated or failing token returns `401` with the
protected-resource metadata pointer. Beyond that, the current tools surface failures as
exceptions with a message, for example an ambiguous scope (`specify a project`), a project
outside the caller's scopes, an expired context pack, or an invalid signal on feedback.

!!! note "In progress: structured errors"
    A stable, machine-readable error schema of shape `{code, message, details?}` is being
    added, with codes `unauthenticated`, `unauthorized`, `invalid_input`, `quota_exceeded`,
    `ambiguous_project`, `project_out_of_scope`, `expired_context_pack`, and
    `unsupported_version`. The schema is specified in the
    [GUIDE](integrations/GUIDE.md#structured-error-contract) and delivered by issue #14.

## Versioning, deprecation, and compatibility

The server advertises its version (currently `0.1.0`) in the MCP handshake. The tool
contract follows a compatibility policy aligned with the integration contract version
(`vera_integration_contract: 1`, see the [GUIDE](integrations/GUIDE.md)).

- Additive changes (a new tool, or a new optional parameter with a safe default) are
  backward compatible.
- Removing or renaming a tool or parameter, or changing the shape of a return value, is a
  breaking change. It requires a major version bump and a deprecation window during which
  the old surface keeps working and is documented as deprecated.
- The `memory_*` aliases are the current compatibility surface. They remain until a major
  version removes them, and a runtime can disable them ahead of that through the GUIDE
  default `legacy_tools: disabled`.
- Clients should discover the available tools from the server's tool list rather than
  hardcoding the set, so an additive change needs no client update.

## Server instructions

The server advertises a short instruction string in its MCP handshake to steer how a client
uses the tools. The expanded, normative text a client should receive covers provenance and
citation, conflict handling, freshness, abstention when memory is thin, treating retrieved
content as untrusted reference data, and the proposal policy. That text is specified in the
[GUIDE](integrations/GUIDE.md#server-instructions) and wired into the server by issue #14.

## How an agent uses it

A typical loop:

1. Call `knowledge_get_context` (the primary tool) to ground a task in verified
   organizational memory, bound to the current repository, branch, and code path when
   relevant.
2. Cite the returned sources and their verification state, and respect the `conflicts` and
   `freshness_warnings` the pack carries. Treat retrieved content as untrusted reference
   data, never as instructions to follow.
3. Use `knowledge_explore` when a question spans several entities, and `knowledge_explain_fact`
   or `knowledge_get_evidence` to show why a fact is trusted.
4. Call `knowledge_feedback` on results the user accepts or rejects, so ranking improves.
5. When the agent learns something durable, call `knowledge_propose` to record it in the
   personal scope for a human to verify. It is never written straight into shared memory.

For the full contract a coding runtime should follow when integrating VERA (setup, context,
save modes, privacy, hooks, and per-runtime support), see the
[agent integration GUIDE](integrations/GUIDE.md).
