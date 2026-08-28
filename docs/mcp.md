# Connecting an AI agent (MCP)

VERA exposes a Model Context Protocol (MCP) server: the safe, minimal surface an AI client
connects to. It is stateless streamable HTTP, so it scales behind an ordinary load balancer.
Every tool resolves the caller's scopes server-side; tools expose only reads and proposals,
never raw graph mutation.

## Endpoint

```
http://localhost:8080/mcp
```

Start it with `python -m vera.entrypoints.mcp.main`.

## Authentication

The MCP server is an OAuth 2.1 Resource Server (RFC 9728). It requires a bearer JWT on every
call; the tools have no anonymous mode. Configure it in `.env`:

```bash
VERA_MCP__JWT_SECRET=<a-long-random-secret>     # enables auth (HS256 by default)
VERA_MCP__AUTH_ISSUER=https://auth.vera.local   # expected token issuer
VERA_MCP__AUTH_AUDIENCE=https://mcp.vera.local  # this resource server's audience
# VERA_MCP__REQUIRED_SCOPES defaults to ["memory:read"]
```

An unauthenticated call returns `401` with a pointer to the protected-resource metadata:

```bash
curl -s http://localhost:8080/.well-known/oauth-protected-resource
```

In production a real authorization server issues the tokens. For local development you can
mint one with the shared secret. The token's `sub` must be a real principal id (the
`principal_id` from `/identity/register`):

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

The principal must be a member of the workspace whose facts you want to read (the principal
that created the tenancy is its owner, so it can read it). A token missing the required scope
is rejected before any tool runs.

## Connecting a client

Point an MCP client at the streamable-HTTP endpoint with the bearer token. The shape varies
by client; conceptually:

```json
{
  "mcpServers": {
    "vera": {
      "url": "http://localhost:8080/mcp",
      "headers": { "Authorization": "Bearer <jwt>" }
    }
  }
}
```

## Tools

| Tool | What it does |
|------|--------------|
| `memory_search` | Ranked verified facts in the caller's scopes, with provenance. Accepts `as_of` for point-in-time queries. |
| `memory_get_context` | The most relevant facts as context for a question. |
| `memory_explore` | Multi-hop: facts within N hops of an entity, to trace how things connect. |
| `memory_explain` | The top matches for a query with their source and verification. |
| `memory_get_source` | The provenance of one published fact. |
| `memory_recent_changes` | Recently published facts across the caller's scopes. |
| `memory_propose` | Propose a fact. It enters the caller's personal scope as an unverified proposal; it is never auto-published. |
| `memory_feedback` | Thumbs up/down on a result. Pass back the result's `signals` so the vote can calibrate ranking. |

## How an agent uses it

A typical loop: call `memory_search` (or `memory_get_context`) to ground an answer in
verified organizational memory, cite the returned `source_id` and `verification`, use
`memory_explore` when a question spans several entities, and call `memory_feedback` on
results the user accepts or rejects so ranking improves over time. When the agent learns
something new, `memory_propose` records it for a human to verify rather than writing it
straight into shared memory.
