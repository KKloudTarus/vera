# Using VERA (HTTP API)

Reading memory over HTTP. All endpoints require a bearer credential and resolve the caller's
tenant scopes server-side, so a client never chooses a scope. The credential is a VERA API key
(`vera_<prefix>.<secret>`) or an OIDC token. On an open deployment you self-register for a key;
on a closed one an admin provisions you (see [Identity and administration](#identity-and-administration)).
Interactive docs are at `http://localhost:8000/docs`.

```bash
KEY=vera_...   # from POST /identity/register, or from an admin via POST /identity/users
```

## Search

Ranked facts across the caller's scopes, each with provenance and the signal vector behind
its score.

```bash
curl -s -X POST localhost:8000/memory/search \
  -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
  -d '{"text":"what does the payment service run on","limit":5}'
```

A hit looks like:

```json
{
  "fact": "paymentapi RUNS_ON prod-eks",
  "score": 0.82,
  "source_id": "p:...:<claim-uuid>",
  "verification": "human_verified",
  "authority": 1.0,
  "signals": {"relevance": 0.9, "authority": 1.0, "verification": 1.0,
              "recency": 0.7, "feedback": 0.5, "confidence": 1.0}
}
```

`verification` and `authority` tell you how much to trust the fact; `source_id` is the
handle for provenance and retraction.

## Point-in-Time Queries (`as_of`)

By default search returns the current view (superseded and retracted facts are hidden). Pass
`as_of` (ISO-8601) to select the valid-time boundary from the authoritative revisions currently
stored:

```bash
curl -s -X POST localhost:8000/memory/search \
  -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
  -d '{"text":"payment service environment","as_of":"2026-01-01T00:00:00Z","limit":5}'
```

Create a knowledge snapshot when retrieval must remain reproducible across later ingestion,
source mutation, supersession, or re-embedding. The snapshot copies fact, citation, chunk, and
vector inputs at one system-time boundary and pins the embedding, retrieval-index, and
assembler/scoring contracts. Persisted context packs retain the canonical request JSON as well
as its hash, so audits do not depend on reconstructing omitted defaults.

## Multi-hop Explore

From a named entity, return the facts on paths within N hops (bounded), to trace how things
connect in ways a single-fact search misses.

```bash
curl -s -X POST localhost:8000/memory/explore \
  -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
  -d '{"entity":"paymentapi","depth":2,"limit":20}'
```

`depth` is 1-3. Each connected fact comes back with its provenance.

## Retraction and Erasure

Withdraw a published source. `source_id` is the value from a search hit.

```bash
# Retract: remove from the graph, hide from search, skip on rebuild. History is kept.
curl -s -X DELETE "localhost:8000/memory/sources/<SOURCE_ID>" \
  -H "authorization: Bearer $KEY"

# Erase (GDPR): also delete raw bytes and every live or snapshotted retrieval copy.
curl -s -X DELETE "localhost:8000/memory/sources/<SOURCE_ID>?erase=true" \
  -H "authorization: Bearer $KEY"
```

Every retraction writes an audit event. The caller must be able to read the source's scope.

## Writing to Memory

There is no raw ingest endpoint on purpose: writing goes through connectors and curation, so
trust and provenance are never bypassed. To add knowledge, configure a source and connector
(see [loading knowledge](loading-knowledge.md)). Agents can *propose* a fact through the MCP
`memory_propose` tool; a proposal lands unverified in the agent's personal scope and is never
auto-published.

## Identity and Administration

`/identity/*` covers registration, organizations, workspaces, projects, memberships, service
accounts, and API-key issuance, rotation, and revocation. A workspace admin can manage keys
for members of a workspace it administers. See `http://localhost:8000/docs` for the full set.
Any authenticated principal can call `POST /identity/mcp-token` with its own API key to obtain
an intentionally non-expiring MCP JWT for itself; this does not require an admin role. This is a
temporary bootstrap contract until production OAuth is complete, and the credential-bearing
project config must remain untracked. This is a fallback for
clients or deployments without external browser OAuth. When OAuth is configured, the MCP server
validates the IdP access token and maps its OIDC identity to the same VERA principal model.

Self-service signup at `POST /identity/register` is gated by `VERA_API__REGISTRATION_OPEN`.
A shared or production deployment sets it off (the route returns `403`), and an admin provisions
each user in one call:

```bash
curl -s -X POST http://localhost:8000/identity/users \
  -H "authorization: Bearer $ADMIN_KEY" -H 'content-type: application/json' \
  -d '{"workspace_id":"<uuid>","display_name":"Bob","email":"bob@example.com","role":"member"}'
# creates the principal, adds the workspace membership, and returns a one-time api_key
```

The first admin on a closed deployment is seeded out of band by the bootstrap Job, not by
signup (see [deployment](deployment.md)). Machines use `POST /identity/service-accounts` for a
scoped key instead.
