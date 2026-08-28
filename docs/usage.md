# Using VERA (HTTP API)

Reading memory over HTTP. All endpoints require a bearer credential (an API key from
`/identity/register`, or an OIDC token) and resolve the caller's tenant scopes server-side,
so a client never chooses a scope. Interactive docs are at `http://localhost:8000/docs`.

```bash
KEY=vera_...   # from POST /identity/register
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

## Point-in-time queries (`as_of`)

By default search returns the current view (superseded and retracted facts are hidden). Pass
`as_of` (ISO-8601) to ask what memory held at that instant:

```bash
curl -s -X POST localhost:8000/memory/search \
  -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
  -d '{"text":"payment service environment","as_of":"2026-01-01T00:00:00Z","limit":5}'
```

This works because facts are bi-temporal: superseding a fact invalidates the old one rather
than deleting it, so history stays queryable.

## Multi-hop explore

From a named entity, return the facts on paths within N hops (bounded), to trace how things
connect in ways a single-fact search misses.

```bash
curl -s -X POST localhost:8000/memory/explore \
  -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
  -d '{"entity":"paymentapi","depth":2,"limit":20}'
```

`depth` is 1-3. Each connected fact comes back with its provenance.

## Retraction and erasure

Withdraw a published source. `source_id` is the value from a search hit.

```bash
# Retract: remove from the graph, hide from search, skip on rebuild. History is kept.
curl -s -X DELETE "localhost:8000/memory/sources/<SOURCE_ID>" \
  -H "authorization: Bearer $KEY"

# Erase (GDPR): also delete the episode row and its raw artifact bytes.
curl -s -X DELETE "localhost:8000/memory/sources/<SOURCE_ID>?erase=true" \
  -H "authorization: Bearer $KEY"
```

Every retraction writes an audit event. The caller must be able to read the source's scope.

## Writing to memory

There is no raw ingest endpoint on purpose: writing goes through connectors and curation, so
trust and provenance are never bypassed. To add knowledge, configure a source and connector
(see [loading knowledge](loading-knowledge.md)). Agents can *propose* a fact through the MCP
`memory_propose` tool; a proposal lands unverified in the agent's personal scope and is never
auto-published.

## Identity and administration

`/identity/*` covers registration, organizations, workspaces, projects, memberships, service
accounts, and API-key issuance, rotation, and revocation. A workspace admin can manage keys
for members of a workspace it administers. See `http://localhost:8000/docs` for the full set.
