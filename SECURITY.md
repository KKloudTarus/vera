# Security policy

## Reporting a vulnerability

Please do not open a public issue for a security vulnerability. Report it privately through
GitHub's [private vulnerability reporting](https://github.com/KKloudTarus/vera/security/advisories/new)
(the "Report a vulnerability" button under the repository's Security tab). Include a
description, affected versions or commit, and reproduction steps. We aim to acknowledge a
report within a few working days, agree on a disclosure timeline, and credit reporters who
wish to be named once a fix is released.

Supported for fixes: the latest commit on `main`. VERA is pre-1.0; there are no maintained
release branches yet.

## Security review

Scope: the VERA platform (API, MCP server, ingestion worker) and its data plane
(PostgreSQL, Neo4j, Valkey, S3-compatible object store). This is the review record for
the hardening phase: a STRIDE pass, secrets handling, dependency scan, and a
prompt-injection review of the MCP surface.

## Trust boundaries

- Untrusted clients reach the API and the MCP server over the network. Both require a
  bearer credential and resolve the caller's scopes server-side.
- The worker consumes only already-published, verified episodes from Postgres. It has no
  inbound network surface.
- PostgreSQL and S3 are authoritative. Neo4j is a projection rebuildable from them.

## STRIDE

### Spoofing
- API: bearer authentication (`get_principal`) accepts a VERA API key or an OIDC token;
  an unauthenticated call returns 401 with `WWW-Authenticate: Bearer`. API keys are
  `<prefix>.<secret>`; only the secret is hashed (SHA-256) and it is verified in constant
  time. OIDC tokens are validated for signature, issuer, audience, and expiry.
- MCP: OAuth 2.1 Resource Server (RFC 9728). Built-in and external OIDC JWT verifiers check
  signature, `iss`, `aud`, and required scopes; external OIDC tokens also require `exp`. A token
  minted for another audience is rejected. External `iss|sub` identities are JIT-linked to
  personal-only VERA principals.
- MCP token issuance: an API-authenticated principal can mint only a non-expiring token whose
  `sub` is its own principal id. The endpoint accepts no target principal id and returns
  `Cache-Control: no-store`; the signing secret never leaves the server. Tokens are read-only
  by default, and requested scopes are limited to the four MCP capability classes. This is an
  intentional temporary bootstrap tradeoff; credential-bearing configs remain untracked and
  rotating the signing key invalidates all issued fallback tokens.
- A service account authenticates as a principal of kind `service_account`, so it carries
  no ambient authority beyond its memberships.

### Tampering
- All group-scoped knowledge tables have row-level security enabled and FORCED. The
  application connects through the non-superuser `vera_app` role and sets
  `vera.group_id` per transaction (`use_tenant`), so a query cannot read or write another
  tenant's rows even if application code is wrong.
- Writes commit to Postgres first; graph updates flow through the transactional outbox, so
  a partial failure cannot leave the graph ahead of the source of truth.

### Repudiation
- Structured logs carry a correlation id and the tenant/group context. Published episodes
  record their pipeline and ontology versions, so any fact is traceable to the run that
  produced it. An `audit_events` table exists for security-relevant actions.

### Information disclosure
- Scope resolution is server-side: a client never chooses its `group_ids`. Reads span
  only the caller's memberships (proven in the identity and MCP tests), so tenants cannot
  see each other's memory.
- Logging redacts sensitive keys (authorization, tokens, API keys, passwords, provider
  keys) at the processor level, including one level of nesting.
- Secrets are typed `SecretStr` in settings and never logged. `.env` is git-ignored;
  `.env.example` carries no real values.

### Denial of service
- Provider calls pass a dual token-bucket rate limiter (RPM and TPM) and a circuit
  breaker, with per-call and per-episode deadlines so a hung dependency cannot pin an
  ingestion lane. The read path has a tight timeout budget.
- The queue is Postgres-native with `FOR UPDATE SKIP LOCKED` and per-group serialization,
  which bounds contention and prevents a single group from starving others.

### Elevation of privilege
- Role-based access control is enforced on workspace-scoped actions: creating projects and
  adding members require an admin+ role; a plain member is denied (403).
- The `vera_app` role is not a superuser and cannot bypass RLS. The container image runs
  as an unprivileged user.

## Prompt-injection review (MCP surface)

The MCP server exposes only reads and a constrained proposal path; there is no tool that
mutates the shared graph directly.

- Content ingested from sources is data, not instructions: retrieval returns facts with
  provenance, and the server never executes text from memory.
- `memory_propose` cannot publish to a shared scope. A proposal lands in the caller's
  personal scope as an unverified, tier-4 claim; the contamination guard blocks unverified
  or personal content from entering a shared group. Promotion to shared memory requires
  human review.
- Every tool resolves the caller's scopes from its authenticated principal, so a crafted
  argument cannot widen access or reach another tenant.
- The write surface is minimal (propose and feedback only); there is no eval, no shell,
  and no arbitrary graph mutation reachable from a tool call.

Residual risk: retrieved text may contain adversarial instructions aimed at the calling
agent. That is the client's responsibility to handle; VERA labels every hit with its
source and verification so a client can weigh trust.

## Secrets handling

- All credentials load from the environment via `pydantic-settings` as `SecretStr`.
- No secret is committed: `.env` and `.env.*` (except `.env.example`) are git-ignored and
  excluded from the image via `.dockerignore`; a source scan found no hardcoded secrets.
- API-key secrets are stored only as SHA-256 hashes; the plaintext is shown once on issue.

## Dependency scan

`pip-audit` runs in CI as a required gate (it fails the build on a known vulnerability).
The packaging tools (pip, setuptools, wheel) are upgraded before the audit, and the runtime
container upgrades them in its virtual environment too, so no vulnerable packaging tool
ships in the image. An advisory with no available fix can be waived only with an explicit
`--ignore-vuln <ID>` entry and a written justification. Dependabot proposes dependency and
GitHub Actions updates weekly. Only open-source, cloud-portable dependencies are used, each
reached through a port, so no single-cloud managed service is a dependency.

## Follow-ups (tracked, not blocking)

- Exercise authorization-code + PKCE login, refresh, and revocation against each production
  IdP and supported coding client before retiring the built-in JWT fallback.
- Admin-managed key rotation for principals other than self.
