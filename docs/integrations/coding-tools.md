# Integrate VERA with Coding Tools

This is the common setup entry point for Claude Code, Codex, and OpenCode. Each
tool has a child page with its project files, authentication flow, permissions,
restart checks, and uninstall steps.

## Fast Path

Open the target project in your coding tool and paste this one prompt. Replace
`<ABSOLUTE_VERA_REPO>` with the local VERA checkout path.

```text
Set up VERA for this project using
<ABSOLUTE_VERA_REPO>/examples/integrations/vera-project-setup/SKILL.md.

Use these setup values:
VERA_API_URL=http://localhost:8000
VERA_MCP_URL=http://localhost:8080/mcp

Detect the current coding tool and apply its matching project-local runtime spec. Install the
canonical MCP config, vera-memory skill, and hook or plugin while preserving unrelated project
configuration. I approve this project-local setup.

Smoke test the API and MCP URLs and validate the installed files. Then ask me to restart the
coding tool and return to this setup session. After I return, confirm the vera MCP server is
connected and its tools are visible, then report setup complete and list the changed files.
```

The agent pauses after installation. Restart the coding tool, resume the same
conversation, and tell it that the restart is complete. The agent then checks
the loaded `vera` MCP server and finishes the setup report.

For a remote deployment, start with the two URLs only. Setup prefers browser OAuth
when the MCP endpoint advertises a working authorization server. If OAuth discovery
or login fails, setup creates a placeholder config and asks you to run a local helper.
Enter either a VERA API key or an existing MCP JWT only at that helper's hidden
terminal prompt, never in the setup conversation.

The API-key fallback works for an ordinary authenticated user; it does not require a
workspace admin. The helper calls `POST /identity/mcp-token`, receives a non-expiring
JWT for that same principal and the four coding scopes, verifies it against MCP, then
writes it directly into the selected coding tool's config. The REST API key is never
written there or shown to the coding agent.

## Prerequisites

- The VERA API and MCP server are running.
- The coding tool is installed and authenticated with its model provider.
- Git is available. Claude Code and Codex also need Node.js for their hooks.
- The target directory is a Git repository opened as a trusted workspace.
- The coding tool can read the local VERA checkout.

For the local stack:

```bash
docker compose --profile app up --build -d
curl --fail --silent http://localhost:8000/health/live
curl --fail --silent http://localhost:8000/health/ready
```

## Authentication

Local development without MCP auth needs only the two endpoint URLs. Remote setup
tries interactive OAuth first:

- The coding tool discovers the external authorization server, opens its browser
  login, stores tokens in its own secure store, and refreshes them automatically.
- OAuth is selected only when authorization-server discovery advertises usable
  authorization and token endpoints with PKCE `S256`, and interactive login plus
  the authenticated MCP probe succeed.
- External access tokens must target the VERA MCP audience and carry the requested
  `memory:*` scopes. VERA maps the verified OIDC `iss|sub` identity to a stable
  internal principal; first login creates only a personal scope.

If OAuth is unavailable or declined, the hidden helper accepts either a VERA REST
API key or a previously issued MCP JWT:

- A VERA API key (`vera_<prefix>.<secret>`) authenticates the REST API. The
  hidden-input helper exchanges it at `POST ${VERA_API_URL}/identity/mcp-token` for a JWT.
- An MCP JWT authenticates only the MCP resource. It is bound to the MCP issuer
  and audience, identifies the caller in `sub`, carries memory scopes, and
  expires.

The built-in token endpoint always issues for the authenticated caller, so a
normal user can obtain their own MCP JWT. Admin access is needed to provision
users or memberships, not to issue the caller's token. The API deployment and
MCP deployment must share `VERA_MCP__JWT_SECRET`, issuer, audience, and
algorithm settings.

Only fallback setup stores the JWT as a literal Authorization header in `.mcp.json`,
`.codex/config.toml`, or `opencode.json`. These files become credential-bearing:
the setup refuses tracked or staged targets and locally excludes a newly created
config before writing. The default token lifetime is eight hours; rerun setup to
replace an expired token with the helper's `--rotate` option. Successful OAuth
config contains no JWT header; the
runtime owns access-token storage and refresh.

## Common Setup Flow

The setup skill:

1. Detects the current coding tool and repository root.
2. Loads only the matching runtime spec.
3. Checks the API liveness/readiness endpoints and MCP URL.
4. Validates OAuth discovery and tries the runtime's interactive browser login.
5. On OAuth success, configures no static header; otherwise creates a placeholder
   config and asks the user to run the hidden-input helper, which exchanges/probes
   the JWT and writes it literally without exposing it to the agent.
6. Installs and validates the behavior skill and hook or plugin.
7. Restarts the tool, completes trust/approval, and resumes the same session.
8. Confirms that `vera` is connected with tools
   visible.

An unauthenticated JSON-RPC `initialize` success selects no-auth mode. A `401` or
`403` triggers OAuth discovery, then JWT fallback only when OAuth is not usable.
Setup removes an existing JWT header only after OAuth login and an authenticated
probe succeed.

## Choose Your Coding Tool

- [Claude Code](adapters/claude-code.md): `.mcp.json`, OAuth/JWT fallback, project
  hooks, workspace trust, and MCP-server approval.
- [Codex](adapters/codex.md): project TOML, `SessionStart` hook, repository trust,
  and MCP write approval.
- [OpenCode](adapters/opencode.md): project JSON, OAuth/JWT fallback, bootstrap
  plugin, and exact tool permissions.

The [agent integration GUIDE](GUIDE.md) defines the shared runtime behavior,
privacy, retrieval, and save contracts after setup.
