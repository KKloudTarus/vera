# Cursor local (Tier 1)

Wire VERA into Cursor as a remote MCP server. Values come from the official Cursor MCP docs.
The example config in `examples/integrations/cursor/mcp.json` is validated on every test run.

!!! success "Status: Tier-1 reference available"
    The configuration is schema-tested and the server prerequisites are available. An
    installation reports `PASS` only after Cursor loads it, `knowledge_bootstrap` resolves
    one project, and one bounded read succeeds under the target deployment's policy.

## At a glance

- Surfaces: Cursor desktop (local). Cite a current Cursor release; the fields below are stable
  in current docs.
- Scopes: `project` (`.cursor/mcp.json` in the repo) and `global` (`~/.cursor/mcp.json` in the
  home directory). Precedence between the two is not documented by Cursor; keep VERA in one
  place to avoid ambiguity.
- MCP config: `.cursor/mcp.json`, top-level `mcpServers`. Transport is inferred from the URL
  scheme, so a remote server needs no `type` field.
- Secrets: `${env:NAME}` expansion in values (also `${userHome}`, `${workspaceFolder}`).
- OAuth: supported through an `auth` object, or discovery via the server's
  `/.well-known/oauth-authorization-server`.

## MCP config

Write `.cursor/mcp.json` at the repo root:

```json
{
  "mcpServers": {
    "vera": {
      "url": "https://mcp.vera.example/mcp",
      "headers": {
        "Authorization": "Bearer ${env:VERA_MCP_TOKEN}"
      }
    }
  }
}
```

## Secrets

Cursor expands `${env:NAME}` in config values. Keep the bearer token in the environment and
reference it from `headers`, so nothing secret is written into `.cursor/mcp.json`. For OAuth,
use the `auth` object and pass the client id and secret as `${env:...}` references:

```json
{
  "mcpServers": {
    "vera": {
      "url": "https://mcp.vera.example/mcp",
      "auth": {
        "CLIENT_ID": "${env:VERA_OAUTH_CLIENT_ID}",
        "CLIENT_SECRET": "${env:VERA_OAUTH_CLIENT_SECRET}"
      }
    }
  }
}
```

If `scopes` are omitted, Cursor discovers them from the authorization-server metadata. Register
Cursor's fixed redirect URLs with your authorization server when you use OAuth.

## Instructions and skills

- Cursor reads `AGENTS.md` at the repo root for project instructions; ship the same
  `AGENTS.md` used elsewhere.
- Provide the portable VERA skill (`examples/integrations/vera-skill/SKILL.md`) as the
  behavior reference.

## Permissions and trust

Cursor asks for approval before running an MCP tool by default; review the arguments before
approving. Servers are enabled or disabled from the Customize panel in the sidebar.

## Verify

Enable VERA in the Customize panel, then confirm it connected. Cursor does not document a
status dot; open the Output panel and select MCP Logs to check for connection errors. A bounded
`knowledge_bootstrap` call with the sanitized Git remote and current branch, followed by a
bounded `knowledge_get_context` call with `persist=false`, confirms the tools and project
mapping work.

## Hooks and plugins

Cursor does not expose the same hook or plugin packaging model as Claude Code. Session-start
context hooks are not part of this adapter.

## Lifecycle

- Update: edit the `.cursor/mcp.json` entry.
- Disable: toggle VERA off in the Customize panel, or set the entry aside.
- Uninstall: remove VERA's entry from `.cursor/mcp.json`, leaving other servers intact.

## Known limitations

- Project vs global precedence is not documented; do not rely on one overriding the other.
- Cloud and managed-policy surfaces are not covered by this local adapter.
- `suggest` remains the default. Enable `auto-propose` only with explicit user selection and
  the `personal-proposal` capability; report and undo through the proposal lifecycle tools.
