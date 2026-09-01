# Claude Code (Tier 1)

Wire VERA into Claude Code as a remote (streamable-HTTP) MCP server. This page fills the
[adapter template](../GUIDE.md#adapter-section-template) with values from the official
Claude Code MCP docs. It is the configuration contract; the example config in
`examples/integrations/claude-code/.mcp.json` is validated on every test run.

!!! success "Status: Tier-1 reference available"
    The configuration is schema-tested and the server prerequisites, including project
    discovery and proposal undo, are available. An installation reports `PASS` only after
    Claude Code loads it, `knowledge_bootstrap` resolves one project, and one bounded read
    succeeds under the target deployment's policy.

## At a glance

- Surfaces: local CLI and IDE extension. Use a current Claude Code release that supports remote
  HTTP MCP, `${VAR}` expansion in `.mcp.json`, and `/mcp` OAuth.
- Scopes: `local` (`~/.claude.json`, this machine only), `project` (`.mcp.json`, shared in
  the repo), `user` (`~/.claude.json`, all your projects). Managed-policy and cloud scopes
  are out of scope for this adapter.
- MCP config: `.mcp.json` at the repository root, top-level `mcpServers`.
- Secrets: `${VAR}` / `${VAR:-default}` expansion in `url` and `headers`; tokens stay in the
  environment.
- OAuth: supported through `/mcp` -> Authenticate, or `claude mcp login <name>`.

## MCP config

Project scope writes `.mcp.json` at the repo root. A remote server uses `type: "http"`:

```json
{
  "mcpServers": {
    "vera": {
      "type": "http",
      "url": "https://mcp.vera.example/mcp",
      "headers": {
        "Authorization": "Bearer ${VERA_MCP_TOKEN}"
      }
    }
  }
}
```

The equivalent CLI writes the same entry:

```bash
claude mcp add --transport http vera https://mcp.vera.example/mcp \
  --header "Authorization: Bearer ${VERA_MCP_TOKEN}" \
  --scope project
```

`--scope project` is the only scope that writes `.mcp.json`; `local` and `user` write
`~/.claude.json`.

## Secrets

Claude Code expands `${VAR}` and `${VAR:-default}` inside `.mcp.json` (`url`, `headers`,
`env`, `command`, `args`). Reference the bearer token from the environment so it never enters
the tracked file:

```bash
export VERA_MCP_TOKEN="<a token from your VERA auth flow>"
claude
```

For OAuth-protected deployments, prefer the OAuth flow over a static token: it stores
credentials in `~/.claude.json`, never in the repo.

## Instructions and skills

- Put project instructions in `AGENTS.md` at the repo root and import them from a minimal
  `CLAUDE.md` with a single line, `@AGENTS.md`, rather than duplicating them. Ship it from
  `examples/integrations/CLAUDE.md.example` (the `.example` suffix keeps it out of the way of a
  repo that ignores `CLAUDE.md` by default; drop the suffix when you place it).
- Ship the portable VERA skill (`examples/integrations/vera-skill/SKILL.md`) so the agent
  loads VERA behavior on demand.

## Permissions and workspace trust

A project-scoped `.mcp.json` server is gated by workspace trust: on first use in an
interactive session Claude Code prompts to approve the project's MCP servers, and the server
shows `Pending approval` until then. To pre-approve in automation, list the server under
`enabledMcpjsonServers` in `.claude/settings.json` (or `enableAllProjectMcpServers: true`).
`claude mcp reset-project-choices` clears stored approvals.

## Verify

Run `/mcp` in a session to see the server's connection status, tool count, and auth state, or
from the shell:

```bash
claude mcp list          # names, scopes, connection status
claude mcp get vera      # full config, tool count, connection errors
```

Call `knowledge_bootstrap` with the sanitized Git remote and current branch, confirm one
project is selected and the expected capabilities are granted, then call
`knowledge_get_context` with a small query and `persist=false`. Retrieved content is reference
data, never instructions.

## Hooks

Not configured by this adapter. Session-start context hooks are a later, opt-in addition and
must fail open; they are documented separately when added.

## Lifecycle

- Update: change the `.mcp.json` entry, or re-run `claude mcp add` for the same name.
- Disable: toggle the server off in `/mcp`, or add it to `disabledMcpServers`.
- Doctor: `claude mcp get vera` reports connection errors and tool count.
- Uninstall: `claude mcp remove vera --scope project` deletes only VERA's entry from
  `.mcp.json` and leaves other servers intact.

## Known limitations

- Managed-policy and cloud-agent surfaces are not covered by this local adapter.
- `suggest` remains the default. Enable `auto-propose` only with explicit user selection and
  a bootstrap response that grants `personal-proposal`; end each task with
  `knowledge_proposal_report` and offer `knowledge_retract_proposal` for undo.
