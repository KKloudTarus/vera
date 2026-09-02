# Claude Code (Tier 1)

Wire VERA into Claude Code as a remote (streamable-HTTP) MCP server. This page fills the
[adapter template](../GUIDE.md#adapter-section-template) with values from the official
Claude Code MCP docs. It is the configuration contract; the MCP config, project hook settings,
and dependency-free hook in `examples/integrations/claude-code/` are validated on every test
run. `examples/integrations/claude-code/SPEC.md` is the compact runtime-specific input consumed
by the setup skill.

!!! success "Status: Tier-1 reference available"
    The project configuration, skill, and hooks are schema-tested. Setup checks API and MCP
    reachability, validates the installed files, then confirms the `vera` MCP server after
    Claude Code restarts.

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
- Hooks: project-scoped `SessionStart` bootstrap metadata and `PreToolUse` write approval in
  `.claude/settings.json`; no prompt, transcript, source, or retrieved content is forwarded.

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

## Behavior skill

- Copy the portable VERA skill (`examples/integrations/vera-skill/SKILL.md`) to
  `.claude/skills/vera-memory/SKILL.md` so the agent loads VERA behavior on demand.
- For onboarding or repair, run the two-endpoint project setup workflow in
  `examples/integrations/vera-project-setup/SKILL.md`.

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

After installing the project files, restart Claude Code and return to the setup session.
Confirm that `vera` is connected and its tools are visible.

## Hooks

The setup skill installs two project hooks by structurally merging
`examples/integrations/claude-code/settings.json` into `.claude/settings.json` and copying
`vera-hook.cjs` to `.claude/hooks/vera-hook.cjs`:

- `SessionStart` derives a sanitized Git remote and branch locally and injects only the bounded
  arguments for the agent's normal `knowledge_bootstrap` call. It is stateless, performs no
  network call, and fails open when no unique safe remote exists.
- `PreToolUse` returns `permissionDecision: "ask"` for proposals, feedback, retractions,
  snapshots, and `knowledge_get_context(persist=true)`. Ephemeral reads are unaffected.

This adapts Serena's reminder-hook architecture without its read/grep denial, persistent
counter, cleanup, or MCP auto-approval. VERA supplements local evidence and exposes write
tools, so those Serena behaviors would violate VERA's lifecycle and suggest-mode policy.

Project hooks run with user privileges and are subject to workspace trust and managed policy.
Review both files before approval. Use `/hooks` to confirm their project source and exact
matchers after restarting Claude Code.

## Lifecycle

- Update: change the `.mcp.json` entry and only the VERA-owned entries in
  `.claude/settings.json`, or re-run `claude mcp add` for the same name.
- Disable: toggle the server off in `/mcp`, or add it to `disabledMcpServers`.
- Doctor: `claude mcp get vera` reports connection errors and tool count.
- Uninstall: `claude mcp remove vera --scope project`, remove only VERA's two hook entries,
  and remove `.claude/hooks/vera-hook.cjs` and the VERA skill only when they have no later
  user edits. Leave other servers, hooks, and skills intact.

## Known limitations

- Managed-policy and cloud-agent surfaces are not covered by this local adapter.
- Project hooks may be disabled by `allowManagedHooksOnly`; managed policy takes precedence.
- `suggest` remains the default. Enable `auto-propose` only with explicit user selection and
  a bootstrap response that grants `personal-proposal`; end each task with
  `knowledge_proposal_report` and offer `knowledge_retract_proposal` for undo.
