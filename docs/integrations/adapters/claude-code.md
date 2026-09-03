# Claude Code (Tier 1)

See [Integrate VERA with coding tools](../coding-tools.md) for the shared fast-path prompt and
setup flow.

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

## At a Glance

- Surfaces: local CLI and IDE extension. Use a current Claude Code release that supports remote
  HTTP MCP and custom headers in `.mcp.json`.
- Scopes: `local` (`~/.claude.json`, this machine only), `project` (`.mcp.json` at the repo
  root), `user` (`~/.claude.json`, all your projects). Managed-policy and cloud scopes
  are out of scope for this adapter.
- MCP config: `.mcp.json` at the repository root, top-level `mcpServers`.
- Authentication: browser OAuth with client-managed storage and refresh; a
  non-expiring literal MCP JWT is the temporary fallback when OAuth is unavailable.
- Hooks: project-scoped `SessionStart` bootstrap metadata and `PreToolUse` write approval in
  `.claude/settings.json`; no prompt, transcript, source, or retrieved content is forwarded.

## MCP Config

Project scope writes `.mcp.json` at the repo root. OAuth config contains no token:

```json
{
  "mcpServers": {
    "vera": {
      "type": "http",
      "url": "https://mcp.vera.example/mcp"
    }
  }
}
```

The equivalent CLI writes the same entry. When the authorization server does not
support dynamic client registration, add its pre-registered client ID and fixed
callback port:

```bash
claude mcp add --transport http vera https://mcp.vera.example/mcp --scope project
# Optional for a pre-registered public client:
#   --client-id <CLIENT_ID> --callback-port <PORT>
claude mcp login vera
```

`--scope project` is the only scope that writes `.mcp.json`; `local` and `user` write
`~/.claude.json`.

Claude Code opens the authorization URL, stores the resulting tokens outside
`.mcp.json`, and refreshes them. Complete `/mcp` login and confirm the tools before
removing a previously working JWT header.

## JWT Fallback

An ordinary VERA user can exchange their REST API key for an MCP JWT without
exposing either credential to the coding agent. First prepare the untracked
`.mcp.json` with one `<VERA_MCP_JWT>` placeholder, then run:

```bash
python <VERA_REPO>/examples/integrations/vera-project-setup/install_jwt.py \
  --api-url https://api.vera.example \
  --mcp-url https://mcp.vera.example/mcp \
  --config .mcp.json
```

The helper prompts without echo, requests all four coding scopes, probes MCP, and atomically
replaces the placeholder. Add `--existing-token` to enter an existing JWT or `--rotate` to
replace the config's single expired JWT. The API key
is not a valid MCP bearer token and must not be written into `.mcp.json`. The JWT is non-expiring,
so rerun the helper with `--rotate` after expiry. Keep this config untracked and do not run commands
that print its static headers.

## Behavior Skill

- Copy the portable VERA skill (`examples/integrations/vera-skill/SKILL.md`) to
  `.claude/skills/vera-memory/SKILL.md` so the agent loads VERA behavior on demand.
- For onboarding or repair, run the two-endpoint project setup workflow in
  `examples/integrations/vera-project-setup/SKILL.md`.

## Permissions and Workspace Trust

A project-scoped `.mcp.json` server is gated by workspace trust: on first use in an
interactive session Claude Code prompts to approve the project's MCP servers, and the server
shows `Pending approval` until then. To pre-approve in automation, list the server under
`enabledMcpjsonServers` in an explicit settings input. Allow only `vera`; do not enable every
project MCP server.
`claude mcp reset-project-choices` clears stored approvals.

## Verify

Run `/mcp` in a session to see the server's connection status, tool count, and auth state, or
from the shell:

```bash
claude mcp list          # names, scopes, connection status
```

After installing the project files, restart Claude Code, trust the workspace, open `/mcp`, and
approve only `vera`. Review the project hooks in `/hooks`, return to the same setup session, and
confirm that `vera` is connected and its tools are visible.

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

- Update: OAuth refresh is runtime-managed. For JWT fallback, rerun setup to issue and replace
  the token; change only VERA-owned entries in `.mcp.json` and `.claude/settings.json`.
- Disable: toggle the server off in `/mcp`, or add it to `disabledMcpServers`.
- Doctor: use `/mcp`; avoid config-detail commands while a static header exists.
- Uninstall: `claude mcp remove vera --scope project`, remove only VERA's two hook entries,
  and remove `.claude/hooks/vera-hook.cjs` and the VERA skill only when they have no later
  user edits. Leave other servers, hooks, and skills intact.

## Known Limitations

- Managed-policy and cloud-agent surfaces are not covered by this local adapter.
- Project hooks may be disabled by `allowManagedHooksOnly`; managed policy takes precedence.
- `suggest` remains the default. Enable `auto-propose` only with explicit user selection and
  a bootstrap response that grants `personal-proposal`; end each task with
  `knowledge_proposal_report` and offer `knowledge_retract_proposal` for undo.
