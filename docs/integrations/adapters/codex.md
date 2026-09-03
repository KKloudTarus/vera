# Codex (Tier 1)

See [Integrate VERA with coding tools](../coding-tools.md) for the shared fast-path prompt and
setup flow.

Wire VERA into Codex as a project-scoped streamable-HTTP MCP server. The reference
`config.toml`, project hook settings, and dependency-free hook in
`examples/integrations/codex/` are validated on every test run. Its compact setup input is
`examples/integrations/codex/SPEC.md`.

!!! success "Status: Tier-1 reference available"
    The project configuration, skill, and hook are schema-tested. Setup checks API and MCP
    reachability, validates the installed files, then confirms the `vera` MCP server after
    Codex restarts.

## At a Glance

- Surfaces: Codex CLI and IDE sessions that load project configuration. Hooks are experimental;
  use a current release and verify the effective schema with `codex --version` and `/hooks`.
- Scopes: project `.codex/config.toml` and `.codex/hooks.json`. User configuration under
  `~/.codex/` and managed requirements are inspected for conflicts but are not modified by
  default.
- MCP config: `[mcp_servers.vera]` in `.codex/config.toml`.
- Authentication: browser OAuth with Codex-managed token refresh; a short-lived
  literal MCP JWT in `http_headers` is the fallback.
- Skills: `.agents/skills/vera-memory/SKILL.md`.
- Hooks: a trusted project `SessionStart` hook supplies sanitized bootstrap metadata. MCP
  write approval uses `default_tools_approval_mode = "writes"`, not a hook.

## MCP Config

Write `.codex/config.toml` at the repository root:

```toml
approval_policy = "on-request"
approvals_reviewer = "user"

[features]
hooks = true

[mcp_servers.vera]
url = "https://mcp.vera.example/mcp"
enabled = true
required = false
startup_timeout_sec = 10
tool_timeout_sec = 60
default_tools_approval_mode = "writes"
```

`required = false` preserves VERA's fail-open coding behavior.

For remote OAuth, run:

```bash
codex mcp login vera --scopes memory:read,memory:propose,memory:feedback,memory:snapshot
```

Codex stores and refreshes its OAuth credential outside the project config. Remove
an existing `http_headers` value only after login and connection succeed. If OAuth
is unavailable, add
`http_headers = { Authorization = "Bearer <VERA_MCP_JWT>" }` and replace the
placeholder through the hidden-input helper:

```bash
python <VERA_REPO>/examples/integrations/vera-project-setup/install_jwt.py \
  --api-url https://api.vera.example \
  --mcp-url https://mcp.vera.example/mcp \
  --config .codex/config.toml
```

A REST API key is not a valid MCP bearer token. Keep fallback config untracked and
rerun the helper with `--rotate` to replace an expired JWT. Loopback local-dev also
omits `http_headers`.

## Behavior Skill

- Copy `examples/integrations/vera-skill/SKILL.md` to
  `.agents/skills/vera-memory/SKILL.md`.
- For onboarding or repair, run the two-endpoint project setup workflow in
  `examples/integrations/vera-project-setup/SKILL.md`.

## Permissions and Trust

Project configuration and hooks load only after Codex trusts the repository. Keep
`approval_policy = "on-request"`, route review to the user, and use the server-level `writes`
mode. A managed MCP allowlist must include the exact VERA URL. Managed policy can disable
project hooks or restrict approval policies; never bypass it.

`writes` prompts whenever the MCP tool is not annotated read-only. VERA deliberately marks
`knowledge_get_context` as write-capable because `persist=true` creates a context pack, so
Codex prompts for that tool even when `persist=false`. This is conservative and safe.

## Hooks

Copy `examples/integrations/codex/vera-hook.cjs` to
`.codex/hooks/vera-hook.cjs`, then merge the VERA `SessionStart` group from
`examples/integrations/codex/hooks.json` into `.codex/hooks.json`. The POSIX command and
`commandWindows` both resolve the Git root, derive a unique remote and optional branch, strip
credentials and local paths, and inject only a bounded bootstrap reminder. The hook makes no
network or MCP call and reads no prompt, transcript, source, or retrieved content.

Use `/hooks` to inspect the project source and approve its exact hash. A hook change requires
review again. Hook sources are additive, so do not duplicate the VERA group in TOML and JSON.

Codex `PreToolUse` currently supports `allow` and `deny`, but not an interactive
`permissionDecision: "ask"`. An `ask` output is treated as a hook error and does not stop the
tool call. The adapter therefore does not install a misleading write hook; MCP `writes` mode
is the supported guard. Exact argument-sensitive approval for only
`knowledge_get_context(persist=true)` is not available.

This adapts Serena's project reminder architecture without its Bash/read denial, MCP
auto-approval, persistent counter, or cleanup. VERA supplements local evidence and its hook is
stateless.

## Verify

Run:

```bash
codex doctor
codex mcp list
```

Restart Codex inside the trusted repository, return to the setup session, then inspect
`/debug-config`, `/hooks`, `/mcp verbose`, and `/skills`.
Confirm that `vera` is connected and its tools are visible.

## Lifecycle

- Update: OAuth refresh is runtime-managed. Structurally merge only the owned
  `[mcp_servers.vera]`, hook group, and approval keys; changing a hook requires trust review.
- Disable: set `enabled = false` or remove only the VERA project table.
- Doctor: use `codex doctor`, `/debug-config`, `/hooks`, and `/mcp verbose`.
- Uninstall: remove only VERA-owned project config, hook, and skill content, then restart and
  verify VERA is absent. Do not use
  `codex mcp remove vera` for the project entry because the CLI command targets user config.

## Known Limitations

- Project configuration and hooks are unavailable until repository trust is granted.
- Managed policy may allow only managed hooks or reject the VERA MCP identity.
- Codex does not currently support hook-driven interactive `ask`, so persisted and ephemeral
  `knowledge_get_context` calls share the conservative tool-level prompt.
- Cloud surfaces are not covered by this local project adapter.
- The hook protocol is evolving and has no single documented minimum version; verify it
  against the installed release rather than bypassing trust.
