# OpenCode (Tier 1)

See [Integrate VERA with coding tools](../coding-tools.md) for the shared fast-path prompt and
setup flow.

Wire VERA into OpenCode as a remote MCP server with a behavior skill,
exact tool permissions, and a local bootstrap plugin. The config and plugin invariants in
`examples/integrations/opencode/` are validated on every test run. Its compact setup input is
`examples/integrations/opencode/SPEC.md`.

!!! success "Status: Tier-1 reference available"
    The project configuration, skill, and plugin are schema-tested. Setup checks API and MCP
    reachability, validates the installed files, then confirms the `vera` MCP server after
    OpenCode restarts.

## At a Glance

- Surfaces: OpenCode CLI (local). Cite a current OpenCode release; the fields below are stable
  in current docs.
- Scopes: project-scoped `opencode.json` (or `opencode.jsonc`) at the repo root.
- MCP config: the `mcp` object, keyed by server name, with `"$schema"` set.
- Authentication: browser OAuth with OpenCode-managed token refresh; a non-expiring
  literal MCP JWT is the fallback.
- Skills: `.opencode/skills/vera-memory/SKILL.md`.
- Plugins: project-local `.opencode/plugins/vera.ts`, discovered without a package entry.
- Permissions: exact `vera_*` tool keys set to `"ask"` after broader wildcard rules.

## MCP Config

Write `opencode.json` at the repo root:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "vera": {
      "type": "remote",
      "url": "https://mcp.vera.example/mcp",
      "enabled": true
    }
  },
  "permission": {
    "vera_knowledge_get_context": "ask",
    "vera_knowledge_propose": "ask",
    "vera_knowledge_retract_proposal": "ask",
    "vera_knowledge_feedback": "ask",
    "vera_knowledge_create_snapshot": "ask",
    "vera_memory_propose": "ask",
    "vera_memory_feedback": "ask"
  }
}
```

A remote OAuth server uses `type: "remote"`, `url`, and `enabled`; omitting
`oauth: false` enables discovery. `timeout` (ms, default
5000) is optional. OpenCode names MCP tools by sanitizing the config key and tool name, so the
fixed key `vera` produces permission names such as
`vera_knowledge_propose`.

Authenticate after writing the endpoint-only config:

```bash
opencode mcp auth vera
```

OpenCode owns token storage and refresh. Remove a prior JWT header only after this
login and the MCP connection succeed.

## JWT Fallback

An ordinary VERA user can exchange their REST API key for an MCP JWT without
exposing either credential to the coding agent. Prepare one placeholder in the
untracked config, then run:

```bash
python <VERA_REPO>/examples/integrations/vera-project-setup/install_jwt.py \
  --api-url https://api.vera.example \
  --mcp-url https://mcp.vera.example/mcp \
  --config opencode.json
```

Set `oauth: false` and add `headers.Authorization = "Bearer <VERA_MCP_JWT>"`. The helper
prompts without echo, requests/probes a JWT, and atomically replaces the placeholder. Add
`--existing-token` when appropriate or `--rotate` after compromise or intentional credential
rotation. The API
key is not a valid MCP bearer token. Keep the resulting config untracked.

## Behavior Skill

- Copy the portable VERA skill (`examples/integrations/vera-skill/SKILL.md`) to
  `.opencode/skills/vera-memory/SKILL.md`.
- For onboarding or repair, run the two-endpoint project setup workflow in
  `examples/integrations/vera-project-setup/SKILL.md`.

## Permissions and Trust

OpenCode runs configured project MCP servers. Keep VERA in `opencode.json` so the setup is
explicit and reviewable. Put the exact VERA permission keys after broader wildcard rules;
normal rule evaluation uses the last matching rule.

The normal TUI prompts for `"ask"`, but a user can choose `Always`, and `--auto` can approve
requests automatically. These permissions are therefore an interactive safety mechanism, not
a non-bypassable security boundary. Automation must use a read-only VERA principal and deny
write tools because write-capable auto mode cannot enforce interactive consent.

Because permission rules are tool-level, `vera_knowledge_get_context: "ask"` also prompts for
ephemeral `persist=false` calls. Omitting that rule would leave `persist=true` unguarded by
client permissions.

## Verify

```bash
opencode mcp list       # servers and their auth status
```

Restart OpenCode, return to the setup session, and use a normal TUI without `--auto`. Confirm
that `vera` is connected and its tools are visible.

## Hooks and Plugins

Copy `examples/integrations/opencode/vera.ts` to `.opencode/plugins/vera.ts`. OpenCode discovers
project-local TypeScript plugins automatically. The `chat.message` hook injects one bounded
bootstrap reminder per session while the process is running. It derives and sanitizes only the
Git remote and optional branch, calls no network or MCP endpoint, and reads no prompt,
transcript, source, or retrieved content.

The plugin does not use `tool.execute.before` as an approval guard. That hook can inspect
arguments or throw to deny a call, but it cannot request an interactive permission decision.
On process restart, a resumed session can receive the idempotent reminder again.

## Lifecycle

- Update: edit only the owned `mcp.vera`, `permission.vera_*`, plugin, skill, and instruction
  content, preserving unrelated configuration.
- Disable: set `"enabled": false`.
- Uninstall: remove the owned MCP entry and permission keys, remove the plugin and skill only
  if unchanged, and leave unrelated content intact.

## Known Limitations

- Cloud and managed-policy surfaces are not covered by this local adapter.
- `"ask"` is bypassable through `Always` or `--auto`; use server-side read-only credentials
  when writes must be impossible.
- OpenCode cannot condition permission on `knowledge_get_context.persist`, so safe interactive
  configuration over-prompts ephemeral context reads.
- `suggest` remains the default. Enable `auto-propose` only with explicit user selection and
  the `personal-proposal` capability; report and undo through the proposal lifecycle tools.
