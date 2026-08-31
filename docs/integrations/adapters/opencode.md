# OpenCode (Tier 1)

Wire VERA into OpenCode as a remote MCP server. Values come from the official OpenCode MCP
docs. The example config in `examples/integrations/opencode/opencode.json` is validated on
every test run.

!!! note "Status"
    Connect, authenticate, verify, and uninstall are complete and B-independent. A full
    end-to-end `PASS` and the whole [verification matrix](../GUIDE.md#verification-matrix)
    also need project discovery and proposal undo (#15). Until then this runtime is not marked
    released at Tier 1.

## At a glance

- Surfaces: OpenCode CLI (local). Cite a current OpenCode release; the fields below are stable
  in current docs.
- Scopes: project-scoped `opencode.json` (or `opencode.jsonc`) at the repo root. Keep VERA in
  the project file for a shared, reviewable setup.
- MCP config: the `mcp` object, keyed by server name, with `"$schema"` set.
- Secrets: `{env:VAR}` expansion in values.
- OAuth: supported (Dynamic Client Registration, or a manual client id and secret), or set
  `oauth: false` to disable.

## MCP config

Write `opencode.json` at the repo root:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "vera": {
      "type": "remote",
      "url": "https://mcp.vera.example/mcp",
      "enabled": true,
      "headers": {
        "Authorization": "Bearer {env:VERA_MCP_TOKEN}"
      }
    }
  }
}
```

A remote server uses `type: "remote"`, `url`, `enabled`, and `headers`. `timeout` (ms, default
5000) and `oauth` are optional.

## Secrets

OpenCode expands `{env:VAR}` in config values. Keep the bearer token in the environment and
reference it from `headers`, so nothing secret is written into `opencode.json`. For OAuth,
OpenCode registers dynamically when the server supports RFC 7591, or takes a manual `clientId`
and `clientSecret`; authenticate with:

```bash
opencode mcp auth vera
```

## Instructions and skills

- OpenCode reads `AGENTS.md` at the repo root for project instructions; ship the same
  `AGENTS.md`.
- Provide the portable VERA skill (`examples/integrations/vera-skill/SKILL.md`).

## Permissions and trust

OpenCode runs the configured MCP servers for the project. Keep VERA in the project
`opencode.json` so the setup is explicit and reviewable in the repo.

## Verify

```bash
opencode mcp list       # servers and their auth status
```

A bounded `knowledge_get_context` read confirms the tools work.

## Hooks and plugins

Session-start context hooks are not part of this adapter.

## Lifecycle

- Update: edit the `mcp.vera` entry.
- Disable: set `"enabled": false`.
- Uninstall: remove the `mcp.vera` entry, leaving other servers intact.

## Known limitations

- Project discovery, ambiguous / out-of-scope / monorepo / worktree scenarios, and
  `auto-propose` depend on #15. Use `suggest`.
