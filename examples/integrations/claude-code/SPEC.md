# Claude Code project setup spec

Read this file only when the selected runtime is Claude Code. The common setup
workflow is `../vera-project-setup/SKILL.md`; this file owns only Claude Code
details.

Preflight both `git --version` and `node --version`. The hooks require Node.js.
If Node is absent, request separate approval before installing it or report
that setup cannot install the hooks; do not install hooks that cannot run.

## Project targets

| Purpose | Target | Reference |
|---|---|---|
| MCP | `.mcp.json` -> `mcpServers.vera` | `.mcp.json` |
| Skill | `.claude/skills/vera-memory/SKILL.md` | `../vera-skill/SKILL.md` |
| Hook config | `.claude/settings.json` -> VERA hook groups | `settings.json` |
| Hook helper | `.claude/hooks/vera-hook.cjs` | `vera-hook.cjs` |

Parse and merge JSON by key. Preserve unrelated servers, settings, and hooks.

## MCP and auth

- Remote OAuth: configure only `type: "http"` and the supplied URL, with no
  `headers`. Restart, approve `vera`, and complete `/mcp` browser authentication.
  Remove an existing JWT header only after OAuth login and connection succeed.
- Remote JWT: replace `<VERA_MCP_JWT>` in the reference `type: "http"` config
  only through `install_jwt.py` after the config is excluded from Git. Never ask
  for or put the REST API key in `.mcp.json`.
- Loopback local-dev: replace the URL with the supplied endpoint and omit the
  Authorization header.
- Project scope is the repository-root `.mcp.json`; do not use local or user
  scope unless separately approved.

## Hooks and approval

Install both reference hook groups. `SessionStart` derives and sanitizes only a
unique Git remote and optional branch, injects bounded bootstrap arguments, and
makes no network call. `PreToolUse` returns `permissionDecision: "ask"` for
proposals, feedback, retraction, snapshot, and
`knowledge_get_context(persist=true)`; ephemeral context remains unaffected.

Do not add Serena's source-read denial, MCP auto-approval, counters, or cleanup.
Project hooks run with user privileges and require workspace trust. A managed
`allowManagedHooksOnly` policy prevents this project hook; never bypass it.

## Runtime verification

1. Parse `.mcp.json` and `.claude/settings.json`, then run `node --check` on the
   hook helper.
2. Restart Claude Code, trust the workspace, open `/mcp`, and approve only the project server named
   `vera`. Open `/hooks` and review the exact project hook source.
3. Return to the same setup session and use `/mcp` to confirm `vera` is connected
   and its tools are visible. Do not run a command that prints static headers.

## Runtime uninstall

Run `claude mcp remove vera --scope project`, remove only the owned hook groups,
and remove the helper and skill only when they are unchanged. Leave all
unrelated project settings intact.
