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

- Remote static token: use the reference `type: "http"` shape and keep
  `Authorization: Bearer ${VERA_MCP_TOKEN}` as an environment reference.
- Remote OAuth: omit the static header and run `claude mcp login vera` or use
  `/mcp` -> Authenticate. Credentials belong in Claude's user secret storage.
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
2. Restart Claude Code and approve the project MCP server and hooks.
3. Run `claude mcp get vera` and confirm `vera` is connected and its tools are
   visible.

## Runtime uninstall

Run `claude mcp remove vera --scope project`, remove only the owned hook groups,
and remove the helper and skill only when they are unchanged. Leave all
unrelated project settings intact.
