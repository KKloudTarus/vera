# OpenCode project setup spec

Read this file only when the selected runtime is OpenCode. The common setup
workflow is `../vera-project-setup/SKILL.md`; this file owns only OpenCode
details.

## Project targets

| Purpose | Target | Reference |
|---|---|---|
| MCP and permissions | `opencode.json` -> `mcp.vera` and `permission.vera_*` | `opencode.json` |
| Skill | `.opencode/skills/vera-memory/SKILL.md` | `../vera-skill/SKILL.md` |
| Bootstrap plugin | `.opencode/plugins/vera.ts` | `vera.ts` |

Parse and merge JSON/JSONC by key. Put exact VERA permission entries after
broader wildcard rules so they win. Preserve unrelated servers, permissions,
plugins, and instructions.

## MCP and auth

- Remote OAuth: omit `headers` and the fallback-only `oauth: false`, then run
  `opencode mcp auth vera`. Remove an existing JWT header only after OAuth login
  and connection succeed.
- Remote JWT: replace `<VERA_MCP_JWT>` in the Authorization header with the JWT
  only through `install_jwt.py` after the config is excluded from Git. Keep
  `oauth: false`; never ask for or put the REST API key in `opencode.json`.
- Loopback local-dev: replace the URL and omit the Authorization header.
- Keep the MCP key exactly `vera`; OpenCode derives tool and permission names
  such as `vera_knowledge_propose` from it.

## Plugin and approval

The project-local `chat.message` plugin is discovered automatically. It injects
one bounded bootstrap reminder per session while the process is running, derives
only a sanitized Git remote and optional branch, and makes no network call. It
does not inspect prompts or use `tool.execute.before` as a permission mechanism.

Exact `permission.vera_* = "ask"` rules protect write-capable tools in the normal
TUI. Permission rules cannot inspect `persist`, so
`vera_knowledge_get_context` prompts for ephemeral calls too. A user can choose
`Always`, and `--auto` can approve asks automatically; these are not hard consent
boundaries. Automation must use a read-only VERA principal and deny write tools,
because write-capable auto mode cannot enforce interactive consent.

## Runtime verification

1. Parse `opencode.json` and type-check or load the project plugin.
2. Restart OpenCode and run `opencode mcp list`.
3. Confirm `vera` is connected and its tools are visible.

## Runtime uninstall

Remove only owned `mcp.vera` and `permission.vera_*` keys. Remove the plugin and
skill only when they are unchanged, restart, and verify VERA is absent.
