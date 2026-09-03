# Codex project setup spec

Read this file only when the selected runtime is Codex. The common setup
workflow is `../vera-project-setup/SKILL.md`; this file owns only Codex details.

Preflight both `git --version` and `node --version`. The hook requires Node.js.
If Node is absent, request separate approval before installing it or report
that setup cannot install the hook; do not install a hook that cannot run.

## Project targets

| Purpose | Target | Reference |
|---|---|---|
| MCP and approval | `.codex/config.toml` -> `mcp_servers.vera` and owned keys | `config.toml` |
| Skill | `.agents/skills/vera-memory/SKILL.md` | `../vera-skill/SKILL.md` |
| Hook config | `.codex/hooks.json` -> VERA `SessionStart` group | `hooks.json` |
| Hook helper | `.codex/hooks/vera-hook.cjs` | `vera-hook.cjs` |

Parse TOML and JSON by key. Preserve unrelated servers, settings, hooks, and
comments; if a TOML-aware merge cannot preserve them, stop with a manual patch.

## MCP and auth

- Remote OAuth: configure only `url`, with no `http_headers`, then run
  `codex mcp login vera --scopes memory:read,memory:propose,memory:feedback,memory:snapshot`.
  Remove an existing JWT header only after OAuth login and connection succeed.
- Remote JWT: replace `<VERA_MCP_JWT>` in `http_headers.Authorization` with the
  JWT only through `install_jwt.py` after the config is excluded from Git. Never
  ask for or put the REST API key in `.codex/config.toml`.
- Loopback local-dev: replace the URL and omit `http_headers`.
- Keep `required = false` for fail-open coding, `approval_policy = "on-request"`,
  `approvals_reviewer = "user"`, and
  `default_tools_approval_mode = "writes"` unless stricter managed policy wins.

## Hooks and approval

Install only the reference `SessionStart` hook. Its POSIX and Windows commands
resolve the Git root, derive sanitized bootstrap metadata, and make no network
call. Use `/hooks` to inspect and trust the exact project hook hash.

Codex cannot currently turn `PreToolUse` output into an interactive ask. Do not
install a `permissionDecision: "ask"` hook: it fails without stopping the call.
The supported MCP `writes` mode prompts for every non-read-only tool. Since
`knowledge_get_context` can persist, both its ephemeral and persisted calls
prompt. Exact argument-sensitive approval is not available.

Project config and hooks load only after repository trust. Managed hook policy or
an MCP allowlist that excludes the exact VERA URL prevents this setup; never
bypass it.

## Runtime verification

1. Parse `.codex/config.toml` and `.codex/hooks.json`, then run `node --check` on
   the hook helper.
2. Restart Codex and run `codex mcp list`; never use `codex mcp get` while a
   static Authorization header is configured because it may expose the JWT.
3. Confirm `vera` is connected and its tools are visible.

## Runtime uninstall

Remove only owned project config, hook, helper, and skill content. Do not use
`codex mcp remove vera` for this entry because that command targets user config.
Restart and verify VERA is absent.
