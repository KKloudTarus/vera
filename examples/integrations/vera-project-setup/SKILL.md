---
name: vera-project-setup
description: >-
  Set up VERA for the current coding project from a VERA API endpoint and MCP
  endpoint. Use when the user asks to install, connect, configure, onboard, or
  repair VERA at project scope for Claude Code, Codex, or OpenCode.
compatibility: Claude Code, Codex, or OpenCode with Git and remote HTTP MCP support
metadata:
  contract: "1"
  version: "1.1.0"
---

# Set Up VERA For This Project

Run the setup; do not merely describe it. Keep the workflow project-local and
load the references below instead of guessing runtime configuration.

## Inputs

There are exactly two required setup parameters:

```yaml
required:
  VERA_API_URL: Absolute VERA REST API base URL
  VERA_MCP_URL: Absolute VERA Streamable HTTP MCP endpoint, including its /mcp path
```

If either value is absent, ask for both in one question. Runtime and repository
root are detected; they are not setup parameters. Detect the runtime from the
current agent host, not from other installed executables: a Claude Code session
selects Claude Code even when Codex or OpenCode is also installed.

Remote setup prefers interactive OAuth and needs no credential input when OAuth
discovery and login succeed. Fallback needs one of these secrets:

```yaml
remote_fallback_auth_one_of:
  VERA_API_KEY: REST API key supplied to the helper process or entered at its hidden terminal prompt
  VERA_MCP_TOKEN: Existing MCP JWT supplied to the helper process with --existing-token
```

A loopback local-dev endpoint needs neither secret. For remote setup, first
validate OAuth discovery and use the runtime's browser login. If OAuth is not
usable, prefer `VERA_API_KEY`: the helper calls
`POST ${VERA_API_URL}/identity/mcp-token` as the current user and installs its
returned `access_token`. This route issues a token for the authenticated caller
and does not require workspace-admin privileges. A supplied `VERA_MCP_TOKEN`
must already be a valid MCP JWT; an API key is not accepted by the MCP server.

If a credential was already supplied in the setup request, never repeat or print it.
Pass it only to the `install_jwt.py` process as `VERA_API_KEY` or `VERA_MCP_TOKEN`,
then run the helper yourself. Otherwise prepare the untracked placeholder config
and ask the user to run the helper; it prompts without echo. The helper exchanges,
probes, and installs the token without printing either secret. Report the config as
credential-bearing and never stage or commit it.

## Load

Read these shared files:

1. `references/preflight.md`
2. `references/apply.md`
3. `../vera-skill/SKILL.md`

Detect the active coding runtime, then read exactly one matching spec:

| Runtime | Required spec |
|---|---|
| Claude Code | `../claude-code/SPEC.md` |
| Codex | `../codex/SPEC.md` |
| OpenCode | `../opencode/SPEC.md` |

Do not load or apply another runtime's spec. If the runtime or a required file is
unavailable, state the concrete problem and stop without changing project files.

## Execute

1. Detect the runtime, version, operating system, and repository root.
2. Run the API, MCP, and authentication checks in `references/preflight.md`.
3. Inspect the selected runtime's project targets and apply its matching local,
   OAuth, or fallback JWT configuration.
4. Report and apply the project-local diff under `references/apply.md`.
5. Run the selected runtime's lightweight config checks.
6. Ask the user to restart the coding tool and return to this setup session.
7. After the session resumes, confirm the project MCP server named `vera` is
   connected and its tools are visible.
8. Report `VERA setup completed for <runtime>`, changed files, selected auth
   mode, and endpoint smoke-test results without credential values.

## Invariants

- Project scope is the default. User-home files and dependencies require separate
  explicit approval.
- Preserve unrelated configuration. A remote runtime config contains a
  non-expiring credential after JWT fallback setup and must remain untracked.
