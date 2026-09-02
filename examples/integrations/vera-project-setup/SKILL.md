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
root are detected; they are not setup parameters.

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
2. Run the API and MCP URL checks in `references/preflight.md`.
3. Inspect the selected runtime's project targets.
4. Report and apply the project-local diff under `references/apply.md`.
5. Run the selected runtime's lightweight config checks.
6. Ask the user to restart the coding tool and return to this setup session.
7. After the session resumes, confirm the project MCP server named `vera` is
   connected and its tools are visible.
8. Report `VERA setup completed for <runtime>`, changed files, and endpoint
   smoke-test results.

## Invariants

- Project scope is the default. User-home files and dependencies require separate
  explicit approval.
- Preserve unrelated configuration and keep credentials in environment or
  runtime secret storage.
