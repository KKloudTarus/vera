# Integrate VERA with Coding Tools

This is the common setup entry point for Claude Code, Codex, and OpenCode. Each
tool has a child page with its project files, authentication flow, permissions,
restart checks, and uninstall steps.

## Fast Path

Open the target project in your coding tool and paste this one prompt. Replace
`<ABSOLUTE_VERA_REPO>` with the local VERA checkout path.

```text
Set up VERA for this project using
<ABSOLUTE_VERA_REPO>/examples/integrations/vera-project-setup/SKILL.md.

Use these setup values:
VERA_API_URL=http://localhost:8000
VERA_MCP_URL=http://localhost:8080/mcp

Detect the current coding tool and apply its matching project-local runtime spec. Install the
canonical MCP config, vera-memory skill, and hook or plugin while preserving unrelated project
configuration. I approve this project-local setup.

Smoke test the API and MCP URLs and validate the installed files. Then ask me to restart the
coding tool and return to this setup session. After I return, confirm the vera MCP server is
connected and its tools are visible, then report setup complete and list the changed files.
```

The agent pauses after installation. Restart the coding tool, resume the same
conversation, and tell it that the restart is complete. The agent then checks
the loaded `vera` MCP server and finishes the setup report.

## Prerequisites

- The VERA API and MCP server are running.
- The coding tool is installed and authenticated with its model provider.
- Git is available. Claude Code and Codex also need Node.js for their hooks.
- The target directory is a Git repository opened as a trusted workspace.
- The coding tool can read the local VERA checkout.

For the local stack:

```bash
docker compose --profile app up --build -d
curl --fail --silent http://localhost:8000/health/live
curl --fail --silent http://localhost:8000/health/ready
```

## Common Setup Flow

The setup skill:

1. Detects the current coding tool and repository root.
2. Loads only the matching runtime spec.
3. Checks the API liveness and readiness endpoints.
4. Checks the MCP URL with one short `OPTIONS` or `HEAD` request.
5. Merges the runtime's project-local config, skill, and hook or plugin.
6. Validates the changed files and asks for a restart.
7. Resumes the same session and confirms that `vera` is connected with tools
   visible.

An MCP response such as `401` or `403` proves that the endpoint is reachable and
still needs authentication. A local FastMCP endpoint commonly returns `405` to
`OPTIONS`; that also proves reachability.

## Choose Your Coding Tool

- [Claude Code](adapters/claude-code.md): `.mcp.json`, project hooks, workspace
  trust, static-token and OAuth setup.
- [Codex](adapters/codex.md): project TOML, `SessionStart` hook, repository trust,
  and MCP write approval.
- [OpenCode](adapters/opencode.md): project JSON, bootstrap plugin, exact tool
  permissions, static-token and OAuth setup.

The [agent integration GUIDE](GUIDE.md) defines the shared runtime behavior,
privacy, retrieval, and save contracts after setup.
