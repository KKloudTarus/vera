# VERA project apply

## Apply

1. Inspect the project targets named by the selected runtime spec and preserve
   unrelated project configuration.
2. Report the exact project-local files and keys to change. An explicit request
   to apply the selected canonical runtime spec approves that project diff when
   no conflict or extra change is found. Ask before resolving a conflict or
   changing anything outside the project.
3. Parse JSON, JSONC, and TOML with format-aware tooling and apply the smallest
   merge. Reuse an equivalent VERA definition instead of creating a duplicate.
4. Copy `../../vera-skill/SKILL.md` to the selected runtime's skill destination
   and install only that runtime's MCP, hook/plugin, and permission artifacts.
   OAuth config contains only the endpoint and runtime OAuth settings; remove any
   static Authorization header only after interactive login succeeds. JWT fallback
   keeps exactly one `<VERA_MCP_JWT>` placeholder for `install_jwt.py` to replace.
   Loopback config omits both OAuth and the Authorization header.
5. Before acquiring or writing a JWT, stop if the target config is tracked or
   staged. For a new untracked config, add its path to `.git/info/exclude` before
   writing. Never stage or commit a credential-bearing config.
6. For JWT fallback, run this command when the credential is already available to
   the setup session; expose it only to that process as `VERA_API_KEY` or
   `VERA_MCP_TOKEN`. Otherwise give the command to the user and wait for its redacted
   hidden-prompt result. Add `--existing-token` only for that mode, or `--rotate` to
   replace the config's single literal JWT; never put a secret in the command:

   `python <VERA_REPO>/examples/integrations/vera-project-setup/install_jwt.py --api-url <VERA_API_URL> --mcp-url <VERA_MCP_URL> --config <PROJECT_CONFIG>`
7. During migration, inspect `AGENTS.md` and `CLAUDE.md`. Remove one only when its
   entire content is obsolete VERA integration guidance and the approved diff
   names its removal. Preserve mixed or unrelated instructions.

## Validate

1. Parse every changed JSON, JSONC, or TOML file.
2. Run `node --check` for each installed JavaScript hook helper.
3. Give the selected runtime's exact trust and approval steps. Ask the user to
   restart the coding tool and return to the setup session.
4. After resume, use a status command or UI that does not print configured headers
   to confirm `vera` is connected and its tools are visible. For OAuth, finish the
   browser flow first; if it fails, restore the prior JWT header before selecting
   fallback.

Report that VERA setup completed, the auth mode, and the changed files. Never
include a credential value. For JWT fallback, report that `expires_in: null`
means the installed token is intentionally non-expiring.
