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
5. During migration, inspect `AGENTS.md` and `CLAUDE.md`. Remove one only when its
   entire content is obsolete VERA integration guidance and the approved diff
   names its removal. Preserve mixed or unrelated instructions.

## Validate

1. Parse every changed JSON, JSONC, or TOML file.
2. Run `node --check` for each installed JavaScript hook helper.
3. Ask the user to restart the coding tool and return to the setup session.
4. After resume, use the runtime's MCP status command or UI to confirm `vera` is
   connected and its tools are visible.

Report that VERA setup completed and list the changed files.
