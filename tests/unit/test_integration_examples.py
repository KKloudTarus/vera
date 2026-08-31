"""Validate the Tier-1 integration example configs against each runtime's schema.

The examples under ``examples/integrations`` are the configs an agent applies when
wiring VERA into Claude Code, Cursor, or OpenCode. This harness checks each one
against the fields the runtime's official docs define and enforces the invariant
that a bearer token is referenced from the environment, never written into a
tracked file. A planted literal token must fail, so the negative test guards the
guard.

Field references:
- Claude Code `.mcp.json`: `mcpServers.<name>` with `type: "http"`, `url`, `headers`;
  `${VAR}` expansion (https://code.claude.com/docs/en/mcp.md).
- Cursor `.cursor/mcp.json`: `mcpServers.<name>` with `url`, `headers`; transport is
  inferred from the URL; `${env:VAR}` expansion (https://cursor.com/docs).
- OpenCode `opencode.json`: `mcp.<name>` with `type: "remote"`, `url`, `enabled`,
  `headers`; `{env:VAR}` expansion (https://opencode.ai/docs/mcp-servers/).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "integrations"
_SERVER = "vera"

# Per-runtime environment-reference syntax for the Authorization header. A match
# proves the token is not a literal in the tracked file.
_ENV_REF = {
    "claude-code": re.compile(r"^Bearer \$\{[A-Z0-9_]+(:-[^}]*)?\}$"),
    "cursor": re.compile(r"^Bearer \$\{env:[A-Z0-9_]+\}$"),
    "opencode": re.compile(r"^Bearer \{env:[A-Z0-9_]+\}$"),
}


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _auth_header(entry: dict[str, Any]) -> str:
    headers = entry.get("headers")
    assert isinstance(headers, dict), "server entry must carry a headers object"
    value = headers.get("Authorization")
    assert isinstance(value, str), "an Authorization header is required"
    return value


def validate_claude_code(config: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    servers = config.get("mcpServers")
    if not isinstance(servers, dict) or _SERVER not in servers:
        return [f"mcpServers.{_SERVER} is missing"]
    entry = servers[_SERVER]
    if entry.get("type") != "http":
        problems.append("type must be 'http' for a remote streamable-HTTP server")
    if not str(entry.get("url", "")).startswith("https://"):
        problems.append("url must be an https endpoint")
    if not _ENV_REF["claude-code"].match(_auth_header(entry)):
        problems.append("Authorization must reference ${VAR}, not a literal token")
    return problems


def validate_cursor(config: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    servers = config.get("mcpServers")
    if not isinstance(servers, dict) or _SERVER not in servers:
        return [f"mcpServers.{_SERVER} is missing"]
    entry = servers[_SERVER]
    if not str(entry.get("url", "")).startswith("https://"):
        problems.append("url must be an https endpoint")
    if not _ENV_REF["cursor"].match(_auth_header(entry)):
        problems.append("Authorization must reference ${env:VAR}, not a literal token")
    return problems


def validate_opencode(config: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    if config.get("$schema") != "https://opencode.ai/config.json":
        problems.append("$schema must be https://opencode.ai/config.json")
    servers = config.get("mcp")
    if not isinstance(servers, dict) or _SERVER not in servers:
        return [*problems, f"mcp.{_SERVER} is missing"]
    entry = servers[_SERVER]
    if entry.get("type") != "remote":
        problems.append("type must be 'remote'")
    if entry.get("enabled") is not True:
        problems.append("enabled must be true")
    if not str(entry.get("url", "")).startswith("https://"):
        problems.append("url must be an https endpoint")
    if not _ENV_REF["opencode"].match(_auth_header(entry)):
        problems.append("Authorization must reference {env:VAR}, not a literal token")
    return problems


_VALIDATORS = {
    "claude-code": (validate_claude_code, _EXAMPLES / "claude-code" / ".mcp.json"),
    "cursor": (validate_cursor, _EXAMPLES / "cursor" / "mcp.json"),
    "opencode": (validate_opencode, _EXAMPLES / "opencode" / "opencode.json"),
}


@pytest.mark.parametrize("runtime", sorted(_VALIDATORS))
def test_example_config_is_valid_and_secret_free(runtime: str) -> None:
    validator, path = _VALIDATORS[runtime]
    assert path.is_file(), f"missing example config for {runtime}: {path}"
    assert validator(_load(path)) == []


def test_validator_rejects_a_literal_token() -> None:
    # A token written into the file (rather than an env reference) must be caught.
    leaked = {
        "mcpServers": {
            _SERVER: {
                "type": "http",
                "url": "https://mcp.vera.example/mcp",
                "headers": {"Authorization": "Bearer sk-live-01234567890abcdef"},
            }
        }
    }
    assert validate_claude_code(leaked) != []


def test_skill_declares_name_and_untrusted_content_rule() -> None:
    skill = (_EXAMPLES / "vera-skill" / "SKILL.md").read_text()
    assert skill.startswith("---"), "the skill needs YAML frontmatter"
    frontmatter = skill.split("---", 2)[1]
    assert "name: vera-memory" in frontmatter
    assert "description:" in frontmatter
    # The core safety rule must be stated in the skill body.
    assert "untrusted reference data" in skill


def test_project_instructions_are_present_and_minimal() -> None:
    agents = (_EXAMPLES / "AGENTS.md").read_text()
    assert "knowledge_get_context" in agents
    assert "suggest" in agents
    assert "untrusted reference data" in agents
    # Claude Code loads AGENTS.md through a one-line import rather than a copy. The example
    # ships with an .example suffix because the repo ignores CLAUDE.md by default.
    assert (_EXAMPLES / "CLAUDE.md.example").read_text().strip() == "@AGENTS.md"
