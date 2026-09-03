"""Validate the Tier-1 integration examples against each runtime's schema.

The examples under ``examples/integrations`` are the configs an agent applies when
wiring VERA into Claude Code, Codex, or OpenCode. This harness checks each one
against the fields the runtime's official docs define and enforces the invariant
that a bearer token is referenced from the environment, never written into a
tracked file. A planted literal token must fail, so the negative test guards the
guard.

Field references:
- Claude Code `.mcp.json`: `mcpServers.<name>` with `type: "http"`, `url`, `headers`;
  `${VAR}` expansion (https://code.claude.com/docs/en/mcp.md).
- Codex `.codex/config.toml`: `mcp_servers.<name>` with `url` and
  `bearer_token_env_var` (https://developers.openai.com/codex/mcp/).
- OpenCode `opencode.json`: `mcp.<name>` with `type: "remote"`, `url`, `enabled`,
  `headers`; `{env:VAR}` expansion (https://opencode.ai/docs/mcp-servers/).
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import pytest

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "integrations"
_ROOT = _EXAMPLES.parents[1]
_SERVER = "vera"

# Per-runtime environment-reference syntax for the Authorization header. A match
# proves the token is not a literal in the tracked file.
_HEADER_ENV_REF = {
    "claude-code": re.compile(r"^Bearer \$\{[A-Z0-9_]+(:-[^}]*)?\}$"),
    "opencode": re.compile(r"^Bearer \{env:[A-Z0-9_]+\}$"),
}


def _load(path: Path) -> dict[str, Any]:
    if path.suffix == ".toml":
        return tomllib.loads(path.read_text())
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
    if not _HEADER_ENV_REF["claude-code"].match(_auth_header(entry)):
        problems.append("Authorization must reference ${VAR}, not a literal token")
    return problems


def validate_codex(config: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    if config.get("approval_policy") != "on-request":
        problems.append("approval_policy must be 'on-request'")
    if config.get("approvals_reviewer") != "user":
        problems.append("approvals_reviewer must be 'user'")
    if config.get("features", {}).get("hooks") is not True:
        problems.append("features.hooks must be true")
    servers = config.get("mcp_servers")
    if not isinstance(servers, dict) or _SERVER not in servers:
        return [*problems, f"mcp_servers.{_SERVER} is missing"]
    entry = servers[_SERVER]
    if not str(entry.get("url", "")).startswith("https://"):
        problems.append("url must be an https endpoint")
    if entry.get("bearer_token_env_var") != "VERA_MCP_TOKEN":
        problems.append("bearer_token_env_var must reference VERA_MCP_TOKEN")
    if "http_headers" in entry:
        problems.append("tracked Codex config must not carry literal HTTP headers")
    if entry.get("default_tools_approval_mode") != "writes":
        problems.append("default_tools_approval_mode must be 'writes'")
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
    if not _HEADER_ENV_REF["opencode"].match(_auth_header(entry)):
        problems.append("Authorization must reference {env:VAR}, not a literal token")
    permissions = config.get("permission")
    if not isinstance(permissions, dict):
        problems.append("permission rules are required")
    else:
        write_tools = {
            "vera_knowledge_get_context",
            "vera_knowledge_propose",
            "vera_knowledge_retract_proposal",
            "vera_knowledge_feedback",
            "vera_knowledge_create_snapshot",
            "vera_memory_propose",
            "vera_memory_feedback",
        }
        if any(permissions.get(tool) != "ask" for tool in write_tools):
            problems.append("every VERA write-capable tool must require ask")
    return problems


_VALIDATORS = {
    "claude-code": (validate_claude_code, _EXAMPLES / "claude-code" / ".mcp.json"),
    "codex": (validate_codex, _EXAMPLES / "codex" / "config.toml"),
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
    assert "knowledge_bootstrap" in skill
    assert "knowledge_proposal_report" in skill
    assert "knowledge_retract_proposal" in skill


def test_setup_skill_has_two_inputs_and_endpoint_smoke_checks() -> None:
    skill = (_EXAMPLES / "vera-project-setup" / "SKILL.md").read_text()
    assert skill.startswith("---"), "the setup skill needs YAML frontmatter"
    frontmatter = skill.split("---", 2)[1]
    assert "name: vera-project-setup" in frontmatter
    assert "description:" in frontmatter

    input_contract = skill.split("```yaml", 1)[1].split("```", 1)[0]
    required = re.findall(r"^  (VERA_[A-Z0-9_]+):", input_contract, flags=re.MULTILINE)
    assert required == ["VERA_API_URL", "VERA_MCP_URL"]

    preflight = (_EXAMPLES / "vera-project-setup" / "references" / "preflight.md").read_text()
    apply_spec = (_EXAMPLES / "vera-project-setup" / "references" / "apply.md").read_text()

    assert "${VERA_API_URL}/health/live" in preflight
    assert "${VERA_API_URL}/health/ready" in preflight
    assert "VERA_MCP_URL" in preflight
    assert "`OPTIONS` or `HEAD`" in preflight
    assert all(runtime in skill for runtime in ("Claude Code", "Codex", "OpenCode"))
    assert "references/preflight.md" in skill
    assert "references/apply.md" in skill
    assert "restart the coding tool" in skill
    assert "tools are visible" in skill
    assert "Report that VERA setup completed" in apply_spec
    assert len(skill.splitlines()) < 120
    assert all(
        detail not in skill
        for detail in (".claude/settings.json", ".codex/config.toml", "opencode.json")
    )


def test_runtime_setup_specs_are_small_and_runtime_specific() -> None:
    expected = {
        "claude-code": (".mcp.json", "permissionDecision", "claude mcp", "node --version"),
        "codex": (
            ".codex/config.toml",
            "default_tools_approval_mode",
            "codex mcp",
            "node --version",
        ),
        "opencode": ("opencode.json", 'permission.vera_* = "ask"', "opencode mcp"),
    }

    for runtime, fragments in expected.items():
        spec = (_EXAMPLES / runtime / "SPEC.md").read_text()
        assert len(spec.splitlines()) < 100
        assert "../vera-project-setup/SKILL.md" in spec
        assert all(fragment in spec for fragment in fragments)


def test_claude_code_hook_config_is_project_scoped_and_never_auto_approves() -> None:
    config = _load(_EXAMPLES / "claude-code" / "settings.json")
    hooks = config["hooks"]
    session_hook = hooks["SessionStart"][0]
    write_hook = hooks["PreToolUse"][0]

    assert session_hook["matcher"] == "startup|resume|clear|compact|fork"
    assert session_hook["hooks"][0]["command"] == "node"
    assert "${CLAUDE_PROJECT_DIR}/.claude/hooks/vera-hook.cjs" in session_hook["hooks"][0]["args"]
    assert write_hook["matcher"].startswith("^mcp__vera__")
    assert "knowledge_propose" in write_hook["matcher"]
    assert "knowledge_get_context" in write_hook["matcher"]

    script = (_EXAMPLES / "claude-code" / "vera-hook.cjs").read_text()
    assert 'permissionDecision: "ask"' in script
    assert 'permissionDecision: "allow"' not in script
    assert "execFileSync" in script
    assert "http://" not in script and "https://" not in script


def test_claude_code_hook_asks_only_for_vera_writes() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to execute the Claude Code hook reference")
    script = _EXAMPLES / "claude-code" / "vera-hook.cjs"

    def invoke(tool_name: str, tool_input: dict[str, Any]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603
            [node, str(script), "require-write-approval"],
            input=json.dumps({"tool_name": tool_name, "tool_input": tool_input}),
            text=True,
            capture_output=True,
            check=True,
            timeout=5,
        )

    for tool_input in ({}, {"persist": False}):
        ephemeral = invoke("mcp__vera__knowledge_get_context", tool_input)
        assert ephemeral.stdout == ""
        assert ephemeral.stderr == ""

    guarded_context_inputs = (
        {"persist": True},
        {"persist": "true"},
        {"persist": "false"},
        {"persist": 1},
        {"persist": None},
    )
    guarded_calls = [
        invoke("mcp__vera__knowledge_get_context", tool_input)
        for tool_input in guarded_context_inputs
    ]
    guarded_calls.append(invoke("mcp__vera__knowledge_propose", {"subject": "checkout-service"}))
    for write in guarded_calls:
        output = json.loads(write.stdout)
        assert output["hookSpecificOutput"]["hookEventName"] == "PreToolUse"
        assert output["hookSpecificOutput"]["permissionDecision"] == "ask"
        assert write.stderr == ""


@pytest.mark.parametrize("runtime", ["claude-code", "codex"])
def test_session_hook_sanitizes_git_metadata(tmp_path: Path, runtime: str) -> None:
    node = shutil.which("node")
    git = shutil.which("git")
    if node is None or git is None:
        pytest.skip("Node.js and Git are required to execute the session hook reference")
    script = _EXAMPLES / runtime / "vera-hook.cjs"

    subprocess.run(  # noqa: S603
        [git, "init", "-b", "feature/setup", str(tmp_path)],
        text=True,
        capture_output=True,
        check=True,
        timeout=5,
    )
    subprocess.run(  # noqa: S603
        [
            git,
            "-C",
            str(tmp_path),
            "remote",
            "add",
            "origin",
            "https://user:secret@GitHub.com/Org/Repo.git?token=hidden#fragment",
        ],
        text=True,
        capture_output=True,
        check=True,
        timeout=5,
    )
    result = subprocess.run(  # noqa: S603
        [node, str(script), "session-start"],
        input=json.dumps({"cwd": str(tmp_path), "transcript_path": "do-not-read.jsonl"}),
        text=True,
        capture_output=True,
        check=True,
        timeout=5,
    )

    output = json.loads(result.stdout)
    context = output["hookSpecificOutput"]["additionalContext"]
    assert output["hookSpecificOutput"]["hookEventName"] == "SessionStart"
    assert '"repository":"github.com/Org/Repo"' in context
    assert '"branch":"feature/setup"' in context
    assert "secret" not in context
    assert "hidden" not in context
    assert str(tmp_path) not in context
    assert "transcript" not in context
    assert result.stderr == ""

    subprocess.run(  # noqa: S603
        [git, "-C", str(tmp_path), "remote", "set-url", "origin", "C:/private/repo.git"],
        text=True,
        capture_output=True,
        check=True,
        timeout=5,
    )
    local_result = subprocess.run(  # noqa: S603
        [node, str(script), "session-start"],
        input=json.dumps({"cwd": str(tmp_path)}),
        text=True,
        capture_output=True,
        check=True,
        timeout=5,
    )
    local_context = json.loads(local_result.stdout)["hookSpecificOutput"]["additionalContext"]
    assert "no unique safe Git remote" in local_context
    assert "C:/private" not in local_context

    subprocess.run(  # noqa: S603
        [
            git,
            "-C",
            str(tmp_path),
            "remote",
            "set-url",
            "origin",
            f"https://example.com/{'a' * 1025}.git",
        ],
        text=True,
        capture_output=True,
        check=True,
        timeout=5,
    )
    long_result = subprocess.run(  # noqa: S603
        [node, str(script), "session-start"],
        input=json.dumps({"cwd": str(tmp_path)}),
        text=True,
        capture_output=True,
        check=True,
        timeout=5,
    )
    long_context = json.loads(long_result.stdout)["hookSpecificOutput"]["additionalContext"]
    assert "no unique safe Git remote" in long_context


def test_codex_hook_is_project_scoped_without_unsupported_write_ask() -> None:
    config = _load(_EXAMPLES / "codex" / "config.toml")
    assert validate_codex(config) == []

    hooks = _load(_EXAMPLES / "codex" / "hooks.json")["hooks"]
    assert set(hooks) == {"SessionStart"}
    command = hooks["SessionStart"][0]["hooks"][0]
    assert ".codex/hooks/vera-hook.cjs" in command["command"]
    assert ".codex/hooks/vera-hook.cjs" in command["commandWindows"]
    assert command["additionalContextLimit"] >= 2048

    script = (_EXAMPLES / "codex" / "vera-hook.cjs").read_text()
    assert "permissionDecision" not in script
    assert "execFileSync" in script
    assert "http://" not in script and "https://" not in script


def test_opencode_plugin_is_local_bootstrap_only() -> None:
    plugin = (_EXAMPLES / "opencode" / "vera.ts").read_text()
    assert '"chat.message"' in plugin
    assert "remindedSessions" in plugin
    assert "vera_knowledge_bootstrap" in plugin
    assert "tool.execute.before" not in plugin
    assert "^[A-Za-z]:" in plugin
    assert "http://" not in plugin and "https://" not in plugin


def test_coding_tool_guide_starts_with_one_shared_setup_prompt() -> None:
    guide = (_ROOT / "docs" / "integrations" / "coding-tools.md").read_text()
    assert guide.index("## Fast path") < guide.index("## Prerequisites")
    assert guide.count("```text") == 1
    prompt = guide.split("```text", 1)[1].split("```", 1)[0]

    assert "VERA_API_URL=http://localhost:8000" in prompt
    assert "VERA_MCP_URL=http://localhost:8080/mcp" in prompt
    assert "examples/integrations/vera-project-setup/SKILL.md" in prompt
    assert "matching project-local runtime spec" in prompt
    assert "Smoke test the API and MCP URLs" in prompt
    assert re.search(r"restart the\s+coding tool", prompt)
    assert "tools are visible" in prompt
    assert all(runtime in guide for runtime in ("OpenCode", "Codex", "Claude Code"))
