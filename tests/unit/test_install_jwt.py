"""The fallback installer keeps credentials out of agent-visible commands."""

from __future__ import annotations

import getpass
import os
import runpy
import shutil
import stat
import subprocess
import types
import warnings
from pathlib import Path
from typing import Any

import pytest

_MCP_URL = "https://mcp.vera.example/mcp"
_SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "examples"
    / "integrations"
    / "vera-project-setup"
    / "install_jwt.py"
)


def _functions() -> dict[str, Any]:
    return runpy.run_path(str(_SCRIPT), run_name="vera_install_jwt")


def _git_repo(path: Path) -> None:
    git = shutil.which("git")
    assert git is not None
    subprocess.run(  # noqa: S603
        [git, "init", "-q", str(path)],
        check=True,
        capture_output=True,
    )


def test_install_replaces_one_placeholder_only_in_untracked_config(tmp_path: Path) -> None:
    _git_repo(tmp_path)
    config = tmp_path / ".mcp.json"
    config.write_text(
        '{"mcpServers":{"vera":{"url":"https://mcp.vera.example/mcp",'
        '"headers":{"Authorization":"Bearer <VERA_MCP_JWT>"}}}}\n'
    )

    _functions()["_install"](config, "header.payload.signature", mcp_url=_MCP_URL)

    assert "Bearer header.payload.signature" in config.read_text()
    if os.name != "nt":
        assert stat.S_IMODE(config.stat().st_mode) & 0o077 == 0


def test_install_rotates_one_existing_literal_jwt_without_returning_it(tmp_path: Path) -> None:
    _git_repo(tmp_path)
    config = tmp_path / ".codex" / "config.toml"
    config.parent.mkdir()
    old_token = "old-header.old-payload.old-signature"  # noqa: S105
    other_token = "other-header.other-payload.other-signature"  # noqa: S105
    config.write_text(
        "[mcp_servers.other]\n"
        'url = "https://other.example/mcp"\n'
        f'http_headers = {{ Authorization = "Bearer {other_token}" }}\n'
        "[mcp_servers.vera]\n"
        f'url = "{_MCP_URL}"\n'
        f'http_headers = {{ Authorization = "Bearer {old_token}" }}\n'
    )

    _functions()["_install"](
        config,
        "new-header.new-payload.new-signature",
        mcp_url=_MCP_URL,
        rotate=True,
    )

    updated = config.read_text()
    assert old_token not in updated
    assert other_token in updated
    assert "Bearer new-header.new-payload.new-signature" in updated


def test_install_rotates_only_vera_in_jsonc_with_comments(tmp_path: Path) -> None:
    _git_repo(tmp_path)
    config = tmp_path / "opencode.jsonc"
    old_token = "old-header.old-payload.old-signature"  # noqa: S105
    other_token = "other-header.other-payload.other-signature"  # noqa: S105
    config.write_text(
        "{\n"
        "  // Keep this project comment.\n"
        '  "mcp": {\n'
        f'    "other": {{"url": "https://other.example/mcp", "headers": '
        f'{{"Authorization": "Bearer {other_token}"}}}},\n'
        f'    "vera": {{"url": "{_MCP_URL}", "headers": '
        f'{{"Authorization": "Bearer {old_token}"}}}},\n'
        "  },\n"
        "}\n"
    )

    _functions()["_install"](
        config,
        "new-header.new-payload.new-signature",
        mcp_url=_MCP_URL,
        rotate=True,
    )

    updated = config.read_text()
    assert "Keep this project comment" in updated
    assert old_token not in updated
    assert other_token in updated
    assert "Bearer new-header.new-payload.new-signature" in updated


def test_credential_requests_never_follow_redirects() -> None:
    handler = _functions()["_NoRedirectHandler"]()

    assert handler.redirect_request(None, None, 302, "Found", {}, "https://evil.example") is None


def test_install_refuses_a_mismatched_vera_url_without_mutation(tmp_path: Path) -> None:
    _git_repo(tmp_path)
    config = tmp_path / ".mcp.json"
    original = (
        '{"mcpServers":{"vera":{"url":"https://attacker.example/mcp",'
        '"headers":{"Authorization":"Bearer <VERA_MCP_JWT>"}}}}\n'
    )
    config.write_text(original)

    with pytest.raises(RuntimeError, match="does not match"):
        _functions()["_install"](
            config,
            "header.payload.signature",
            mcp_url=_MCP_URL,
        )

    assert config.read_text() == original


def test_hidden_prompt_fails_closed_without_a_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    functions = _functions()

    def unsafe_fallback(_prompt: str) -> str:
        warnings.warn("cannot disable echo", getpass.GetPassWarning, stacklevel=2)
        return "must-not-be-read"

    monkeypatch.setattr(getpass, "getpass", unsafe_fallback)
    with pytest.raises(RuntimeError, match="real terminal"):
        functions["_read_secret"]("Secret: ")


def test_windows_permissions_apply_a_restricted_dacl(monkeypatch: pytest.MonkeyPatch) -> None:
    restrict = _functions()["_restrict_permissions"]
    module_globals = restrict.__globals__
    monkeypatch.setitem(module_globals, "os", types.SimpleNamespace(name="nt"))
    monkeypatch.setitem(
        module_globals,
        "_windows_system_directory",
        lambda: Path("C:/Windows/System32"),
    )
    commands: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        stdout = '"DOMAIN\\user","S-1-5-21-42"\n' if command[0].endswith("whoami.exe") else ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout)

    monkeypatch.setattr(subprocess, "run", run)
    restrict(Path("credential.json"))

    assert commands[1][0].endswith("System32/icacls.exe")
    assert "/inheritance:r" in commands[1]
    assert "*S-1-5-21-42:F" in commands[1]
    assert "*S-1-1-0" in commands[1]


def test_install_refuses_a_tracked_config(tmp_path: Path) -> None:
    _git_repo(tmp_path)
    config = tmp_path / "opencode.json"
    config.write_text(
        '{"mcp":{"vera":{"url":"https://mcp.vera.example/mcp",'
        '"headers":{"Authorization":"Bearer <VERA_MCP_JWT>"}}}}\n'
    )
    git = shutil.which("git")
    assert git is not None
    subprocess.run(  # noqa: S603
        [git, "-C", str(tmp_path), "add", "opencode.json"],
        check=True,
        capture_output=True,
    )

    with pytest.raises(RuntimeError, match="tracked or staged"):
        _functions()["_install"](config, "header.payload.signature", mcp_url=_MCP_URL)


@pytest.mark.parametrize(
    ("url", "mcp"),
    [
        ("https://api.vera.example", False),
        ("https://mcp.vera.example/mcp", True),
        ("http://127.0.0.1:8080/mcp", True),
    ],
)
def test_safe_url_accepts_https_and_loopback(url: str, mcp: bool) -> None:
    assert _functions()["_safe_url"](url, mcp=mcp) == url


@pytest.mark.parametrize(
    "url",
    [
        "http://api.vera.example",
        "https://user:secret@mcp.vera.example/mcp",
        "https://mcp.vera.example/not-mcp",
    ],
)
def test_safe_url_rejects_unsafe_remote_values(url: str) -> None:
    with pytest.raises(ValueError):
        _functions()["_safe_url"](url, mcp=True)
