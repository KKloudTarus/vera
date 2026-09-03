"""Install a VERA fallback JWT without exposing credentials to an agent transcript."""

from __future__ import annotations

import argparse
import csv
import getpass
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import tomllib
import urllib.error
import urllib.request
import warnings
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

_PLACEHOLDER = "<VERA_MCP_JWT>"
_SCOPES = ["memory:read", "memory:propose", "memory:feedback", "memory:snapshot"]
_JWT_PATTERN = re.compile(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def _read_secret(prompt: str) -> str:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            return getpass.getpass(prompt)
    except getpass.GetPassWarning as exc:
        raise RuntimeError("a real terminal with hidden input is required") from exc


def _credential(*, existing_token: bool) -> str:
    env_name = "VERA_MCP_TOKEN" if existing_token else "VERA_API_KEY"
    value = os.environ.pop(env_name, None)
    if value is not None:
        return value
    return _read_secret("Existing MCP JWT: " if existing_token else "VERA API key: ")


def _safe_url(value: str, *, mcp: bool = False) -> str:
    url = value.rstrip("/")
    parsed = urlsplit(url)
    loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if parsed.scheme not in ({"http", "https"} if loopback else {"https"}):
        raise ValueError("URL must use HTTPS except on loopback")
    if not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("URL must be absolute and contain no credentials, query, or fragment")
    if mcp and not parsed.path.endswith("/mcp"):
        raise ValueError("MCP URL must end in /mcp")
    return url


def _request_json(
    request: urllib.request.Request,
    *,
    failure: str,
) -> tuple[dict[str, Any], Any]:
    try:
        with _OPENER.open(request, timeout=15) as response:
            payload = json.loads(response.read())
            return payload, response.headers
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
        json.JSONDecodeError,
    ) as exc:
        raise RuntimeError(failure) from exc


def _exchange(api_url: str, api_key: str) -> tuple[str, int | None]:
    request = urllib.request.Request(  # noqa: S310
        f"{api_url}/identity/mcp-token",
        data=json.dumps({"scopes": _SCOPES}).encode(),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    payload, headers = _request_json(request, failure="MCP token issuance failed")
    token = payload.get("access_token")
    expires_in = payload.get("expires_in")
    if (
        not isinstance(token, str)
        or not token
        or "expires_in" not in payload
        or (expires_in is not None and not isinstance(expires_in, int))
    ):
        raise RuntimeError("MCP token response is incomplete")
    if "no-store" not in headers.get("Cache-Control", ""):
        raise RuntimeError("MCP token response is missing Cache-Control: no-store")
    return token, expires_in


def _probe(mcp_url: str, token: str) -> None:
    request = urllib.request.Request(  # noqa: S310
        mcp_url,
        data=json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "vera-project-setup", "version": "1"},
                },
            }
        ).encode(),
        headers={
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    payload, _ = _request_json(request, failure="Authenticated MCP initialize failed")
    if payload.get("jsonrpc") != "2.0" or "result" not in payload:
        raise RuntimeError("Authenticated MCP initialize returned an invalid response")


def _parse_jsonc(content: str) -> dict[str, Any]:
    stripped: list[str] = []
    index = 0
    in_string = False
    escaped = False
    while index < len(content):
        char = content[index]
        following = content[index + 1] if index + 1 < len(content) else ""
        if in_string:
            stripped.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            stripped.append(char)
            index += 1
            continue
        if char == "/" and following == "/":
            index += 2
            while index < len(content) and content[index] not in "\r\n":
                index += 1
            continue
        if char == "/" and following == "*":
            index += 2
            while index + 1 < len(content) and content[index : index + 2] != "*/":
                if content[index] in "\r\n":
                    stripped.append(content[index])
                index += 1
            if index + 1 >= len(content):
                raise json.JSONDecodeError("unterminated comment", content, index)
            index += 2
            continue
        if char in "}]":
            previous = len(stripped) - 1
            while previous >= 0 and stripped[previous].isspace():
                previous -= 1
            if previous >= 0 and stripped[previous] == ",":
                del stripped[previous]
        stripped.append(char)
        index += 1
    return json.loads("".join(stripped))


def _vera_config(content: str, config_path: Path) -> tuple[str, str]:
    try:
        if config_path.suffix == ".toml":
            config = tomllib.loads(content)
            entry = config["mcp_servers"]["vera"]
            authorization = entry["http_headers"]["Authorization"]
        else:
            config = _parse_jsonc(content)
            servers = config.get("mcpServers") or config.get("mcp")
            entry = servers["vera"]
            authorization = entry["headers"]["Authorization"]
    except (
        json.JSONDecodeError,
        tomllib.TOMLDecodeError,
        AttributeError,
        KeyError,
        TypeError,
    ) as exc:
        raise RuntimeError("config has no parseable vera Authorization header") from exc
    if not isinstance(authorization, str):
        raise RuntimeError("vera Authorization header must be a string")
    url = entry.get("url")
    if not isinstance(url, str):
        raise RuntimeError("vera MCP URL must be a string")
    return authorization, url


def _windows_system_directory() -> Path:
    import ctypes

    buffer = ctypes.create_unicode_buffer(32768)
    length = ctypes.windll.kernel32.GetWindowsDirectoryW(buffer, len(buffer))
    if length == 0 or length >= len(buffer):
        raise OSError("could not resolve the Windows system directory")
    return Path(buffer.value) / "System32"


def _restrict_permissions(path: Path) -> None:
    if os.name != "nt":
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        if stat.S_IMODE(path.stat().st_mode) & 0o077:
            raise RuntimeError("could not restrict credential config to its owner")
        return

    system32 = _windows_system_directory()
    icacls = system32 / "icacls.exe"
    whoami = system32 / "whoami.exe"
    identity = subprocess.run(  # noqa: S603
        [str(whoami), "/user", "/fo", "csv", "/nh"],
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    if identity.returncode != 0 or not identity.stdout.strip():
        raise RuntimeError("could not resolve the current Windows identity")
    try:
        sid = next(csv.reader([identity.stdout.strip()]))[1]
    except (IndexError, csv.Error) as exc:
        raise RuntimeError("could not parse the current Windows identity") from exc
    secured = subprocess.run(  # noqa: S603
        [
            str(icacls),
            str(path),
            "/inheritance:r",
            "/grant:r",
            f"*{sid}:F",
            "/remove:g",
            "*S-1-1-0",
            "*S-1-5-11",
            "*S-1-5-32-545",
        ],
        capture_output=True,
        check=False,
        timeout=10,
    )
    if secured.returncode != 0:
        raise RuntimeError("could not apply an owner-only Windows ACL to credential config")


def _install(config_path: Path, token: str, *, mcp_url: str, rotate: bool = False) -> None:
    config_path = config_path.resolve()
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("Git is required to verify that the credential config is untracked")
    root_result = subprocess.run(  # noqa: S603
        [git, "-C", str(config_path.parent), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=False,
    )
    if root_result.returncode != 0:
        raise RuntimeError("credential config must be inside a Git repository")
    root = Path(root_result.stdout.strip()).resolve()
    try:
        relative_path = config_path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError("credential config must be inside its Git repository") from exc
    tracked = subprocess.run(  # noqa: S603
        [git, "-C", str(root), "ls-files", "--error-unmatch", "--", str(relative_path)],
        capture_output=True,
        check=False,
    )
    if tracked.returncode == 0:
        raise RuntimeError("refusing to write a JWT into a tracked or staged config")

    content = config_path.read_text()
    authorization, configured_url = _vera_config(content, config_path)
    if _safe_url(configured_url, mcp=True) != mcp_url:
        raise RuntimeError("vera config URL does not match the authenticated MCP URL")
    if not rotate and authorization == f"Bearer {_PLACEHOLDER}":
        previous = authorization
    elif rotate and authorization.startswith("Bearer "):
        previous_token = authorization.removeprefix("Bearer ")
        if _JWT_PATTERN.fullmatch(previous_token) is None:
            raise RuntimeError("rotation requires an existing literal bearer JWT")
        previous = authorization
    else:
        action = "literal bearer JWT" if rotate else _PLACEHOLDER
        raise RuntimeError(f"vera Authorization must contain exactly one {action}")
    if content.count(previous) != 1:
        raise RuntimeError("vera Authorization value is ambiguous in config")
    updated = content.replace(previous, f"Bearer {token}", 1)
    with tempfile.NamedTemporaryFile(dir=config_path.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        _restrict_permissions(temporary_path)
        temporary_path.write_text(updated)
        os.replace(temporary_path, config_path)
        _restrict_permissions(config_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--mcp-url", required=True)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument(
        "--existing-token",
        action="store_true",
        help="prompt for an existing MCP JWT instead of a VERA API key",
    )
    parser.add_argument(
        "--rotate",
        action="store_true",
        help="replace the config's single existing literal bearer JWT",
    )
    args = parser.parse_args()

    try:
        api_url = _safe_url(args.api_url)
        mcp_url = _safe_url(args.mcp_url, mcp=True)
        secret = _credential(existing_token=args.existing_token)
        if not secret:
            raise RuntimeError("credential is required")
        if args.existing_token:
            token, expires_in = secret, None
        else:
            token, expires_in = _exchange(api_url, secret)
        _probe(mcp_url, token)
        _install(args.config, token, mcp_url=mcp_url, rotate=args.rotate)
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(1, f"VERA JWT install failed: {exc}\n")

    lifetime = f"; expires in {expires_in} seconds" if expires_in else "; non-expiring"
    print(f"VERA JWT fallback installed and MCP authentication verified{lifetime}.")


if __name__ == "__main__":
    main()
