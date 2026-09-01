"""Safe, stable repository identities for client-to-project discovery."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

_WINDOWS_PATH = re.compile(r"^[A-Za-z]:")
_SCP_REMOTE = re.compile(r"^(?:[^@/\s]+@)?(?P<host>[^:/\s]+):(?P<path>.+)$")
_AUTHORITY_WITH_PORT = re.compile(r"^(?:\[[0-9A-Fa-f:.]+\]|[^@:/\s]+):[0-9]+/.+$")


def canonical_repository_ref(value: str | None) -> str | None:
    """Remove credentials and local-only details from a Git repository reference.

    Remote URLs collapse to ``host/path``. Opaque connector names such as ``vera`` remain
    usable, while local paths are deliberately not turned into server-visible identities.
    """
    if value is None:
        return None
    raw = value.strip().split("?", 1)[0].split("#", 1)[0].strip()
    if not raw:
        return None
    lowered = raw.lower()
    if (
        lowered.startswith(("file:", "/", "./", "../", "~"))
        or _WINDOWS_PATH.match(raw)
        or "\\" in raw
    ):
        return None
    if "://" not in raw and not any(separator in raw for separator in ("/", "\\", ":")):
        return raw[:-4] if raw.lower().endswith(".git") else raw

    colon = raw.find(":")
    at = raw.find("@")
    if "://" not in raw and 0 <= colon < at:
        return None

    has_canonical_port = "://" not in raw and _AUTHORITY_WITH_PORT.match(raw) is not None
    scp = _SCP_REMOTE.match(raw) if "://" not in raw and not has_canonical_port else None
    host: str | None = None
    port: int | None = None
    if scp is not None:
        host = scp.group("host").lower()
        path = scp.group("path")
    else:
        try:
            parsed = urlsplit(raw if "://" in raw else f"//{raw}")
            host = parsed.hostname.lower() if parsed.hostname else None
            port = parsed.port
        except ValueError:
            return None
        if parsed.scheme and parsed.scheme.lower() not in {"git", "http", "https", "ssh"}:
            return None
        if "://" in raw and host is None:
            return None
        path = parsed.path if host else raw

    parts = [part for part in path.replace("\\", "/").split("/") if part and part != "."]
    if not parts or ".." in parts:
        return None
    if parts[-1].lower().endswith(".git"):
        parts[-1] = parts[-1][:-4]
    if not parts[-1]:
        return None
    repository_path = "/".join(parts)
    if host is None:
        return repository_path
    normalized_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    authority = f"{normalized_host}:{port}" if port is not None else normalized_host
    return f"{authority}/{repository_path}"
