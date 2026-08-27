"""Build a source connector from a config spec (for scheduled worker sync).

Keeps connector construction in one place so the worker can turn declarative specs into
live connectors. Filesystem and git need only a local path; the HTTP connectors take a
base URL and an optional bearer token.
"""

from __future__ import annotations

import os
from typing import Any

from vera.adapters.connectors.confluence import ConfluenceConnector
from vera.adapters.connectors.filesystem import FilesystemConnector
from vera.adapters.connectors.git import GitConnector
from vera.adapters.connectors.jira import JiraConnector
from vera.adapters.connectors.pdf import PdfConnector
from vera.adapters.connectors.slack import SlackConnector
from vera.domain.ports.connectors import ConnectorBatch, SourceConnector
from vera.shared.errors import VeraError
from vera.shared.types import JsonDict


def _resolve_token(spec: dict[str, Any]) -> str | None:
    """The connector's bearer token, from an environment variable named by ``token_env``
    (preferred, so secrets stay out of the specs blob and logs) or an inline ``token``.
    """
    env_name = spec.get("token_env")
    if env_name:
        token = os.environ.get(str(env_name))
        if not token:
            raise VeraError(f"connector token env {env_name!r} is not set")
        return token
    inline = spec.get("token")
    return str(inline) if inline else None


def _http_client(spec: dict[str, Any]) -> Any:
    import httpx

    token = _resolve_token(spec)
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.AsyncClient(headers=headers, timeout=30.0)


class _EmptyConnector:
    """Fallback for an unconfigured/empty CMDB spec (no records provider)."""

    @property
    def kind(self) -> str:
        return "cmdb"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        return ConnectorBatch(records=(), next_cursor=cursor or {})


def build_connector(spec: dict[str, Any]) -> SourceConnector:
    kind = str(spec.get("kind", ""))
    if kind == "filesystem":
        return FilesystemConnector(str(spec["root"]))
    if kind == "git":
        return GitConnector(str(spec["repo_path"]))
    if kind == "pdf":
        return PdfConnector(str(spec["directory"]))
    if kind == "confluence":
        return ConfluenceConnector(
            _http_client(spec), base_url=str(spec["base_url"]), space_key=str(spec["space_key"])
        )
    if kind == "jira":
        return JiraConnector(
            _http_client(spec), base_url=str(spec["base_url"]), project_key=str(spec["project_key"])
        )
    if kind == "slack":
        return SlackConnector(
            _http_client(spec), base_url=str(spec["base_url"]), channel_id=str(spec["channel_id"])
        )
    if kind == "cmdb":
        return _EmptyConnector()
    raise ValueError(f"unknown connector kind: {kind!r}")
