"""Build a source connector from a config spec (for scheduled worker sync).

Keeps connector construction in one place so the worker can turn declarative specs into
live connectors. Filesystem and git need only a local path; the HTTP connectors take a
base URL and an optional bearer token.
"""

from __future__ import annotations

import os
from typing import Any, cast

from vera.adapters.connectors.cmdb import CmdbConnector, RecordsProvider
from vera.adapters.connectors.confluence import ConfluenceConnector
from vera.adapters.connectors.filesystem import FilesystemConnector
from vera.adapters.connectors.git import GitConnector
from vera.adapters.connectors.jira import JiraConnector
from vera.adapters.connectors.pdf import PdfConnector
from vera.adapters.connectors.slack import SlackConnector
from vera.domain.ports.connectors import SourceConnector
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


def _cmdb_records_provider(spec: dict[str, Any]) -> RecordsProvider:
    """Fetch configuration items from an HTTP endpoint that returns the CMDB as JSON.

    Accepts a raw array of CIs, or an object wrapping the array under ``items``,
    ``records``, ``configuration_items``, or ``cis``. The endpoint is expected to return
    the full current set; ``CmdbConnector`` filters by the ``updated_at`` watermark, so a
    plain export URL works even without server-side incremental support. Each CI is
    ``{id, name, type?, updated_at?, relations: [{predicate, object}]}``.
    """
    client = _http_client(spec)
    url = str(spec.get("url") or spec.get("base_url") or "")
    if not url:
        raise VeraError("cmdb connector needs 'url' (or 'base_url') to fetch configuration items")

    async def _provider() -> list[JsonDict]:
        response = await client.get(url)
        response.raise_for_status()
        payload: Any = response.json()
        rows: list[Any] = []
        if isinstance(payload, list):
            rows = cast("list[Any]", payload)
        elif isinstance(payload, dict):
            wrapper = cast("dict[str, Any]", payload)
            for key in ("items", "records", "configuration_items", "cis"):
                value = wrapper.get(key)
                if isinstance(value, list):
                    rows = cast("list[Any]", value)
                    break
        return [cast("JsonDict", item) for item in rows if isinstance(item, dict)]

    return _provider


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
        return CmdbConnector(_cmdb_records_provider(spec))
    raise ValueError(f"unknown connector kind: {kind!r}")
