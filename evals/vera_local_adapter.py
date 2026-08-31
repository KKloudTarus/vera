"""JSON-stdio evaluator adapter for a dedicated, disposable VERA stack."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import os
import re
import socket
import ssl
import sys
import tempfile
import time
from collections import Counter
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import cache
from itertools import product
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import aioboto3
import httpx
import httpx2
import jwt
from botocore.config import Config
from botocore.exceptions import ClientError
from jsonschema import Draft202012Validator, FormatChecker
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from neo4j import AsyncGraphDatabase
from redis.asyncio import Redis
from sqlalchemy import text

from evals.generate_load_fixture import (
    GENERATOR_VERSION,
    canonical_line,
    query_for,
    records,
)
from evals.generate_load_fixture import fact as load_fact
from evals.validate import fixture_data, load_cases
from vera.adapters.persistence.repositories.projection import SqlAlchemyProjectionSource
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.connectors import SyncRunner
from vera.application.curation.service import CurationService, IngestArtifact, IngestResult
from vera.application.projection import FactProjectionService
from vera.application.queries.calibration import CalibrationService
from vera.bootstrap import (
    Container,
    build_container,
    build_rerank_weights,
    dispose_container,
)
from vera.config.settings import Settings, active_embedding, get_settings
from vera.domain.knowledge.models import SourceKind
from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.domain.ports.curation import ClaimExtractor, ExtractedClaim
from vera.shared.errors import Ok
from vera.shared.types import JsonDict

_STATE_ROOT = Path(
    os.environ.get("VERA_EVAL_STATE_ROOT", Path(tempfile.gettempdir()) / "vera-eval-state")
)
_CASES = {case["case_id"]: case for case in load_cases()}
_SAFE_DATABASE_TABLES = frozenset({"alembic_version", "ontology_versions"})
_DEFAULT_TIMEOUT_S = 60.0
_HTTP_SSL_CONTEXT = ssl.create_default_context()
_ISO_INSTANT = re.compile(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\b")
_SEARCH_MATRIX = {
    "entrypoints": ["http", "mcp"],
    "cache_states": ["cold", "warm"],
    "scope_counts": [1, 5, 20],
    "result_limits": [5, 10, 50],
    "virtual_users": [1, 20],
}
_INGESTION_MATRIX = {"record_counts": [100, 500], "concurrency": [1, 8]}


class AdapterBlocked(RuntimeError):
    """A required external or product boundary is unavailable."""


class AdapterFailed(RuntimeError):
    """A boundary responded, but the observed product behavior failed."""


def _exception_type_summary(exc: BaseException) -> str:
    pending = [exc]
    leaves: set[str] = set()
    while pending:
        current = pending.pop()
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)
        else:
            leaves.add(type(current).__name__)
    suffix = f"[{','.join(sorted(leaves))}]" if leaves else ""
    return f"{type(exc).__name__}{suffix}"


class _FixtureSyncFailure(RuntimeError):
    pass


class _DisabledExtractor:
    """Evaluation control seam that stores artifacts without deriving claims."""

    provider = "disabled"
    model = "none"

    async def extract(
        self, *, body: str, knowledge_type: str, metadata: JsonDict
    ) -> list[ExtractedClaim]:
        return []


@dataclass(slots=True)
class _Outcome:
    status: str = "PASS"
    observations: dict[str, Any] = field(default_factory=dict)
    metrics: list[dict[str, Any]] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    message: str = ""
    boundaries: tuple[str, ...] = ("database",)


@dataclass(frozen=True, slots=True)
class _ModelResult:
    payload: dict[str, Any]
    model_id: str
    latency_ms: float
    usage: dict[str, int]
    cost_usd: float | None


class _FixtureConnector:
    kind = "evaluation-fixture"

    def __init__(
        self,
        pages: tuple[tuple[str, ...], ...],
        *,
        fail_on_page: int | None,
    ) -> None:
        self._pages = pages
        self._fail_on_page = fail_on_page

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        page_index = int(cursor.get("page", 0)) if cursor is not None else 0
        if page_index >= len(self._pages):
            return ConnectorBatch(records=(), next_cursor={"page": page_index})
        page_number = page_index + 1
        if self._fail_on_page == page_number:
            raise _FixtureSyncFailure(f"fixture connector failed on page {page_number}")
        records = tuple(
            ConnectorRecord(external_id=value, body=f"connector record {value}")
            for value in self._pages[page_index]
        )
        return ConnectorBatch(
            records=records,
            next_cursor={"page": page_number},
            has_more=page_number < len(self._pages),
        )


def _timeout(name: str, default: float = _DEFAULT_TIMEOUT_S) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise AdapterBlocked(f"{name} must be a positive number") from exc
    if value <= 0:
        raise AdapterBlocked(f"{name} must be a positive number")
    return value


def _bounded_concurrency(name: str, default: int, *, maximum: int = 128) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise AdapterBlocked(f"{name} must be a positive integer") from exc
    if value < 1 or value > maximum:
        raise AdapterBlocked(f"{name} must be between 1 and {maximum}")
    return value


def _validated_matrix_list(
    matrix: dict[str, Any], key: str, expected: list[str] | list[int]
) -> list[str] | list[int]:
    value = matrix.get(key)
    if not isinstance(value, list) or value != expected:
        raise AdapterBlocked(f"load matrix {key} must equal {expected!r}")
    if any(isinstance(item, bool) for item in value):
        raise AdapterBlocked(f"load matrix {key} contains a boolean")
    return value


def _generator_parameters(
    fixture: dict[str, Any], *, seed_override: Any = None
) -> tuple[int, int, int, int, str]:
    generator = fixture.get("generator")
    if not isinstance(generator, dict):
        raise AdapterBlocked("load fixture generator metadata is missing")
    if generator.get("version") != GENERATOR_VERSION:
        raise AdapterBlocked("load fixture generator version does not match the executable")
    values = {
        "scope_count": generator.get("scope_count"),
        "facts_per_scope": generator.get("facts_per_scope"),
        "query_count": generator.get("query_count"),
        "seed": generator.get("seed") if seed_override is None else seed_override,
    }
    for name, value in values.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise AdapterBlocked(f"load fixture generator {name} must be a positive integer")
    corpus_sha256 = generator.get("corpus_sha256")
    if not isinstance(corpus_sha256, str) or len(corpus_sha256) != 64:
        raise AdapterBlocked("load fixture generator corpus_sha256 is invalid")
    return (
        cast(int, values["scope_count"]),
        cast(int, values["facts_per_scope"]),
        cast(int, values["query_count"]),
        cast(int, values["seed"]),
        corpus_sha256,
    )


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise AdapterBlocked("evaluation timestamps must include a UTC offset")
    return parsed


def _format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat().replace("+00:00", "Z")


def _state_path(run_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", run_id)
    return _STATE_ROOT / f"{safe}.json"


def _load_state(run_id: str) -> dict[str, Any]:
    path = _state_path(run_id)
    if not path.exists():
        return {"cases": {}, "resources": []}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AdapterBlocked("evaluation adapter state must be an object")
    return cast(dict[str, Any], value)


def _save_state(run_id: str, state: dict[str, Any]) -> None:
    path = _state_path(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def _case_state(request: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    case_id = str(request["case_id"])
    cases = state.setdefault("cases", {})
    current = cases.setdefault(case_id, {})
    if "slug" not in current:
        digest = hashlib.sha256(f"{request['run_id']}:{case_id}".encode()).hexdigest()[:20]
        current.update(
            {
                "slug": f"eval-{digest}",
                "ingest_count": 0,
                "principals": {},
                "sources": {},
                "searches": {},
            }
        )
    return cast(dict[str, Any], current)


def _set_path(root: dict[str, Any], path: str, value: Any) -> None:
    current = root
    parts = path.split(".")
    for index, part in enumerate(parts):
        if index == len(parts) - 1:
            current[part] = copy.deepcopy(value)
            return
        child = current.get(part)
        if not isinstance(child, dict):
            child = {}
            current[part] = child
        current = child


def _deep_merge(target: dict[str, Any], incoming: dict[str, Any]) -> None:
    for key, value in incoming.items():
        existing = target.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            _deep_merge(existing, value)
        else:
            target[key] = copy.deepcopy(value)


def _metric(
    name: str,
    value: float | int,
    *,
    sample_size: int,
    unit: str,
    dimensions: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "dimensions": dimensions or {},
        "unit": unit,
        "value": value,
        "sample_size": sample_size,
    }


def _declared_observations(full: dict[str, Any], declared: list[str]) -> dict[str, Any]:
    roots = {re.split(r"[.[]", path, maxsplit=1)[0] for path in declared}
    return {root: copy.deepcopy(full[root]) for root in roots if root in full}


def _step_labels(request: dict[str, Any]) -> list[str]:
    requested = request.get("evidence_labels")
    if isinstance(requested, list) and all(isinstance(label, str) for label in requested):
        return sorted(set(cast(list[str], requested)))
    case = _CASES.get(str(request["case_id"]))
    if case is None:
        return []
    roots = {re.split(r"[.[]", path, maxsplit=1)[0] for path in request["observe"]}
    return sorted(
        {
            label
            for assertion in case["assertions"]
            if re.split(r"[.[]", assertion["target"], maxsplit=1)[0] in roots
            for label in assertion["evidence"]
        }
    )


def _required_url(name: str) -> str:
    value = os.environ.get(name, "").rstrip("/")
    if not value:
        raise AdapterBlocked(f"{name} is required")
    return value


def _http_client(
    *, headers: dict[str, str] | None = None, timeout_s: float | None = None
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=headers,
        timeout=httpx.Timeout(timeout_s or _DEFAULT_TIMEOUT_S),
        follow_redirects=False,
        verify=_HTTP_SSL_CONTEXT,
    )


@cache
def _resolved_http_url(url: str) -> tuple[str, str | None]:
    parsed = urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname is None or parsed.username is not None:
        return url, None
    try:
        address = socket.gethostbyname(parsed.hostname)
    except OSError:
        return url, None
    netloc = address if parsed.port is None else f"{address}:{parsed.port}"
    return urlunsplit(parsed._replace(netloc=netloc)), parsed.netloc


async def _configure_graph_dependency(state: str) -> dict[str, Any]:
    if state not in {"available", "unavailable"}:
        raise AdapterFailed(f"unsupported graph dependency state: {state}")
    base_url = os.environ.get("VERA_EVAL_DEPENDENCY_CONTROL_URL", "").rstrip("/")
    if not base_url.startswith(("http://", "https://")):
        raise AdapterBlocked("graph dependency control URL is not configured")
    proxy_url = f"{base_url}/proxies/neo4j"
    toxic_url = f"{proxy_url}/toxics/eval-graph-outage"
    started = time.perf_counter()
    try:
        async with _http_client(timeout_s=5.0) as client:
            removed = await client.delete(toxic_url)
            if removed.status_code not in {204, 404}:
                raise AdapterFailed(
                    f"graph dependency control rejected reset with HTTP {removed.status_code}"
                )
            if state == "unavailable":
                created = await client.post(
                    f"{proxy_url}/toxics",
                    json={
                        "name": "eval-graph-outage",
                        "type": "reset_peer",
                        "stream": "downstream",
                        "toxicity": 1.0,
                        "attributes": {"timeout": 0},
                    },
                )
                if created.status_code not in {200, 201}:
                    raise AdapterFailed(
                        "graph dependency control rejected outage injection "
                        f"with HTTP {created.status_code}"
                    )
            inspected = await client.get(proxy_url)
            if inspected.status_code != 200:
                raise AdapterFailed(
                    f"graph dependency control inspection returned HTTP {inspected.status_code}"
                )
            payload = inspected.json()
    except httpx.HTTPError as exc:
        raise AdapterBlocked(f"graph dependency control is unreachable: {exc}") from exc
    if not isinstance(payload, dict):
        raise AdapterFailed("graph dependency control returned an invalid proxy document")
    toxics = payload.get("toxics")
    toxic_names = (
        {str(item.get("name")) for item in toxics if isinstance(item, dict)}
        if isinstance(toxics, list)
        else set()
    )
    effective_state = "unavailable" if "eval-graph-outage" in toxic_names else "available"
    if effective_state != state:
        raise AdapterFailed(
            f"graph dependency control state mismatch: requested {state}, got {effective_state}"
        )
    return {
        "dependency": "graph",
        "requested_state": state,
        "effective_state": effective_state,
        "changed_at": _format_time(datetime.now(UTC)),
        "control_latency_ms": round((time.perf_counter() - started) * 1000, 3),
    }


async def _api_json(
    method: str,
    path: str,
    *,
    api_key: str | None = None,
    body: dict[str, Any] | None = None,
    expected: set[int],
) -> tuple[httpx.Response, dict[str, Any]]:
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else None
    async with _http_client(headers=headers) as client:
        try:
            response = await client.request(
                method, f"{_required_url('VERA_EVAL_API_URL')}{path}", json=body
            )
        except httpx.TimeoutException as exc:
            raise AdapterBlocked(f"API request to {path} reached its bounded timeout") from exc
        except httpx.HTTPError as exc:
            raise AdapterBlocked(f"API transport for {path} is unavailable") from exc
    if response.status_code not in expected:
        raise AdapterFailed(f"API {path} returned HTTP {response.status_code}")
    if response.status_code == 204 or not response.content:
        return response, {}
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise AdapterFailed(f"API {path} did not return JSON") from exc
    if not isinstance(payload, dict):
        raise AdapterFailed(f"API {path} did not return a JSON object")
    return response, cast(dict[str, Any], payload)


async def _ensure_scope(
    request: dict[str, Any], current: dict[str, Any], alias: str = "default"
) -> dict[str, Any]:
    principals = cast(dict[str, Any], current.setdefault("principals", {}))
    existing = principals.get(alias)
    if isinstance(existing, dict) and existing.get("api_key"):
        if alias == "default":
            current["group_id"] = existing["group_id"]
        return cast(dict[str, Any], existing)

    suffix = "" if alias == "default" else f"-{alias}"
    slug = f"{current['slug']}{suffix}"
    _, registered = await _api_json(
        "POST",
        "/identity/register",
        body={"display_name": f"Evaluation {request['case_id']} {alias}"},
        expected={201},
    )
    api_key = registered.get("api_key")
    principal_id = registered.get("principal_id")
    if not isinstance(api_key, str) or not api_key or not isinstance(principal_id, str):
        raise AdapterFailed("identity registration omitted the principal or API key")
    _, organization = await _api_json(
        "POST",
        "/identity/orgs",
        api_key=api_key,
        body={"name": f"Evaluation {request['case_id']} {alias}", "slug": slug},
        expected={201},
    )
    _, workspace = await _api_json(
        "POST",
        "/identity/workspaces",
        api_key=api_key,
        body={
            "org_id": organization.get("id"),
            "name": f"Evaluation {request['case_id']} {alias}",
            "slug": slug,
        },
        expected={201},
    )
    _, project = await _api_json(
        "POST",
        "/identity/projects",
        api_key=api_key,
        body={
            "workspace_id": workspace.get("id"),
            "name": f"Evaluation {request['case_id']} {alias}",
            "slug": slug,
        },
        expected={201},
    )
    required = {
        "principal_id": principal_id,
        "api_key": api_key,
        "organization_id": organization.get("id"),
        "workspace_id": workspace.get("id"),
        "project_id": project.get("id"),
        "group_id": project.get("group_id"),
    }
    if not all(isinstance(value, str) and value for value in required.values()):
        raise AdapterFailed("identity setup returned an incomplete scope")
    principals[alias] = required
    if alias == "default":
        current["group_id"] = required["group_id"]
    return required


def _principal(current: dict[str, Any], alias: str) -> dict[str, Any]:
    value = cast(dict[str, Any], current.get("principals", {})).get(alias)
    if not isinstance(value, dict) or not isinstance(value.get("api_key"), str):
        raise AdapterBlocked(f"principal {alias!r} has not been created by the fixture adapter")
    return cast(dict[str, Any], value)


async def _ensure_source(
    container: Container,
    request: dict[str, Any],
    current: dict[str, Any],
    *,
    trust_tier: int = 1,
    name: str | None = None,
    kind: str = "filesystem",
    principal_alias: str = "default",
) -> str:
    scope = await _ensure_scope(request, current, principal_alias)
    try:
        source_kind = SourceKind(kind).value
    except ValueError as exc:
        raise AdapterFailed(
            f"source kind {kind!r} is unsupported by the operator boundary"
        ) from exc
    key = f"{principal_alias}:{name or 'default'}:{source_kind}:{trust_tier}"
    sources = cast(dict[str, Any], current.setdefault("sources", {}))
    existing = sources.get(key)
    if isinstance(existing, str):
        current["default_source_id"] = existing
        return existing
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(str(scope["group_id"]))
        source_id = await uow.sources.create(
            workspace_id=UUID(str(scope["workspace_id"])),
            project_id=UUID(str(scope["project_id"])),
            kind=source_kind,
            name=name or f"eval-{request['case_id']}-source",
            trust_tier=trust_tier,
        )
        await uow.commit()
    source = str(source_id)
    sources[key] = source
    current["default_source_id"] = source
    return source


def _record_payload(inputs: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    fixture = inputs.get("fixture")
    source = copy.deepcopy(fixture if isinstance(fixture, dict) else {})
    triple = source.get("triple")
    metadata_value = source.get("metadata")
    metadata = dict(metadata_value) if isinstance(metadata_value, dict) else {}
    if isinstance(triple, dict):
        metadata["triples"] = [triple]
    for key in ("type", "document_id", "created_at"):
        if key in source and key not in metadata:
            metadata[key] = source[key]
    current["ingest_count"] = int(current.get("ingest_count", 0)) + 1
    external_id = source.get("external_id") or source.get("fact_id") or source.get("document_id")
    if external_id is None and isinstance(triple, dict):
        identity = json.dumps(triple, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        external_id = f"triple-{hashlib.sha256(identity.encode()).hexdigest()[:20]}"
    return {
        "external_id": str(external_id or f"record-{current['ingest_count']}"),
        "body": str(source.get("body") or source.get("content") or source.get("text") or ""),
        "knowledge_type": str(
            source.get("knowledge_type") or ("fact_triple" if metadata.get("triples") else "text")
        ),
        "metadata": metadata,
        "reference_time": _parse_time(
            source.get("source_event_time") or source.get("valid_at") or source.get("created_at")
        ),
        "source_revision": source.get("source_revision"),
        "source_updated_at": _parse_time(source.get("source_updated_at")),
        "source_version_id": source.get("source_version_id"),
        "trust_tier": int(source.get("trust_tier", inputs.get("trust_tier", 1))),
        "source_name": source.get("source_name"),
        "source_kind": str(source.get("source_kind") or "filesystem"),
        "title": source.get("title"),
    }


def _load_task_current(current: dict[str, Any], *, alias: str, source_id: str) -> dict[str, Any]:
    principal = copy.deepcopy(_principal(current, alias))
    return {
        "slug": current["slug"],
        "ingest_count": 0,
        "principals": {alias: principal},
        "sources": {f"{alias}:default:filesystem:1": source_id},
        "searches": {},
    }


def _curation_service(
    container: Container,
    uow: SqlAlchemyUnitOfWork,
    *,
    extractor: ClaimExtractor | None = None,
) -> CurationService:
    settings = container.settings
    embedding_model, embedding_dimension = active_embedding(settings)
    return CurationService(
        uow,
        extractor or container.extractor,
        object_store=container.object_store,
        judge=container.judge,
        embedder=(container.embedder if settings.memory.vector_search_enabled else None),
        embedding_provider=settings.memory.embedder,
        embedding_model=embedding_model,
        embedding_model_version=settings.memory.embedding_model_version,
        embedding_dimension=embedding_dimension,
    )


async def _wait_for_group_jobs(
    container: Container, group_id: str, *, timeout_s: float
) -> dict[str, int]:
    deadline = time.monotonic() + timeout_s
    latest: dict[str, int] = {}
    while True:
        async with container.sessionmaker() as session:
            rows = (
                await session.execute(
                    text(
                        "SELECT status, count(*) AS count FROM ingestion_jobs "
                        "WHERE group_id=:g GROUP BY status"
                    ),
                    {"g": group_id},
                )
            ).mappings()
            latest = {str(row["status"]): int(row["count"]) for row in rows}
        if latest.get("dead", 0):
            raise AdapterFailed(f"worker queue contains dead jobs; state={latest}")
        if latest.get("pending", 0) == 0 and latest.get("inflight", 0) == 0:
            return latest
        if time.monotonic() >= deadline:
            raise AdapterBlocked(
                f"worker queue did not settle within {timeout_s:g}s; state={latest}"
            )
        await asyncio.sleep(0.2)


async def _group_queue_state(container: Container, group_id: str) -> dict[str, int]:
    async with container.sessionmaker() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT status, count(*) AS count FROM ingestion_jobs "
                    "WHERE group_id=:group_id GROUP BY status"
                ),
                {"group_id": group_id},
            )
        ).mappings()
    return {str(row["status"]): int(row["count"]) for row in rows}


async def _wait_for_graph_failure(
    container: Container, group_id: str, *, timeout_s: float = _DEFAULT_TIMEOUT_S * 2
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while True:
        async with container.sessionmaker() as session:
            rows = list(
                (
                    await session.execute(
                        text(
                            "SELECT status, attempts, last_error FROM ingestion_jobs "
                            "WHERE group_id=:group_id AND payload->>'job_kind'='project_facts'"
                        ),
                        {"group_id": group_id},
                    )
                ).mappings()
            )
        failed = [row for row in rows if row["last_error"] or row["status"] == "dead"]
        if failed:
            return {
                "failure_observable": True,
                "retry_count": sum(max(0, int(row["attempts"]) - 1) for row in rows),
                "attempt_count": sum(int(row["attempts"]) for row in rows),
                "dead_job_count": sum(row["status"] == "dead" for row in rows),
            }
        if time.monotonic() >= deadline:
            raise AdapterFailed("graph outage produced no observable projection failure")
        await asyncio.sleep(0.2)


def _visible_artifact(result: dict[str, Any], artifact_version_id: str) -> bool:
    for fact in result.get("facts", []):
        if not isinstance(fact, dict):
            continue
        citation = fact.get("citation")
        if (
            isinstance(citation, dict)
            and citation.get("artifact_version_id") == artifact_version_id
        ):
            return True
    return False


async def _wait_for_search_visibility(
    *,
    api_key: str,
    query: str,
    project: str,
    artifact_version_id: str,
) -> dict[str, Any]:
    timeout_s = _timeout("VERA_EVAL_VISIBILITY_TIMEOUT_S", 30.0)
    deadline = time.monotonic() + timeout_s
    latest_status = 503
    latest: dict[str, Any] = {}
    while True:
        latest_status, latest = await _search_http(
            api_key=api_key,
            query=query,
            limit=10,
            project=project,
        )
        if 200 <= latest_status < 300 and _visible_artifact(latest, artifact_version_id):
            latest["visibility_poll_s"] = round(timeout_s - max(deadline - time.monotonic(), 0), 3)
            return latest
        if 400 <= latest_status < 500:
            raise AdapterFailed(f"post-ingest product search returned HTTP {latest_status}")
        if time.monotonic() >= deadline:
            if latest_status in {503, 504}:
                raise AdapterBlocked(
                    f"post-ingest product search stayed unavailable for {timeout_s:g}s"
                )
            raise AdapterFailed(
                "the ingested artifact did not become visible through product search "
                f"within {timeout_s:g}s"
            )
        await asyncio.sleep(0.2)


async def _ingest(
    container: Container,
    request: dict[str, Any],
    current: dict[str, Any],
    record: dict[str, Any],
    *,
    tombstone: bool = False,
    principal_alias: str = "default",
    require_search_visibility: bool = True,
    allow_projection_lag: bool = False,
) -> tuple[IngestResult, str, dict[str, int]]:
    source_id = await _ensure_source(
        container,
        request,
        current,
        trust_tier=int(record.get("trust_tier", 1)),
        name=record.get("source_name"),
        kind=str(record.get("source_kind") or "filesystem"),
        principal_alias=principal_alias,
    )
    scope = _principal(current, principal_alias)
    group_id = str(scope["group_id"])
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group_id)
        extractor = _DisabledExtractor() if current.get("extractor_state") == "disabled" else None
        result = await _curation_service(container, uow, extractor=extractor).ingest_artifact(
            IngestArtifact(
                source_id=UUID(source_id),
                group_id=group_id,
                external_id=str(record["external_id"]),
                body=str(record["body"]),
                knowledge_type=str(record["knowledge_type"]),
                title=record.get("title"),
                metadata=record.get("metadata"),
                reference_time=record.get("reference_time"),
                source_revision=record.get("source_revision"),
                source_updated_at=record.get("source_updated_at"),
                source_version_id=record.get("source_version_id"),
                tombstone=tombstone,
            )
        )
        if not isinstance(result, Ok):
            raise AdapterFailed(f"curation rejected the artifact: {result.error}")
        await uow.commit()
    value = result.value
    if allow_projection_lag:
        queue = await _group_queue_state(container, group_id)
    else:
        queue = await _wait_for_group_jobs(
            container,
            group_id,
            timeout_s=_timeout("VERA_EVAL_INGEST_TIMEOUT_S"),
        )
    triples = record.get("metadata", {}).get("triples", [])
    probe = ""
    if triples and isinstance(triples[0], dict):
        probe = " ".join(str(triples[0].get(key, "")) for key in ("subject", "predicate", "object"))
    elif str(record["body"]).strip():
        probe = str(record["body"])[:500]
    visibility: dict[str, Any] | None = None
    if probe and require_search_visibility and not allow_projection_lag:
        visibility = await _wait_for_search_visibility(
            api_key=str(scope["api_key"]),
            query=probe,
            project=str(scope["group_id"]),
            artifact_version_id=value.artifact_version_id,
        )
    current.update(
        {
            "last_external_id": record["external_id"],
            "last_artifact_version_id": value.artifact_version_id,
            "last_record": {
                "external_id": record["external_id"],
                "body": record["body"],
                "metadata": record["metadata"],
            },
            "last_ingest_search": visibility,
        }
    )
    current.setdefault("artifact_versions", []).append(value.artifact_version_id)
    if visibility is not None:
        current.setdefault("visibility_ms", []).append(
            round(float(visibility.get("visibility_poll_s", 0.0)) * 1000, 3)
        )
    current.setdefault("external_sources", {})[record["external_id"]] = source_id
    return value, source_id, queue


def _row_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return _format_time(value)
    if isinstance(value, UUID):
        return str(value)
    return value


def _rows(values: list[Any]) -> list[dict[str, Any]]:
    return [{str(key): _row_value(value) for key, value in row.items()} for row in values]


async def _database_snapshot(container: Container, group_id: str) -> dict[str, Any]:
    async with container.sessionmaker() as session, session.begin():
        await session.execute(
            text("SELECT set_config('vera.group_id', :group_id, true)"), {"group_id": group_id}
        )
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM artifacts a JOIN knowledge_sources s "
                        "ON s.id=a.source_id "
                        " LEFT JOIN projects p ON p.id=s.project_id WHERE p.group_id=:g) "
                        "AS artifacts, "
                        "(SELECT count(*) FROM artifact_versions av JOIN artifacts a "
                        "ON a.id=av.artifact_id JOIN knowledge_sources s ON s.id=a.source_id "
                        "LEFT JOIN projects p ON p.id=s.project_id "
                        " WHERE p.group_id=:g) AS versions, "
                        "(SELECT count(*) FROM candidate_claims WHERE group_id=:g) AS claims, "
                        "(SELECT count(*) FROM published_episodes WHERE group_id=:g) AS episodes, "
                        "(SELECT count(*) FROM facts WHERE group_id=:g) AS facts"
                    ),
                    {"g": group_id},
                )
            )
            .mappings()
            .one()
        )
        versions = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT av.id, av.artifact_id, av.version, av.content_hash, "
                            "av.reference_time, av.observed_at, av.predecessor_version_id, "
                            "a.external_id, a.source_id "
                            "FROM artifact_versions av JOIN artifacts a ON a.id=av.artifact_id "
                            "JOIN knowledge_sources s ON s.id=a.source_id "
                            "LEFT JOIN projects p ON p.id=s.project_id WHERE p.group_id=:g "
                            "ORDER BY av.created_at, av.id"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
        claims = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT id, artifact_version_id, statement, subject, predicate, "
                            "object, "
                            "verification_status, confidence, extraction_run_id, needs_review "
                            "FROM candidate_claims WHERE group_id=:g ORDER BY created_at, id"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
        episodes = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT id, source_id, artifact_version_id, reference_time, payload, "
                            "retracted_at FROM published_episodes WHERE group_id=:g "
                            "ORDER BY created_at, id"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
        facts = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT f.id, f.fact_key, f.subject_entity_id, "
                            "cs.canonical_name AS subject, "
                            "f.predicate, COALESCE(co.canonical_name, f.object_scalar) AS object, "
                            "f.lifecycle_state, f.valid_from, f.valid_to, f.authority, "
                            "f.confidence "
                            "FROM facts f JOIN canonical_entities cs ON cs.id=f.subject_entity_id "
                            "LEFT JOIN canonical_entities co ON co.id=f.object_entity_id "
                            "WHERE f.group_id=:g ORDER BY f.created_at, f.id"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
        jobs = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT id, source_id, status, attempts, last_error, trace_context, "
                            "payload->>'job_kind' AS job_kind, "
                            "payload->'_fabric'->>'artifact_version_id' AS artifact_version_id "
                            "FROM ingestion_jobs WHERE group_id=:g ORDER BY created_at, id"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
        assertions = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT id, fact_id, knowledge_source_id, artifact_version_id, "
                            "state, verification_state, recorded_at, valid_from, valid_to "
                            "FROM assertions WHERE group_id=:g ORDER BY created_at, id"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
        graph_edges = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT edge_uuid, published_episode_id FROM graph_edge_map "
                            "WHERE group_id=:g ORDER BY created_at, edge_uuid"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
        reviews = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT r.id, r.candidate_claim_id, r.decision, r.authority, r.notes, "
                            "r.created_at FROM reviews r JOIN candidate_claims c "
                            "ON c.id=r.candidate_claim_id WHERE c.group_id=:g "
                            "ORDER BY r.created_at, r.id"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
        audits = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT id, occurred_at, actor, group_id, action, target, payload "
                            "FROM audit_events WHERE group_id=:g ORDER BY occurred_at, id"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
        sources = _rows(
            list(
                (
                    await session.execute(
                        text(
                            "SELECT s.id, s.name, s.kind, s.trust_tier FROM knowledge_sources s "
                            "LEFT JOIN projects p ON p.id=s.project_id WHERE p.group_id=:g "
                            "ORDER BY s.id"
                        ),
                        {"g": group_id},
                    )
                ).mappings()
            )
        )
    queue: dict[str, int] = {}
    for job in jobs:
        status = str(job["status"])
        queue[status] = queue.get(status, 0) + 1
    return {
        **{str(key): int(value) for key, value in counts.items()},
        "versions_state": versions,
        "claims_state": claims,
        "episodes_state": episodes,
        "facts_state": facts,
        "assertions_state": assertions,
        "graph_edges_state": graph_edges,
        "jobs_state": jobs,
        "reviews_state": reviews,
        "audits_state": audits,
        "sources_state": sources,
        "queue": queue,
    }


def _trace_id(snapshot: dict[str, Any], version_id: str) -> str | None:
    for job in snapshot["jobs_state"]:
        if job.get("artifact_version_id") != version_id:
            continue
        context = job.get("trace_context")
        if not isinstance(context, dict):
            continue
        for key in ("trace_id", "traceparent"):
            value = context.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def _decision_observation(snapshot: dict[str, Any], version_id: str) -> dict[str, Any]:
    claim_ids = {
        str(claim["id"])
        for claim in snapshot["claims_state"]
        if claim["artifact_version_id"] == version_id
    }
    claims = [claim for claim in snapshot["claims_state"] if str(claim["id"]) in claim_ids]
    slots = {
        (str(claim["subject"]).casefold(), str(claim["predicate"]).casefold()) for claim in claims
    }
    related_claim_ids = {
        str(claim["id"])
        for claim in snapshot["claims_state"]
        if (str(claim["subject"]).casefold(), str(claim["predicate"]).casefold()) in slots
    }
    reviews = [
        review
        for review in snapshot["reviews_state"]
        if str(review["candidate_claim_id"]) in related_claim_ids
    ]
    active = [fact for fact in snapshot["facts_state"] if fact["lifecycle_state"] == "active"]
    duplicate_slots = len({(fact["subject"], fact["predicate"]) for fact in active}) < len(active)
    return {
        "reviewable": bool(reviews or any(claim["needs_review"] for claim in claims)),
        "silent_overwrite": bool(not reviews and duplicate_slots),
        "uncertainty_visible": bool(
            reviews
            or any(claim["verification_status"] in {"pending", "disputed"} for claim in claims)
        ),
        "review_ids": [review["id"] for review in reviews],
    }


def _ingest_observations(
    request: dict[str, Any],
    result: IngestResult,
    source_id: str,
    group_id: str,
    queue: dict[str, int],
    snapshot: dict[str, Any],
    record: dict[str, Any],
) -> dict[str, Any]:
    version = next(
        item for item in snapshot["versions_state"] if str(item["id"]) == result.artifact_version_id
    )
    episodes = [
        item
        for item in snapshot["episodes_state"]
        if str(item.get("artifact_version_id")) == result.artifact_version_id
    ]
    jobs = [
        item
        for item in snapshot["jobs_state"]
        if item.get("artifact_version_id") == result.artifact_version_id
    ]
    value = {
        "id": result.artifact_version_id,
        "version_id": result.artifact_version_id,
        "semantic_hash": version["content_hash"],
        "action": result.action,
        "available": True,
        "version_count": snapshot["versions"],
        "reference_time": version["reference_time"],
        "observed_at": version["observed_at"],
    }
    full: dict[str, Any] = {}
    for path in request["observe"]:
        root = path.split(".", maxsplit=1)[0]
        if root in {
            "artifact",
            "version",
            "old",
            "new",
            "newer",
            "late",
            "first",
            "second",
            "authoritative",
            "weaker",
            "inventory",
            "tax",
        }:
            _set_path(full, path, value)
            if root == "artifact":
                full["artifact"]["version_count"] = snapshot["versions"]
        elif root == "claim":
            _set_path(full, path, list(result.claim_ids))
        elif root == "episode":
            _set_path(full, path, episodes[0]["id"] if episodes else None)
        elif root == "projection":
            _set_path(full, path, jobs[0]["id"] if jobs else None)
        elif root == "trace":
            _set_path(full, path, _trace_id(snapshot, result.artifact_version_id))
        elif root == "ingest":
            _set_path(full, path, {"action": result.action, "queue": queue})
        elif root == "decision":
            _set_path(full, path, _decision_observation(snapshot, result.artifact_version_id))
        elif root == "review":
            _set_path(full, path, snapshot["reviews_state"])
        elif root == "queue":
            _set_path(full, path, queue)
        elif root == "source":
            published_source_id = episodes[0].get("source_id") if episodes else None
            durable_source_id = f"{group_id}:{result.claim_ids[0]}" if result.claim_ids else None
            _set_path(full, path, published_source_id or durable_source_id or source_id)
    if any(path.startswith("old.invalid_at") for path in request["observe"]):
        invalid_at = next(
            (
                fact["valid_to"]
                for fact in reversed(snapshot["facts_state"])
                if fact["valid_to"] is not None
            ),
            None,
        )
        _set_path(full, "old.invalid_at", invalid_at)
    if any("canonical_id" in path for path in request["observe"]):
        triples = record.get("metadata", {}).get("triples", [])
        subject = str(triples[0].get("subject", "")) if triples else ""
        asserted_fact_ids = {
            str(assertion["fact_id"])
            for assertion in snapshot["assertions_state"]
            if assertion.get("artifact_version_id") == result.artifact_version_id
        }
        canonical = next(
            (
                fact["subject_entity_id"]
                for fact in reversed(snapshot["facts_state"])
                if str(fact["id"]) in asserted_fact_ids
            ),
            None,
        )
        if canonical is None:
            canonical = next(
                (
                    fact["subject_entity_id"]
                    for fact in reversed(snapshot["facts_state"])
                    if str(fact["subject"]).casefold() == subject.casefold()
                ),
                None,
            )
        for path in request["observe"]:
            if "canonical_id" in path:
                _set_path(full, path, canonical)
    return full


def _normalize_product_search(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[Any]]:
    raw = payload.get("results")
    if not isinstance(raw, list):
        raise AdapterFailed("knowledge search response omitted results")
    normalized: list[dict[str, Any]] = []
    semantic_results: list[Any] = []
    for value in raw:
        if not isinstance(value, dict):
            raise AdapterFailed("knowledge search returned a non-object result")
        item = copy.deepcopy(cast(dict[str, Any], value))
        ref = item.get("ref")
        result_text = item.get("text")
        if isinstance(ref, str):
            item["id"] = ref
        if isinstance(result_text, str):
            item["fact"] = result_text
        normalized.append(item)
        semantic_results.append({key: item[key] for key in ("id", "kind", "fact") if key in item})
    return normalized, semantic_results


def _search_equivalence_key(results: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    keys: list[tuple[Any, ...]] = []
    for result in results:
        citation = result.get("citation")
        citation = citation if isinstance(citation, dict) else {}
        keys.append(
            (
                result.get("id"),
                result.get("kind"),
                result.get("fact"),
                citation.get("artifact_version_id"),
                citation.get("assertion_id"),
                citation.get("evidence_id"),
                citation.get("structured_record"),
            )
        )
    return keys


def _actual_trace(response: httpx.Response, payload: dict[str, Any]) -> str | None:
    for key in ("trace_id", "request_id"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    for key in ("x-trace-id", "x-request-id", "traceparent"):
        value = response.headers.get(key)
        if value:
            return value
    return None


async def _search_http(
    *,
    api_key: str,
    query: str,
    limit: int,
    project: str | None,
    as_of: datetime | None = None,
    known_as_of: datetime | None = None,
) -> tuple[int, dict[str, Any]]:
    timeout_s = _timeout("VERA_EVAL_SEARCH_TIMEOUT_S", 10.0)
    body: dict[str, Any] = {"query": query, "limit": limit}
    if project is not None:
        body["project"] = project
    if as_of is not None:
        body["as_of"] = _format_time(as_of)
    if known_as_of is not None:
        body["known_as_of"] = _format_time(known_as_of)
    url, host = _resolved_http_url(f"{_required_url('VERA_EVAL_API_URL')}/v2/knowledge/search")
    headers = {"Authorization": f"Bearer {api_key}"}
    if host is not None:
        headers["Host"] = host
    started = time.perf_counter()
    async with _http_client(headers=headers, timeout_s=timeout_s) as client:
        try:
            response = await client.post(url, json=body)
        except httpx.TimeoutException:
            elapsed = round((time.perf_counter() - started) * 1000, 3)
            return 504, {
                "status": 504,
                "results": [],
                "facts": [],
                "answerable_result_count": 0,
                "latency_ms": elapsed,
                "behavior_bounded": elapsed <= timeout_s * 1000 + 100,
                "bounded_outcome": "timeout",
                "timeout_s": timeout_s,
            }
        except httpx.HTTPError:
            elapsed = round((time.perf_counter() - started) * 1000, 3)
            return 503, {
                "status": 503,
                "results": [],
                "facts": [],
                "answerable_result_count": 0,
                "latency_ms": elapsed,
                "behavior_bounded": elapsed <= timeout_s * 1000 + 100,
                "bounded_outcome": "transport_error",
                "timeout_s": timeout_s,
            }
    elapsed = round((time.perf_counter() - started) * 1000, 3)
    try:
        product = response.json()
    except json.JSONDecodeError as exc:
        raise AdapterFailed("knowledge HTTP search returned non-JSON content") from exc
    if not isinstance(product, dict):
        raise AdapterFailed("knowledge HTTP search returned a non-object response")
    if 200 <= response.status_code < 300:
        facts, flattened = _normalize_product_search(cast(dict[str, Any], product))
    else:
        facts, flattened = [], []
    observation: dict[str, Any] = {
        "status": response.status_code,
        "results": flattened,
        "facts": facts,
        "answerable_result_count": len(facts),
        "latency_ms": elapsed,
        "behavior_bounded": elapsed <= timeout_s * 1000 + 100,
        "bounded_outcome": "response",
        "timeout_s": timeout_s,
    }
    trace_id = _actual_trace(response, cast(dict[str, Any], product))
    if trace_id is not None:
        observation["trace_id"] = trace_id
    for key in ("conflicts", "freshness_warnings", "omitted"):
        if key in product:
            observation[key] = product[key]
    return response.status_code, observation


def _mcp_token(settings: Settings, principal_id: str) -> str:
    configured = os.environ.get("VERA_EVAL_MCP_JWT_SECRET")
    runtime = settings.mcp.jwt_secret
    if runtime is None:
        raise AdapterBlocked("MCP JWT signing is not configured for the evaluation stack")
    secret = runtime.get_secret_value()
    if configured is not None and configured != secret:
        raise AdapterBlocked("evaluation MCP JWT secret does not match the active runtime")
    now = datetime.now(UTC)
    return jwt.encode(
        {
            "sub": principal_id,
            "iss": settings.mcp.auth_issuer,
            "aud": settings.mcp.auth_audience,
            "scope": " ".join(settings.mcp.required_scopes),
            "iat": now,
            "exp": now + timedelta(minutes=5),
        },
        secret,
        algorithm=settings.mcp.jwt_algorithm,
    )


def _mcp_headers(settings: Settings, principal_id: str) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {_mcp_token(settings, principal_id)}"}
    host = os.environ.get("VERA_EVAL_MCP_HOST_HEADER")
    if host:
        headers["Host"] = host
    return headers


def _mcp_payload(result: Any) -> dict[str, Any]:
    if bool(getattr(result, "is_error", False)):
        raise AdapterFailed("MCP tool returned an error result")
    structured = getattr(result, "structured_content", None)
    if isinstance(structured, dict):
        return cast(dict[str, Any], structured)
    for block in cast(list[Any], getattr(result, "content", [])):
        value = getattr(block, "text", None)
        if not isinstance(value, str):
            continue
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return cast(dict[str, Any], parsed)
    raise AdapterFailed("MCP tool response omitted structured JSON content")


async def _call_mcp_tool(
    settings: Settings,
    *,
    principal_id: str,
    name: str,
    arguments: dict[str, Any],
) -> tuple[dict[str, Any], float]:
    timeout_s = _timeout("VERA_EVAL_MCP_TIMEOUT_S", 15.0)
    started = time.perf_counter()
    try:
        async with httpx2.AsyncClient(
            headers=_mcp_headers(settings, principal_id),
            timeout=httpx2.Timeout(timeout_s),
            follow_redirects=True,
        ) as http_client:
            transport = streamable_http_client(
                _required_url("VERA_EVAL_MCP_URL"), http_client=http_client
            )
            async with Client(transport, read_timeout_seconds=timeout_s) as client:
                result = await client.call_tool(name, arguments, read_timeout_seconds=timeout_s)
    except AdapterFailed:
        raise
    except Exception as exc:
        raise AdapterBlocked(f"MCP {name} transport or protocol boundary is unavailable") from exc
    return _mcp_payload(result), round((time.perf_counter() - started) * 1000, 3)


async def _search_mcp(
    settings: Settings,
    *,
    principal_id: str,
    query: str,
    limit: int,
    project: str | None,
    as_of: datetime | None = None,
    known_as_of: datetime | None = None,
) -> dict[str, Any]:
    arguments: dict[str, Any] = {"query": query, "limit": limit}
    if project is not None:
        arguments["project"] = project
    if as_of is not None:
        arguments["as_of"] = _format_time(as_of)
    if known_as_of is not None:
        arguments["known_as_of"] = _format_time(known_as_of)
    payload, latency_ms = await _call_mcp_tool(
        settings,
        principal_id=principal_id,
        name="knowledge_search",
        arguments=arguments,
    )
    facts, flattened = _normalize_product_search(payload)
    timeout_s = _timeout("VERA_EVAL_MCP_TIMEOUT_S", 15.0)
    result: dict[str, Any] = {
        "results": flattened,
        "facts": facts,
        "answerable_result_count": len(facts),
        "latency_ms": latency_ms,
        "behavior_bounded": latency_ms <= timeout_s * 1000 + 100,
        "bounded_outcome": "response",
        "timeout_s": timeout_s,
    }
    for key in ("trace_id", "request_id", "conflicts", "freshness_warnings", "omitted"):
        if key in payload:
            result[key] = payload[key]
    return result


def _openai_url(settings: Settings) -> str:
    return (settings.memory.openai_base_url or "https://api.openai.com/v1").rstrip("/")


def _model_response_payload(response: httpx.Response) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "").split(";", maxsplit=1)[0]
    if content_type != "text/event-stream":
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise AdapterFailed("the configured model returned non-JSON content") from exc
        if not isinstance(payload, dict):
            raise AdapterFailed("the configured model returned a non-object response")
        return cast(dict[str, Any], payload)

    model_id: str | None = None
    content: list[str] = []
    usage: dict[str, Any] | None = None
    cost: float | None = None
    event_count = 0
    for line in response.text.splitlines():
        if not line.startswith("data:"):
            continue
        data = line.removeprefix("data:").strip()
        if not data or data == "[DONE]":
            continue
        try:
            event = json.loads(data)
        except json.JSONDecodeError as exc:
            raise AdapterFailed("the configured model returned malformed SSE JSON") from exc
        if not isinstance(event, dict):
            raise AdapterFailed("the configured model returned a non-object SSE event")
        event_count += 1
        event_model = event.get("model")
        if isinstance(event_model, str):
            model_id = event_model
        event_usage = event.get("usage")
        if isinstance(event_usage, dict):
            usage = cast(dict[str, Any], event_usage)
        for source in (event_usage, event):
            if not isinstance(source, dict):
                continue
            value = source.get("cost_usd", source.get("cost"))
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                cost = float(value)
        choices = event.get("choices")
        if not isinstance(choices, list):
            continue
        for choice in choices:
            if not isinstance(choice, dict) or choice.get("index", 0) != 0:
                continue
            delta = choice.get("delta")
            if isinstance(delta, dict) and isinstance(delta.get("content"), str):
                content.append(str(delta["content"]))
            message = choice.get("message")
            if isinstance(message, dict) and isinstance(message.get("content"), str):
                content.append(str(message["content"]))
    if event_count == 0 or model_id is None:
        raise AdapterFailed("the configured model SSE response omitted events or the model ID")
    payload: dict[str, Any] = {
        "model": model_id,
        "choices": [{"message": {"content": "".join(content)}}],
    }
    if usage is not None:
        payload["usage"] = usage
    if cost is not None:
        payload["cost_usd"] = cost
    return payload


async def _call_model(
    settings: Settings,
    *,
    model: str,
    messages: list[dict[str, str]],
) -> _ModelResult:
    secret = settings.memory.openai_api_key
    if secret is None or not secret.get_secret_value():
        raise AdapterBlocked("the configured OpenAI-compatible model has no API key")
    timeout_s = _timeout("VERA_EVAL_MODEL_TIMEOUT_S", settings.resilience.per_call_timeout_s)
    started = time.perf_counter()
    async with _http_client(
        headers={
            "Authorization": f"Bearer {secret.get_secret_value()}",
            "Accept": "application/json",
        },
        timeout_s=timeout_s,
    ) as client:
        try:
            response = await client.post(
                f"{_openai_url(settings)}/chat/completions",
                json={
                    "model": model,
                    "messages": messages,
                    "response_format": {"type": "json_object"},
                    "stream": False,
                    "temperature": 0,
                },
            )
        except httpx.TimeoutException as exc:
            raise AdapterBlocked("the configured model reached its bounded timeout") from exc
        except httpx.HTTPError as exc:
            raise AdapterBlocked("the configured model transport is unavailable") from exc
    if not 200 <= response.status_code < 300:
        raise AdapterBlocked(f"the configured model returned HTTP {response.status_code}")
    raw = _model_response_payload(response)
    choices = raw.get("choices")
    actual_model = raw.get("model")
    if not isinstance(choices, list) or not choices or not isinstance(actual_model, str):
        raise AdapterFailed("the model response omitted choices or the actual model ID")
    first = choices[0]
    content = first.get("message", {}).get("content") if isinstance(first, dict) else None
    if not isinstance(content, str):
        raise AdapterFailed("the model response omitted assistant content")
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        raise AdapterFailed("the consuming agent model did not return its JSON contract") from exc
    if not isinstance(payload, dict):
        raise AdapterFailed("the consuming agent model returned a non-object contract")
    usage_value = raw.get("usage")
    usage: dict[str, int] = {}
    if isinstance(usage_value, dict):
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            value = usage_value.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                usage[key] = value
    cost: float | None = None
    for source in (usage_value, raw):
        if not isinstance(source, dict):
            continue
        value = source.get("cost_usd", source.get("cost"))
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            cost = float(value)
            break
    return _ModelResult(
        payload=cast(dict[str, Any], payload),
        model_id=actual_model,
        latency_ms=round((time.perf_counter() - started) * 1000, 3),
        usage=usage,
        cost_usd=cost,
    )


def _context_references(context: dict[str, Any]) -> tuple[set[str], dict[str, dict[str, Any]]]:
    references = {
        str(value) for value in context.get("result_references", []) if isinstance(value, str)
    }
    citations: dict[str, dict[str, Any]] = {}
    raw_results = context.get("results")
    if isinstance(raw_results, list):
        for raw in raw_results:
            if not isinstance(raw, dict):
                continue
            item = cast(dict[str, Any], raw)
            ref = item.get("ref")
            if isinstance(ref, str):
                references.add(ref)
                citation = item.get("citation")
                if isinstance(citation, dict):
                    citations[ref] = cast(dict[str, Any], copy.deepcopy(citation))
    return references, citations


async def _agent_answer(
    settings: Settings,
    *,
    principal: dict[str, Any],
    question: str,
    retrieval_limit: int = 10,
    token_budget: int = 4000,
) -> dict[str, Any]:
    started = time.perf_counter()
    usage_ref = f"eval-agent:{uuid4()}"
    base_arguments: dict[str, Any] = {
        "project": principal["group_id"],
        "limit": retrieval_limit,
        "token_budget": token_budget,
        "usage_ref": usage_ref,
    }
    instant = _ISO_INSTANT.search(question)
    if instant is not None:
        base_arguments["as_of"] = instant.group(0)

    contexts: list[dict[str, Any]] = []
    tool_calls: list[dict[str, Any]] = []
    references: set[str] = set()
    product_citations: dict[str, dict[str, Any]] = {}
    mcp_latency = 0.0

    async def retrieve(query: str) -> None:
        nonlocal mcp_latency
        arguments = {"query": query, **base_arguments}
        context, latency = await _call_mcp_tool(
            settings,
            principal_id=str(principal["principal_id"]),
            name="knowledge_get_context",
            arguments=arguments,
        )
        current_references, current_citations = _context_references(context)
        references.update(current_references)
        product_citations.update(current_citations)
        mcp_latency += latency
        contexts.append({"query": query, **context})
        tool_calls.append(
            {
                "tool": "knowledge_get_context",
                "arguments": arguments,
                "latency_ms": latency,
                "pack_id": context.get("pack_id"),
                "result_ids": sorted(current_references),
            }
        )

    await retrieve(question)
    plan = await _call_model(
        settings,
        model=settings.memory.llm_model,
        messages=[
            {
                "role": "system",
                "content": (
                    "Decompose the user's request into standalone memory search queries. "
                    "Return JSON with one key, queries, containing an array of at most three "
                    "strings. Include one query for each independently requested fact, preserve "
                    "concrete entity names and times, and do not answer the request."
                ),
            },
            {"role": "user", "content": question},
        ],
    )
    planned = plan.payload.get("queries")
    if not isinstance(planned, list) or not all(isinstance(value, str) for value in planned):
        raise AdapterFailed("the consuming agent model returned an invalid retrieval plan")
    seen_queries = {question.strip().casefold()}
    for value in cast(list[str], planned)[:3]:
        query = value.strip()
        if query and query.casefold() not in seen_queries:
            seen_queries.add(query.casefold())
            await retrieve(query)

    context = {
        "contexts": contexts,
        "result_references": sorted(references),
    }
    prompt = (
        "Answer the user only from the VERA context below. Return JSON with keys answer "
        "(string), used_result_ids (array of exact context result IDs), citations (array of "
        "objects with result_id), and abstained (boolean). If the context is insufficient, "
        "abstain. Never create a result ID. Reconcile claims by event time before describing "
        "current state: later evidence may qualify or supersede earlier evidence. Preserve "
        "material unresolved conflicts.\n\nVERA context:\n"
        + json.dumps(context, ensure_ascii=True, separators=(",", ":"), default=str)
    )
    model = await _call_model(
        settings,
        model=settings.memory.llm_model,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": question},
        ],
    )
    answer = model.payload.get("answer")
    used = model.payload.get("used_result_ids")
    declared_citations = model.payload.get("citations")
    abstained = model.payload.get("abstained")
    if (
        not isinstance(answer, str)
        or not isinstance(used, list)
        or not all(isinstance(value, str) for value in used)
        or not isinstance(declared_citations, list)
        or not isinstance(abstained, bool)
    ):
        raise AdapterFailed("the consuming agent model returned an invalid answer contract")
    used_ids = cast(list[str], used)
    if any(value not in references for value in used_ids):
        raise AdapterFailed("the consuming agent cited a result absent from the MCP context")
    citation_ids: list[str] = []
    for citation in declared_citations:
        if not isinstance(citation, dict) or not isinstance(citation.get("result_id"), str):
            raise AdapterFailed("the consuming agent returned an invalid citation")
        result_id = str(citation["result_id"])
        if result_id not in references:
            raise AdapterFailed("the consuming agent cited a result absent from the MCP context")
        citation_ids.append(result_id)
    actual_citations = [
        {"result_id": value, "citation": product_citations.get(value)} for value in citation_ids
    ]
    usage = {
        key: int(plan.usage.get(key, 0)) + int(model.usage.get(key, 0))
        for key in set(plan.usage) | set(model.usage)
    }
    result: dict[str, Any] = {
        "answer": answer,
        "tool_calls": tool_calls,
        "used_result_ids": used_ids,
        "citations": actual_citations,
        "model_id": model.model_id,
        "prompt_version": "external-agent-v2",
        "latency_ms": round((time.perf_counter() - started) * 1000, 3),
        "mcp_latency_ms": round(mcp_latency, 3),
        "model_latency_ms": round(plan.latency_ms + model.latency_ms, 3),
        "usage_ref": usage_ref,
        "abstained": abstained,
        "unsupported_claim_count": int(bool(answer.strip()) and not used_ids and not abstained),
    }
    if usage:
        result["token_usage"] = usage
    if plan.cost_usd is not None and model.cost_usd is not None:
        result["cost_usd"] = plan.cost_usd + model.cost_usd
    return result


def _question_text(value: Any) -> str:
    if isinstance(value, dict):
        prompt = value.get("prompt", value.get("text"))
        if isinstance(prompt, str):
            return prompt
    if isinstance(value, str):
        return value
    raise AdapterBlocked("agent.run requires a string question or an object with prompt/text")


def _question_positive_int(value: Any, key: str, default: int, maximum: int) -> int:
    if not isinstance(value, dict) or key not in value:
        return default
    selected = value[key]
    if (
        isinstance(selected, bool)
        or not isinstance(selected, int)
        or selected < 1
        or selected > maximum
    ):
        raise AdapterBlocked(f"agent.run {key} must be between 1 and {maximum}")
    return selected


async def _agent(
    container: Container, request: dict[str, Any], current: dict[str, Any]
) -> dict[str, Any]:
    settings = container.settings
    principal = _principal(current, str(request["inputs"].get("principal", "default")))
    questions = request["inputs"].get("questions_ref")
    if isinstance(questions, list):
        measure_mcp_tokens = request.get("case_id") == "PERF-003"
        repetitions = request["inputs"].get("repetitions", 1)
        if isinstance(repetitions, bool) or not isinstance(repetitions, int) or repetitions < 1:
            raise AdapterBlocked("agent.run repetitions must be a positive integer")
        work = [
            (question_index, repetition_index, question)
            for repetition_index in range(repetitions)
            for question_index, question in enumerate(questions)
        ]

        async def answer_one(
            question_index: int, repetition_index: int, question: Any
        ) -> dict[str, Any]:
            run = await _agent_answer(
                settings,
                principal=principal,
                question=_question_text(question),
            )
            run["question_index"] = question_index
            run["repetition_index"] = repetition_index
            if isinstance(question, dict) and isinstance(question.get("query_id"), str):
                run["query_id"] = question["query_id"]
            return run

        if repetitions == 1:
            runs = [
                await answer_one(question_index, repetition_index, question)
                for question_index, repetition_index, question in work
            ]
        else:
            runs_by_index: list[dict[str, Any] | None] = [None] * len(work)
            semaphore = asyncio.Semaphore(_bounded_concurrency("VERA_EVAL_AGENT_CONCURRENCY", 4))

            async def run_one(
                index: int, question_index: int, repetition_index: int, question: Any
            ) -> None:
                async with semaphore:
                    runs_by_index[index] = await answer_one(
                        question_index, repetition_index, question
                    )

            async with asyncio.TaskGroup() as task_group:
                for index, (question_index, repetition_index, question) in enumerate(work):
                    task_group.create_task(
                        run_one(index, question_index, repetition_index, question)
                    )
            runs = [cast(dict[str, Any], run) for run in runs_by_index]
        answers: dict[str, Any] = {}
        names = ("current", "historical", "budget")
        for index, run in enumerate(runs):
            key = names[index] if index < len(names) else f"question_{index + 1}"
            answers[key] = (
                {"answer": run["answer"], "abstained": run["abstained"]}
                if key == "budget"
                else run["answer"]
            )
        result = {
            "answers": answers,
            "tool_calls": [call for run in runs for call in run["tool_calls"]],
            "result_ids": [value for run in runs for value in run["used_result_ids"]],
            "citations": [value for run in runs for value in run["citations"]],
            "runs": runs,
        }
        if measure_mcp_tokens:
            usage_refs = [str(run["usage_ref"]) for run in runs]
            mcp_tokens = await _usage_tokens_by_ref(
                container,
                group_id=str(principal["group_id"]),
                request_kind="search",
                refs=usage_refs,
            )
            if set(mcp_tokens) != set(usage_refs) or any(
                mcp_tokens[usage_ref] <= 0 for usage_ref in usage_refs
            ):
                raise AdapterFailed("agent MCP usage was not attributable to every run")
            for run in runs:
                usage = cast(dict[str, Any], run.setdefault("token_usage", {}))
                model_tokens = int(usage.get("total_tokens", 0)) or int(
                    usage.get("prompt_tokens", 0)
                ) + int(usage.get("completion_tokens", 0))
                if model_tokens <= 0:
                    raise AdapterFailed(
                        "agent model response omitted provider-reported token usage"
                    )
                usage["mcp_tokens"] = mcp_tokens[str(run["usage_ref"])]
                usage["total_tokens"] = model_tokens + int(usage["mcp_tokens"])
            result["mcp_token_usage"] = {
                "total_tokens": sum(mcp_tokens.values()),
                "source": "llm_usage",
            }
        return result
    question_value = request["inputs"].get("question_ref", request["inputs"].get("question"))
    return {
        "agent": await _agent_answer(
            settings,
            principal=principal,
            question=_question_text(question_value),
            retrieval_limit=_question_positive_int(question_value, "retrieval_limit", 10, 50),
            token_budget=_question_positive_int(question_value, "token_budget", 4000, 16000),
        )
    }


async def _graph_counts(settings: Settings) -> dict[str, int]:
    if settings.memory.provider != "graphiti" or settings.memory.graph_backend != "neo4j":
        raise AdapterBlocked("the evaluation stack must use Graphiti with Neo4j")
    if settings.neo4j.uri is None or settings.neo4j.password is None:
        raise AdapterBlocked("Neo4j runtime settings are incomplete")
    driver = AsyncGraphDatabase.driver(
        settings.neo4j.uri,
        auth=(settings.neo4j.user, settings.neo4j.password.get_secret_value()),
    )
    try:
        result = await driver.execute_query(
            "MATCH (n) WITH count(n) AS nodes OPTIONAL MATCH ()-[r]->() "
            "RETURN nodes, count(r) AS edges"
        )
        record = result.records[0]
        return {"nodes": int(record["nodes"]), "edges": int(record["edges"])}
    except Exception as exc:
        raise AdapterBlocked("Neo4j cannot be independently inspected") from exc
    finally:
        await driver.close()


async def _clear_graph(settings: Settings) -> dict[str, int]:
    if settings.neo4j.uri is None or settings.neo4j.password is None:
        raise AdapterBlocked("Neo4j runtime settings are incomplete")
    driver = AsyncGraphDatabase.driver(
        settings.neo4j.uri,
        auth=(settings.neo4j.user, settings.neo4j.password.get_secret_value()),
    )
    try:
        await driver.execute_query("MATCH (n) DETACH DELETE n")
    except Exception as exc:
        raise AdapterBlocked("Neo4j cleanup failed") from exc
    finally:
        await driver.close()
    return await _graph_counts(settings)


def _s3_client(settings: Settings) -> Any:
    objectstore = settings.objectstore
    session = aioboto3.Session()
    return session.client(
        "s3",
        endpoint_url=objectstore.endpoint_url,
        aws_access_key_id=(
            objectstore.access_key.get_secret_value() if objectstore.access_key else None
        ),
        aws_secret_access_key=(
            objectstore.secret_key.get_secret_value() if objectstore.secret_key else None
        ),
        region_name=objectstore.region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


async def _object_count(settings: Settings) -> int:
    if not settings.objectstore.endpoint_url:
        raise AdapterBlocked("the evaluation object-store endpoint is not configured")
    try:
        async with _s3_client(settings) as client:
            buckets = await client.list_buckets()
            count = 0
            for bucket in buckets.get("Buckets", []):
                name = bucket["Name"]
                paginator = client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(Bucket=name):
                    count += len(page.get("Contents", []))
            return count
    except ClientError as exc:
        raise AdapterBlocked("MinIO cannot be independently inspected") from exc


async def _clear_objects(settings: Settings) -> int:
    try:
        async with _s3_client(settings) as client:
            buckets = await client.list_buckets()
            for bucket in buckets.get("Buckets", []):
                name = bucket["Name"]
                paginator = client.get_paginator("list_object_versions")
                async for page in paginator.paginate(Bucket=name):
                    objects = [
                        {"Key": value["Key"], "VersionId": value["VersionId"]}
                        for key in ("Versions", "DeleteMarkers")
                        for value in page.get(key, [])
                    ]
                    for offset in range(0, len(objects), 1000):
                        await client.delete_objects(
                            Bucket=name,
                            Delete={"Objects": objects[offset : offset + 1000], "Quiet": True},
                        )
    except ClientError as exc:
        raise AdapterBlocked("MinIO cleanup failed") from exc
    return await _object_count(settings)


async def _valkey_count(settings: Settings) -> int:
    if not settings.resilience.valkey_url:
        raise AdapterBlocked("the evaluation Valkey endpoint is not configured")
    client = Redis.from_url(settings.resilience.valkey_url)
    try:
        if not await client.ping():
            raise AdapterBlocked("Valkey readiness failed")
        return int(await client.dbsize())
    except AdapterBlocked:
        raise
    except Exception as exc:
        raise AdapterBlocked("Valkey cannot be independently inspected") from exc
    finally:
        await client.aclose()


async def _clear_valkey(settings: Settings) -> int:
    if not settings.resilience.valkey_url:
        raise AdapterBlocked("the evaluation Valkey endpoint is not configured")
    client = Redis.from_url(settings.resilience.valkey_url)
    try:
        await client.flushdb()
    except Exception as exc:
        raise AdapterBlocked("Valkey cleanup failed") from exc
    finally:
        await client.aclose()
    return await _valkey_count(settings)


async def _mutable_database_tables(container: Container) -> list[str]:
    async with container.sessionmaker() as session:
        values = await session.scalars(
            text(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='public' AND c.relkind IN ('r','p') AND NOT c.relispartition "
                "ORDER BY c.relname"
            )
        )
    return [str(value) for value in values if str(value) not in _SAFE_DATABASE_TABLES]


async def _database_counts(container: Container) -> dict[str, int]:
    counts: dict[str, int] = {}
    async with container.sessionmaker() as session:
        for table in await _mutable_database_tables(container):
            quoted = table.replace('"', '""')
            counts[table] = int(
                await session.scalar(text(f'SELECT count(*) FROM "{quoted}"')) or 0  # noqa: S608
            )
    return counts


async def _database_inventory(container: Container) -> list[str]:
    inventory: list[str] = []
    async with container.sessionmaker() as session:
        for table, prefix, column in (
            ("projects", "group", "group_id"),
            ("knowledge_sources", "source", "id"),
            ("artifact_versions", "artifact-version", "id"),
            ("retrieval_feedback", "feedback", "id"),
        ):
            values = await session.scalars(text(f"SELECT {column}::text FROM {table}"))  # noqa: S608
            inventory.extend(f"{prefix}:{value}" for value in values)
    return sorted(set(inventory))


async def _clear_database(container: Container) -> dict[str, int]:
    tables = await _mutable_database_tables(container)
    if not tables:
        raise AdapterBlocked("no mutable PostgreSQL tables were discovered")
    quoted = ", ".join(f'"{table.replace(chr(34), chr(34) * 2)}"' for table in tables)
    async with container.sessionmaker() as session, session.begin():
        await session.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY CASCADE"))
    return await _database_counts(container)


async def _api_readiness() -> dict[str, Any]:
    async with _http_client(timeout_s=10.0) as client:
        try:
            response = await client.get(f"{_required_url('VERA_EVAL_API_URL')}/health/ready")
        except httpx.HTTPError as exc:
            raise AdapterBlocked("VERA API readiness is unavailable") from exc
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise AdapterBlocked("VERA API readiness returned non-JSON content") from exc
    if (
        response.status_code != 200
        or not isinstance(payload, dict)
        or payload.get("status") != "ok"
    ):
        raise AdapterBlocked("VERA API readiness is degraded")
    return cast(dict[str, Any], payload)


async def _mcp_readiness(settings: Settings) -> int:
    timeout_s = _timeout("VERA_EVAL_MCP_TIMEOUT_S", 15.0)
    try:
        async with httpx2.AsyncClient(
            headers=_mcp_headers(settings, "00000000-0000-0000-0000-000000000001"),
            timeout=httpx2.Timeout(timeout_s),
            follow_redirects=True,
        ) as http_client:
            transport = streamable_http_client(
                _required_url("VERA_EVAL_MCP_URL"), http_client=http_client
            )
            async with Client(transport, read_timeout_seconds=timeout_s) as client:
                result = await client.list_tools()
    except Exception as exc:
        raise AdapterBlocked("VERA MCP readiness is unavailable") from exc
    names = {str(tool.name) for tool in result.tools}
    required = {"knowledge_search", "knowledge_get_context"}
    if not required <= names:
        raise AdapterBlocked("VERA MCP is missing required knowledge tools")
    return len(names)


def _runtime_manifest_errors(
    container: Container, request: dict[str, Any]
) -> tuple[list[str], dict[str, str | None]]:
    manifest = request["run_context"].get("manifest")
    quality = request["run_context"].get("quality_config")
    if not isinstance(manifest, dict) or not isinstance(quality, dict):
        return ["preflight requires manifest and quality_config"], {}
    settings = container.settings
    errors: list[str] = []
    expected_backend = f"{settings.memory.provider}/{settings.memory.graph_backend}"
    if manifest.get("graph_backend") != expected_backend:
        errors.append("manifest graph_backend does not match the active Graphiti backend")
    if quality.get("fabric_write_mode") != settings.memory.effective_fabric_write_mode:
        errors.append("manifest fabric_write_mode does not match the active runtime")
    embedding_model, embedding_dimension = active_embedding(settings)
    reranker_model: str | None = None
    if settings.rerank.cross_encoder_enabled:
        reranker_model = (
            settings.voyage.rerank_model
            if settings.rerank.cross_encoder_provider == "voyage"
            else settings.memory.small_llm_model
        )
    actual_models: dict[str, str | None] = {
        "candidate": settings.memory.llm_model,
        "extractor": str(container.extractor.model),
        "contradiction_judge": (
            settings.memory.small_llm_model if container.judge is not None else None
        ),
        "entity_judge": settings.memory.llm_model if container.entity_judge is not None else None,
        "embedder": settings.memory.embedder,
        "embedding": embedding_model,
        "embedding_dimension": str(embedding_dimension),
        "reranker": reranker_model if container.reranker is not None else None,
    }
    declared = manifest.get("models")
    if not isinstance(declared, dict):
        errors.append("manifest models are missing")
    else:
        for key, value in actual_models.items():
            if declared.get(key) != value:
                errors.append(f"manifest model {key!r} does not match the active runtime")
    if settings.memory.provider != "graphiti" or settings.memory.graph_backend != "neo4j":
        errors.append("evaluation runtime must use Graphiti with Neo4j")
    if settings.memory.effective_fabric_write_mode not in {"dual", "fabric"}:
        errors.append("evaluation runtime must write the authoritative Fabric")
    return errors, actual_models


async def _provider_preflight(container: Container) -> dict[str, str]:
    settings = container.settings
    if container.extractor.provider != "openai-compatible":
        raise AdapterBlocked("evaluation extraction is not using a configured model API")
    if container.judge is None:
        raise AdapterBlocked("evaluation contradiction judging has no configured model API")
    if container.entity_judge is None:
        raise AdapterBlocked("evaluation entity resolution has no configured model API")
    if settings.memory.embedder not in {"openai", "voyage"}:
        raise AdapterBlocked("evaluation embedding is not using a configured model API")
    if container.embedder is None:
        raise AdapterBlocked("the configured provider embedder is unavailable")
    if container.reranker is None:
        raise AdapterBlocked("the configured provider reranker is unavailable")
    models = {
        settings.memory.llm_model,
        str(container.extractor.model),
        settings.memory.small_llm_model,
    }
    resolved: dict[str, str] = {}
    for model in sorted(models):
        result = await _call_model(
            settings,
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Provider readiness check. Return exactly one JSON object and nothing "
                        "else, with key ok set to true."
                    ),
                }
            ],
        )
        if result.payload.get("ok") is not True:
            raise AdapterBlocked(f"provider preflight failed for configured model {model!r}")
        resolved[model] = result.model_id
    vector = await container.embedder.embed("VERA evaluation provider preflight")
    if len(vector) != active_embedding(settings)[1]:
        raise AdapterBlocked("provider embedder returned the wrong dimension")
    scores = await container.reranker.rerank(
        query="Where does Atlas API run?",
        facts=("Atlas API runs on cluster blue.", "Finance owns Billing API."),
    )
    if (
        len(scores) != 2
        or not all(math.isfinite(value) for value in scores)
        or scores == [0.5, 0.5]
        or scores[0] == scores[1]
    ):
        raise AdapterBlocked("provider reranker returned fallback or non-discriminating scores")
    return resolved


def _settings_fingerprint(settings: Settings) -> str:
    model, dimension = active_embedding(settings)
    value = {
        "memory_provider": settings.memory.provider,
        "graph_backend": settings.memory.graph_backend,
        "fabric_write_mode": settings.memory.effective_fabric_write_mode,
        "llm_model": settings.memory.llm_model,
        "extractor_model": settings.memory.small_llm_model,
        "embedder": settings.memory.embedder,
        "embedding_model": model,
        "embedding_dimension": dimension,
    }
    encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


# Hosts a disposable evaluation stack is allowed to run against. The scope id is only a logical
# label, so it cannot by itself prove the configured Postgres/Neo4j/S3/Valkey endpoints are not
# production; this allowlist guards the destructive cleanup against a matching-scope run pointed
# at a real (even if momentarily empty) stack. Override with VERA_EVAL_ALLOWED_HOSTS.
_DEFAULT_DISPOSABLE_HOSTS = frozenset(
    {"localhost", "127.0.0.1", "::1", "postgres", "neo4j", "minio", "valkey", "falkordb"}
)


def _endpoint_hosts(settings: Settings) -> list[tuple[str, str | None]]:
    hosts: list[tuple[str, str | None]] = [("postgres", urlsplit(str(settings.db.dsn)).hostname)]
    if settings.neo4j.uri:
        hosts.append(("neo4j", urlsplit(settings.neo4j.uri).hostname))
    if settings.objectstore.endpoint_url:
        hosts.append(("objectstore", urlsplit(settings.objectstore.endpoint_url).hostname))
    valkey_url = getattr(settings.resilience, "valkey_url", None)
    if valkey_url:
        hosts.append(("valkey", urlsplit(valkey_url).hostname))
    return hosts


def _assert_disposable_endpoints(settings: Settings) -> None:
    configured = os.environ.get("VERA_EVAL_ALLOWED_HOSTS", "")
    allowed = {h.strip().lower() for h in configured.split(",") if h.strip()} or set(
        _DEFAULT_DISPOSABLE_HOSTS
    )
    for name, host in _endpoint_hosts(settings):
        if host is not None and host.lower() not in allowed:
            raise AdapterBlocked(
                f"refusing destructive evaluation: {name} endpoint host {host!r} is not in the "
                "disposable-host allowlist; set VERA_EVAL_ALLOWED_HOSTS to permit it"
            )


async def _preflight(
    container: Container, request: dict[str, Any], state: dict[str, Any]
) -> dict[str, Any]:
    scope = request["inputs"].get("evaluation_scope")
    configured_scope = os.environ.get("VERA_EVAL_SCOPE_ID")
    safe_scope = (
        isinstance(scope, dict)
        and scope.get("kind") == "ephemeral_stack"
        and scope.get("run_owned") is True
        and scope.get("production_writable") is False
        and isinstance(scope.get("id"), str)
        and scope.get("id") == configured_scope
    )
    if not safe_scope:
        return {
            "schema_version": "1.0",
            "request_nonce": request["request_nonce"],
            "status": "FAIL",
            "observations": {
                "safety": {
                    "scope_run_owned": False,
                    "production_writable": False,
                    "cost_bounded": False,
                    "cleanup_supported": False,
                }
            },
            "message": "evaluation scope does not exactly match the configured ephemeral stack",
        }
    _assert_disposable_endpoints(container.settings)
    if state.get("preflight") is not None or state.get("cases"):
        raise AdapterBlocked("evaluation state is not pristine before preflight")
    manifest_errors, actual_models = _runtime_manifest_errors(container, request)
    if manifest_errors:
        raise AdapterBlocked("; ".join(manifest_errors))
    database = await _database_counts(container)
    graph = await _graph_counts(container.settings)
    objects = await _object_count(container.settings)
    valkey = await _valkey_count(container.settings)
    dirty_tables = sorted(table for table, count in database.items() if count)
    if dirty_tables or graph["nodes"] or graph["edges"] or objects or valkey:
        raise AdapterBlocked(
            "evaluation stores are not pristine: "
            f"postgres_tables={dirty_tables}, graph={graph}, minio_objects={objects}, "
            f"valkey_keys={valkey}"
        )
    api = await _api_readiness()
    tool_count = await _mcp_readiness(container.settings)
    provider_models = await _provider_preflight(container)
    state["preflight"] = {
        "run_id": request["run_id"],
        "scope_id": configured_scope,
        "settings_fingerprint": _settings_fingerprint(container.settings),
        "disposable_full_store": True,
    }
    _save_state(str(request["run_id"]), state)
    return {
        "schema_version": "1.0",
        "request_nonce": request["request_nonce"],
        "status": "PASS",
        "observations": {
            "safety": {
                "scope_run_owned": True,
                "production_writable": False,
                "cost_bounded": True,
                "cleanup_supported": True,
                "api_ready": api.get("status") == "ok",
                "mcp_tool_count": tool_count,
                "database_pristine": True,
                "graph_pristine": True,
                "object_store_pristine": True,
                "valkey_pristine": True,
                "active_models": actual_models,
                "provider_models": provider_models,
            }
        },
        "message": "dedicated disposable stack and active providers verified",
    }


async def _cleanup(
    container: Container, request: dict[str, Any], state: dict[str, Any]
) -> _Outcome:
    scope = request["run_context"].get("evaluation_scope")
    attestation = state.get("preflight")
    configured_scope = os.environ.get("VERA_EVAL_SCOPE_ID")
    safe = (
        isinstance(scope, dict)
        and scope.get("kind") == "ephemeral_stack"
        and scope.get("run_owned") is True
        and scope.get("production_writable") is False
        and scope.get("id") == configured_scope
        and isinstance(attestation, dict)
        and attestation.get("run_id") == request["run_id"]
        and attestation.get("scope_id") == configured_scope
        and attestation.get("disposable_full_store") is True
        and attestation.get("settings_fingerprint") == _settings_fingerprint(container.settings)
    )
    if not safe:
        raise AdapterBlocked("full-store cleanup lacks a matching disposable-stack preflight")
    _assert_disposable_endpoints(container.settings)
    if os.environ.get("VERA_EVAL_DEPENDENCY_CONTROL_URL"):
        await _configure_graph_dependency("available")
    inventory = await _database_inventory(container)
    database = await _clear_database(container)
    graph = await _clear_graph(container.settings)
    objects = await _clear_objects(container.settings)
    valkey = await _clear_valkey(container.settings)
    api = await _api_readiness()
    tools = await _mcp_readiness(container.settings)
    remaining_tables = sorted(table for table, count in database.items() if count)
    verified = (
        not remaining_tables
        and graph == {"nodes": 0, "edges": 0}
        and objects == 0
        and valkey == 0
        and api.get("status") == "ok"
        and tools > 0
    )
    cleanup = {
        "inventory_complete": True,
        "created_resource_ids": inventory,
        "removed_resource_ids": inventory if verified else [],
        "remaining_resource_ids": [] if verified else inventory,
        "health_restored": verified,
        "stores": {
            "postgres_nonempty_tables": remaining_tables,
            "graph": graph,
            "minio_objects": objects,
            "valkey_keys": valkey,
            "api_ready": api.get("status") == "ok",
            "mcp_tool_count": tools,
        },
    }
    return _Outcome(
        status="PASS" if verified else "FAIL",
        observations={"cleanup": cleanup},
        removed=inventory if verified else [],
        message=(
            "all disposable stores independently verified empty"
            if verified
            else "store cleanup verification failed"
        ),
        boundaries=("database", "graph", "api", "mcp"),
    )


async def _projection_observation(
    container: Container, request: dict[str, Any], current: dict[str, Any]
) -> _Outcome:
    timeout_s = float(request["inputs"].get("timeout_s", _DEFAULT_TIMEOUT_S))
    started = time.perf_counter()
    queue = await _wait_for_group_jobs(container, str(current["group_id"]), timeout_s=timeout_s)
    snapshot = await _database_snapshot(container, str(current["group_id"]))
    full: dict[str, Any] = {}
    projection = {
        "pending_jobs": queue.get("pending", 0),
        "inflight_jobs": queue.get("inflight", 0),
        "dead_jobs": queue.get("dead", 0),
        "queue_state": queue,
        "searchable_at": _format_time(datetime.now(UTC)),
        "latency_ms": round((time.perf_counter() - started) * 1000, 3),
    }
    if str(request["case_id"]) == "RES-001":
        fact_projection = container.fact_projection
        if fact_projection is None:
            return _Outcome(status="BLOCKED", message="active runtime has no fact projection")
        expected = sorted(
            str(fact["fact_key"])
            for fact in snapshot["facts_state"]
            if fact["lifecycle_state"] == "active"
        )
        graph = sorted(await fact_projection.projected_fact_keys(group_id=str(current["group_id"])))
        jobs = snapshot["jobs_state"]
        retry_count = sum(max(0, int(job["attempts"]) - 1) for job in jobs)
        failure = cast(dict[str, Any], current.get("graph_failure", {}))
        projection.update(
            {
                "failure_observable": bool(failure.get("failure_observable")) or retry_count > 0,
                "retry_count": retry_count,
                "expected": expected,
                "graph": graph,
            }
        )
    for path in request["observe"]:
        if path == "counts.before":
            _set_path(full, path, snapshot)
        elif path.startswith("lineage"):
            continue
        else:
            _set_path(full, path, projection)
    if any(path.startswith("lineage") for path in request["observe"]):
        version_id = current.get("last_artifact_version_id")
        version = next(
            (item for item in snapshot["versions_state"] if str(item["id"]) == str(version_id)),
            None,
        )
        claims = [
            item
            for item in snapshot["claims_state"]
            if str(item["artifact_version_id"]) == str(version_id)
        ]
        active_facts = [
            fact for fact in snapshot["facts_state"] if fact["lifecycle_state"] == "active"
        ]
        assertions = [
            item
            for item in snapshot["assertions_state"]
            if item["artifact_version_id"] == str(version_id) and item["state"] == "active"
        ]
        job = next(
            (
                item
                for item in snapshot["jobs_state"]
                if item.get("artifact_version_id") == str(version_id)
            ),
            None,
        )
        assertion_fact_ids = {str(item["fact_id"]) for item in assertions}
        joined_facts = [fact for fact in active_facts if str(fact["id"]) in assertion_fact_ids]
        edge_uuid = joined_facts[0]["fact_key"] if len(joined_facts) == 1 else None
        lineage = {
            "complete": bool(version and claims and job and assertions and joined_facts),
            "source_event_time": version.get("reference_time") if version else None,
            "edge_uuid": edge_uuid,
            "artifact_version_id": version_id,
            "claim_ids": [item["id"] for item in claims],
            "job_id": job.get("id") if job else None,
            "assertion_ids": [item["id"] for item in assertions],
            "fact_keys": [item["fact_key"] for item in joined_facts],
        }
        full["lineage"] = lineage
    return _Outcome(observations=full, boundaries=("database", "graph"))


async def _seed_load_fixture(
    container: Container,
    request: dict[str, Any],
    current: dict[str, Any],
    fixture: dict[str, Any],
) -> _Outcome:
    scope_count, facts_per_scope, query_count, seed, corpus_sha256 = _generator_parameters(
        fixture, seed_override=request["inputs"].get("seed")
    )
    digest = hashlib.sha256()
    for item in records(
        scopes=scope_count,
        facts_per_scope=facts_per_scope,
        queries=query_count,
        seed=seed,
    ):
        digest.update(canonical_line(item))
    actual_sha256 = digest.hexdigest()
    if actual_sha256 != corpus_sha256:
        return _Outcome(
            status="FAIL",
            observations={"dataset": {"sha256": actual_sha256}, "resources": []},
            message="generated load corpus fingerprint differs from fixture metadata",
        )

    aliases = [f"scope-{index:02d}" for index in range(scope_count)]
    sources: dict[str, str] = {}
    group_ids: list[str] = []
    for alias in aliases:
        principal = await _ensure_scope(request, current, alias)
        sources[alias] = await _ensure_source(container, request, current, principal_alias=alias)
        group_ids.append(str(principal["group_id"]))

    pool_capacity = container.settings.db.pool_size + container.settings.db.max_overflow
    semaphore = asyncio.Semaphore(
        _bounded_concurrency(
            "VERA_EVAL_LOAD_INGEST_CONCURRENCY",
            min(8, pool_capacity),
            maximum=pool_capacity,
        )
    )

    async def ingest_scope(scope_index: int, alias: str) -> tuple[int, int]:
        accepted = 0
        failed = 0
        task_current = _load_task_current(current, alias=alias, source_id=sources[alias])
        async with semaphore:
            for fact_index in range(facts_per_scope):
                item = load_fact(scope_index, fact_index, seed, facts_per_scope)
                record = _record_payload({"fixture": item}, task_current)
                try:
                    await _ingest(
                        container,
                        request,
                        task_current,
                        record,
                        principal_alias=alias,
                        allow_projection_lag=True,
                        require_search_visibility=False,
                    )
                except (AdapterBlocked, AdapterFailed):
                    failed += 1
                else:
                    accepted += 1
        return accepted, failed

    tasks: list[asyncio.Task[tuple[int, int]]] = []
    async with asyncio.TaskGroup() as task_group:
        for scope_index, alias in enumerate(aliases):
            tasks.append(task_group.create_task(ingest_scope(scope_index, alias)))
    counts = [task.result() for task in tasks]

    queue_states: list[dict[str, int] | None] = [None] * scope_count

    async def settle(index: int, group_id: str) -> None:
        queue_states[index] = await _wait_for_group_jobs(
            container,
            group_id,
            timeout_s=_timeout("VERA_EVAL_LOAD_SETTLE_TIMEOUT_S", _DEFAULT_TIMEOUT_S * 20),
        )

    async with asyncio.TaskGroup() as task_group:
        for index, group_id in enumerate(group_ids):
            task_group.create_task(settle(index, group_id))

    accepted = sum(value[0] for value in counts)
    failed = sum(value[1] for value in counts)
    current["group_id"] = group_ids[0]
    current["load_fixture"] = {
        "scope_count": scope_count,
        "facts_per_scope": facts_per_scope,
        "query_count": query_count,
        "seed": seed,
        "aliases": aliases,
        "dataset_sha256": actual_sha256,
    }
    created = [
        resource
        for alias, group_id in zip(aliases, group_ids, strict=True)
        for resource in (f"group:{group_id}", f"source:{sources[alias]}")
    ]
    return _Outcome(
        status="PASS" if failed == 0 else "FAIL",
        observations={
            "dataset": {"sha256": actual_sha256},
            "resources": group_ids,
            "ingestion": {
                "accepted_document_count": accepted,
                "failed_document_count": failed,
            },
            "queue": {alias: queue_states[index] for index, alias in enumerate(aliases)},
        },
        created=created,
        message=("" if failed == 0 else f"{failed} load fixture records failed at a boundary"),
        boundaries=("database", "graph"),
    )


async def _seed(container: Container, request: dict[str, Any], current: dict[str, Any]) -> _Outcome:
    fixture_input = request["inputs"].get("fixture")
    fixture_file = request["inputs"].get("fixture_file")
    fixture = fixture_file.get("data") if isinstance(fixture_file, dict) else fixture_input
    accepted = 0
    failed = 0
    pending_claims: list[str] = []
    expected_claims: list[dict[str, Any]] = []
    graph_failure: dict[str, Any] = {}

    if (
        str(request["case_id"]) == "PERF-001"
        and isinstance(fixture, dict)
        and isinstance(fixture.get("generator"), dict)
    ):
        return await _seed_load_fixture(container, request, current, fixture)

    async def ingest_item(
        item: dict[str, Any],
        *,
        trust_tier: int | None = None,
        principal_alias: str = "default",
        require_search_visibility: bool = True,
        allow_projection_lag: bool = False,
    ) -> None:
        nonlocal accepted, failed
        prepared = copy.deepcopy(item)
        if trust_tier is not None:
            prepared["trust_tier"] = trust_tier
        record = _record_payload({"fixture": prepared}, current)
        try:
            result, _source, _queue = await _ingest(
                container,
                request,
                current,
                record,
                principal_alias=principal_alias,
                require_search_visibility=require_search_visibility,
                allow_projection_lag=allow_projection_lag,
            )
        except (AdapterBlocked, AdapterFailed):
            failed += 1
            return
        accepted += 1
        pending_claims.extend(result.claim_ids)
        triples = prepared.get("expected_triples")
        if isinstance(triples, list):
            expected_claims.extend(
                cast(list[dict[str, Any]], [value for value in triples if isinstance(value, dict)])
            )

    if isinstance(fixture, list) and fixture and all(isinstance(item, dict) for item in fixture):
        items = cast(list[dict[str, Any]], fixture)
        if all("name" in item and "canary" in item for item in items):
            for item in items:
                alias = str(item["name"])
                await _ensure_scope(request, current, alias)
                await ingest_item(
                    {
                        "external_id": f"tenant-{alias}-canary",
                        "triple": {
                            "subject": f"Tenant {alias}",
                            "predicate": "HAS_CANARY",
                            "object": str(item["canary"]),
                        },
                    },
                    principal_alias=alias,
                )
        elif all("decision" in item and "triple" in item for item in items):
            for item in items:
                await ingest_item(
                    item,
                    trust_tier=int(request["inputs"].get("trust_tier", 3)),
                    require_search_visibility=False,
                )
        elif all("trust_tier" in item and len(item) == 1 for item in items):
            for item in items:
                tier = int(item["trust_tier"])
                await ingest_item(
                    {
                        "external_id": f"trust-tier-{tier}",
                        "source_name": f"trust-tier-{tier}",
                        "triple": {
                            "subject": f"Trust Tier {tier}",
                            "predicate": "OWNS",
                            "object": f"Tier {tier} Service",
                        },
                    },
                    trust_tier=tier,
                    require_search_visibility=tier <= 2,
                )
        elif all("name" in item and "trust_tier" in item for item in items):
            triple = _CASES[str(request["case_id"])].get("fixture", {}).get("triple")
            if not isinstance(triple, dict):
                return _Outcome(
                    status="BLOCKED",
                    message="source corroboration fixture has no concrete triple adapter input",
                )
            source_ids: list[str] = []
            for item in items:
                source_id = await _ensure_source(
                    container,
                    request,
                    current,
                    trust_tier=int(item["trust_tier"]),
                    name=str(item["name"]),
                )
                source_ids.append(source_id)
                await ingest_item(
                    {
                        "external_id": f"source-{item['name']}",
                        "triple": triple,
                        "trust_tier": item["trust_tier"],
                        "source_name": item["name"],
                    }
                )
            current["seed_source_ids"] = source_ids
        else:
            for item in items:
                await ingest_item(item)
    elif isinstance(fixture, list) and fixture and all(isinstance(item, str) for item in fixture):
        for case_id in cast(list[str], fixture):
            seed_case = _CASES.get(case_id)
            seed_fixture = seed_case.get("fixture") if isinstance(seed_case, dict) else None
            if not isinstance(seed_fixture, dict):
                return _Outcome(
                    status="BLOCKED",
                    message=f"fixture.seed references an unknown or empty case: {case_id}",
                )
            records: list[dict[str, Any]] = []
            for key in ("events", "add"):
                values = seed_fixture.get(key)
                if isinstance(values, list):
                    records.extend(
                        cast(
                            list[dict[str, Any]],
                            [value for value in values if isinstance(value, dict)],
                        )
                    )
            fact = seed_fixture.get("fact")
            if isinstance(fact, dict):
                records.append(cast(dict[str, Any], fact))
            if not records:
                return _Outcome(
                    status="BLOCKED",
                    message=f"fixture.seed case has no concrete temporal records: {case_id}",
                )
            for item in records:
                await ingest_item(item)
    elif (
        isinstance(fixture, dict)
        and str(request["case_id"]) == "RES-001"
        and isinstance(fixture.get("generator"), dict)
    ):
        fact_count = int(request["inputs"].get("facts_per_scope", 0))
        if fact_count < 1:
            return _Outcome(status="BLOCKED", message="resilience seed requires facts_per_scope")
        for index in range(fact_count):
            await ingest_item(
                {
                    "external_id": f"resilience-{index:04d}",
                    "triple": {
                        "subject": f"Resilience Service {index:04d}",
                        "predicate": "RUNS_ON",
                        "object": f"resilience-node-{index:04d}",
                    },
                },
                require_search_visibility=False,
                allow_projection_lag=True,
            )
        await _ensure_scope(request, current)
        graph_failure = await _wait_for_graph_failure(
            container,
            str(current["group_id"]),
            timeout_s=_timeout("VERA_EVAL_GRAPH_FAILURE_TIMEOUT_S", _DEFAULT_TIMEOUT_S * 2),
        )
        current["graph_failure"] = graph_failure
    elif isinstance(fixture, dict):
        records = fixture.get("records", fixture.get("facts"))
        if not isinstance(records, list):
            return _Outcome(
                status="BLOCKED",
                message="fixture.seed has no concrete records/facts transport adapter",
            )
        for item in records:
            if isinstance(item, dict):
                await ingest_item(cast(dict[str, Any], item))
        if isinstance(fixture.get("facts"), list):
            current["fixture_facts"] = copy.deepcopy(fixture["facts"])
        if isinstance(fixture.get("queries"), list):
            current["queries"] = fixture["queries"]
    else:
        return _Outcome(
            status="BLOCKED", message="fixture.seed received an unsupported fixture shape"
        )

    await _ensure_scope(request, current)
    snapshot = await _database_snapshot(container, str(current["group_id"]))
    if current.get("seed_source_ids"):
        current["seed_source_ids"] = [
            episode["source_id"]
            for episode in snapshot["episodes_state"]
            if isinstance(episode.get("source_id"), str)
        ]
    actual_claims = [
        {
            "subject": claim["subject"],
            "predicate": claim["predicate"],
            "object": claim["object"],
        }
        for claim in snapshot["claims_state"]
        if claim["subject"] and claim["predicate"] and claim["object"]
    ]
    current["pending_claims"] = pending_claims
    current["expected_claims"] = expected_claims
    queue_state = (
        await _group_queue_state(container, str(current["group_id"]))
        if str(request["case_id"]) == "RES-001"
        else {}
    )
    full = {
        "ingestion": {
            "accepted_document_count": accepted,
            "failed_document_count": failed,
        },
        "resources": [str(current["group_id"])],
        "dataset": {
            "sha256": fixture_file.get("sha256") if isinstance(fixture_file, dict) else None
        },
        "actual_claims": actual_claims,
        "expected_claims": expected_claims,
        "pending_claims": pending_claims,
        "timeline": snapshot["facts_state"],
        "claims": snapshot["claims_state"],
        "episodes": snapshot["episodes_state"],
        "queue": queue_state,
        "retries": graph_failure,
        "reviews": snapshot["reviews_state"],
        "scope": {"group_ids": [str(current["group_id"])]},
        "source": {"ids": current.get("seed_source_ids", [])},
        "episode": {"ids": [episode["id"] for episode in snapshot["episodes_state"]]},
        "edge": {"ids": [edge["edge_uuid"] for edge in snapshot["graph_edges_state"]]},
        "principals": sorted(cast(dict[str, Any], current["principals"])),
        "groups": [
            value["group_id"]
            for value in cast(dict[str, dict[str, Any]], current["principals"]).values()
        ],
        "fact": {"ids": [fact["id"] for fact in snapshot["facts_state"]]},
    }
    status = "PASS" if failed == 0 else "FAIL"
    return _Outcome(
        status=status,
        observations=full,
        created=[f"group:{current['group_id']}"],
        message="" if failed == 0 else f"{failed} fixture records failed at a real boundary",
    )


def _temporal_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    facts = snapshot["facts_state"]
    current = [fact for fact in facts if fact["lifecycle_state"] == "active"]
    return {
        "current_search": current,
        "as_of_search": facts,
        "intervals": [
            {"fact_key": fact["fact_key"], "from": fact["valid_from"], "to": fact["valid_to"]}
            for fact in facts
        ],
        "provenance": [fact["fact_key"] for fact in facts],
    }


def _routing(snapshot: dict[str, Any]) -> dict[str, Any]:
    source_tiers = {str(item["id"]): int(item["trust_tier"]) for item in snapshot["sources_state"]}
    version_sources = {
        str(item["id"]): str(item["source_id"]) for item in snapshot["versions_state"]
    }
    by_tier: dict[int, list[dict[str, Any]]] = {}
    claim_tiers: dict[str, int] = {}
    for claim in snapshot["claims_state"]:
        source = version_sources.get(str(claim["artifact_version_id"]))
        tier = source_tiers.get(str(source))
        if tier is not None:
            by_tier.setdefault(tier, []).append(claim)
            claim_tiers[str(claim["id"])] = tier

    published_claim_ids = {
        str(episode.get("source_id", "")).rsplit(":", maxsplit=1)[-1]
        for episode in snapshot["episodes_state"]
    }
    return {
        "tier1_2_published_count": sum(
            1 for claim_id in published_claim_ids if claim_tiers.get(claim_id) in {1, 2}
        ),
        "tier3_status": (
            by_tier.get(3, [{}])[-1].get("verification_status") if by_tier.get(3) else None
        ),
        "tier4_status": (
            by_tier.get(4, [{}])[-1].get("verification_status") if by_tier.get(4) else None
        ),
        "shared_unverified_count": sum(
            1
            for claim in snapshot["claims_state"]
            if str(claim["id"]) in published_claim_ids
            and claim["verification_status"] == "unverified"
        ),
    }


async def _handle_search(
    container: Container,
    request: dict[str, Any],
    current: dict[str, Any],
) -> _Outcome:
    action = str(request["action"])
    inputs = cast(dict[str, Any], request["inputs"])
    alias = str(inputs.get("principal", "default"))
    principal = _principal(current, alias)
    attempted = inputs.get("attempted_scope")
    project = str(principal["group_id"])
    if isinstance(attempted, str):
        attempted_principal = _principal(current, attempted)
        project = str(attempted_principal["group_id"])

    async def run_one(query: str) -> tuple[int, dict[str, Any]]:
        if action == "search.http":
            return await _search_http(
                api_key=str(principal["api_key"]),
                query=query,
                limit=int(inputs.get("limit", 10)),
                project=project,
                as_of=_parse_time(inputs.get("as_of")),
                known_as_of=_parse_time(inputs.get("known_as_of")),
            )
        result = await _search_mcp(
            container.settings,
            principal_id=str(principal["principal_id"]),
            query=query,
            limit=int(inputs.get("limit", 10)),
            project=project,
            as_of=_parse_time(inputs.get("as_of")),
            known_as_of=_parse_time(inputs.get("known_as_of")),
        )
        return 200, result

    queries = inputs.get("queries_ref")
    if isinstance(queries, list):
        known = {
            str(item.get("query_id")): item
            for item in current.get("queries", [])
            if isinstance(item, dict)
        }
        ranked: dict[str, list[str]] = {}
        events: list[dict[str, Any]] = []
        latencies: list[float] = []
        for index, item in enumerate(queries):
            query_item = item if isinstance(item, dict) else known.get(str(item))
            if not isinstance(query_item, dict):
                return _Outcome(
                    status="BLOCKED", message=f"query reference {item!r} was not seeded"
                )
            status, result = await run_one(str(query_item.get("text", "")))
            if not 200 <= status < 300:
                return _Outcome(
                    status="FAIL",
                    message=f"{action} returned status {status} for query {index}",
                    boundaries=("api" if action == "search.http" else "mcp",),
                )
            query_id = str(query_item.get("query_id", index))
            facts = [
                cast(dict[str, Any], value)
                for value in result["facts"]
                if isinstance(value, dict) and isinstance(value.get("id"), str)
            ]
            result_ids = [str(value["id"]) for value in facts]
            ranked[query_id] = [
                _fixture_fact_id(value, current) or str(value["id"]) for value in facts
            ]
            events.append(
                {
                    "query_id": query_id,
                    "result_ids": result_ids,
                    "results": facts,
                }
            )
            latencies.append(float(result["latency_ms"]))
        return _Outcome(
            observations={
                "ranked_results": ranked,
                "retrieval": {"events": events},
                "signals": {
                    event["query_id"]: [value.get("signals") for value in event["results"]]
                    for event in events
                },
                "latencies": latencies,
            },
            boundaries=("api" if action == "search.http" else "mcp",),
        )

    query = str(inputs.get("query", ""))
    try:
        status, result = await run_one(query)
    except AdapterFailed as exc:
        if attempted is None:
            raise
        result = {
            "results": [],
            "facts": [],
            "answerable_result_count": 0,
            "bounded_outcome": "authorization_error",
        }
        status = 403
        result["error"] = type(exc).__name__
    full: dict[str, Any] = {}
    for path in request["observe"]:
        _set_path(full, path, result)
    boundary = "http" if action == "search.http" else "mcp"
    current["searches"][boundary] = result["facts"]
    full.setdefault("search", {})["http_mcp_equivalent"] = (
        "http" in current["searches"]
        and "mcp" in current["searches"]
        and _search_equivalence_key(current["searches"]["http"])
        == _search_equivalence_key(current["searches"]["mcp"])
    )
    full.setdefault("search", {})["result_joinable"] = all(
        item.get("id") and isinstance(item.get("citation"), dict) for item in result["facts"]
    )
    expected_denial = attempted is not None and status in {403, 404}
    action_status = "PASS" if 200 <= status < 300 or expected_denial else "FAIL"
    return _Outcome(
        status=action_status,
        observations=full,
        message="" if action_status == "PASS" else f"{action} returned product status {status}",
        boundaries=("api" if action == "search.http" else "mcp",),
    )


def _fixture_fact_id(result: dict[str, Any], current: dict[str, Any]) -> str | None:
    citation = result.get("citation")
    structured = citation.get("structured_record") if isinstance(citation, dict) else None
    if not isinstance(structured, dict):
        return None
    observed = {
        key: str(structured.get(key, "")).casefold() for key in ("subject", "predicate", "object")
    }
    for fact in current.get("fixture_facts", []):
        if not isinstance(fact, dict) or not isinstance(fact.get("triple"), dict):
            continue
        triple = fact["triple"]
        expected = {
            key: str(triple.get(key, "")).casefold() for key in ("subject", "predicate", "object")
        }
        if observed == expected and isinstance(fact.get("fact_id"), str):
            return str(fact["fact_id"])
    return None


def _labeled_queries(case_id: str) -> dict[str, dict[str, Any]]:
    case = _CASES.get(case_id)
    if case is None:
        raise AdapterBlocked(f"evaluation case {case_id!r} is not declared")
    queries = fixture_data(case).get("queries")
    if not isinstance(queries, list):
        raise AdapterBlocked(f"evaluation case {case_id!r} has no labeled queries")
    return {
        str(item["query_id"]): item
        for item in queries
        if isinstance(item, dict)
        and isinstance(item.get("query_id"), str)
        and isinstance(item.get("relevance"), dict)
    }


async def _feedback_submit(
    container: Container,
    request: dict[str, Any],
    current: dict[str, Any],
) -> _Outcome:
    inputs = cast(dict[str, Any], request["inputs"])
    labels = inputs.get("labels_ref")
    history = current.get("observations", {})
    internal_events = (
        history.get("retrieval", {}).get("events") if isinstance(history, dict) else None
    )
    events = (
        internal_events if isinstance(internal_events, list) else inputs.get("retrieval_events_ref")
    )
    if not isinstance(labels, list) or not isinstance(events, list):
        return _Outcome(status="BLOCKED", message="feedback labels or retrieval events are missing")
    label_query_ids = {
        str(label.get("query_id")) if isinstance(label, dict) else str(label) for label in labels
    }
    queries = _labeled_queries(str(request["case_id"]))
    principal = _principal(current, "default")
    repetitions = int(
        _CASES[str(request["case_id"])]["fixture"].get("train_feedback_repetitions", 1)
    )
    submitted: list[dict[str, Any]] = []
    joined = 0
    ambiguous = 0
    expected = 0
    for event in events:
        if not isinstance(event, dict):
            continue
        query_id = str(event.get("query_id", ""))
        if query_id not in label_query_ids:
            continue
        query = queries.get(query_id)
        if query is None or not isinstance(query.get("relevance"), dict):
            ambiguous += 1
            continue
        relevance = cast(dict[str, Any], query["relevance"])
        results = event.get("results")
        if not isinstance(results, list):
            ambiguous += 1
            continue
        for result in results:
            if not isinstance(result, dict) or not isinstance(result.get("id"), str):
                continue
            expected += 1
            fact_id = _fixture_fact_id(cast(dict[str, Any], result), current)
            if fact_id is None:
                ambiguous += 1
                continue
            signal = "up" if float(relevance.get(fact_id, 0)) > 0 else "down"
            raw_signals = result.get("signals")
            signals = {
                str(key): float(value)
                for key, value in cast(dict[str, Any], raw_signals or {}).items()
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            }
            for _ in range(repetitions):
                _, response = await _api_json(
                    "POST",
                    "/v2/knowledge/feedback",
                    api_key=str(principal["api_key"]),
                    body={
                        "result_ref": str(result["id"]),
                        "signal": signal,
                        "query": str(query.get("text", "")),
                        "signals": signals,
                    },
                    expected={200},
                )
                submitted.append(response)
            joined += 1
    rate = joined / expected if expected else 0.0
    async with container.sessionmaker() as session:
        rows = list(
            (
                await session.execute(
                    text(
                        "SELECT id::text, group_id, result_ref, signal FROM retrieval_feedback "
                        "WHERE principal_id=:principal_id ORDER BY created_at, id"
                    ),
                    {"principal_id": UUID(str(principal["principal_id"]))},
                )
            ).mappings()
        )
    feedback_rows = _rows(rows)
    current["feedback_group_ids"] = sorted({str(row["group_id"]) for row in feedback_rows})
    return _Outcome(
        status="PASS" if rate == 1.0 and ambiguous == 0 else "FAIL",
        observations={
            "feedback": {
                "events": feedback_rows,
                "joins": {"rate": rate, "ambiguity_count": ambiguous},
                "submitted_count": len(submitted),
            }
        },
        created=[f"feedback:{row['id']}" for row in feedback_rows],
        message=(
            "" if rate == 1.0 and ambiguous == 0 else "feedback labels did not join every result"
        ),
        boundaries=("api", "database"),
    )


def _ranking_quality(
    events: list[dict[str, Any]],
    current: dict[str, Any],
    queries: dict[str, dict[str, Any]],
    *,
    weights: Any | None = None,
) -> tuple[float, float]:
    hits = 0
    reciprocal_ranks = 0.0
    samples = 0
    for event in events:
        query = queries.get(str(event.get("query_id", "")))
        results = event.get("results")
        if (
            query is None
            or not isinstance(query.get("relevance"), dict)
            or not isinstance(results, list)
        ):
            continue
        ranked = [cast(dict[str, Any], result) for result in results if isinstance(result, dict)]
        if weights is not None:
            ranked.sort(
                key=lambda result: sum(
                    float(cast(dict[str, Any], result.get("signals") or {}).get(name, 0.5))
                    * float(getattr(weights, name))
                    for name in (
                        "relevance",
                        "authority",
                        "verification",
                        "recency",
                        "feedback",
                        "confidence",
                    )
                ),
                reverse=True,
            )
        relevant = cast(dict[str, Any], query["relevance"])
        rank = next(
            (
                index
                for index, result in enumerate(ranked, start=1)
                if float(relevant.get(_fixture_fact_id(result, current) or "", 0)) > 0
            ),
            None,
        )
        samples += 1
        if rank is not None and rank <= 5:
            hits += 1
        if rank is not None:
            reciprocal_ranks += 1.0 / rank
    if samples == 0:
        raise AdapterBlocked("holdout scoring found no labeled retrieval events")
    return hits / samples, reciprocal_ranks / samples


async def _calibration_evaluate(
    container: Container,
    request: dict[str, Any],
    current: dict[str, Any],
) -> _Outcome:
    inputs = cast(dict[str, Any], request["inputs"])
    if inputs.get("apply") is not False:
        return _Outcome(status="BLOCKED", message="evaluation calibration must not apply weights")
    group_ids = current.get("feedback_group_ids")
    if not isinstance(group_ids, list) or not group_ids:
        return _Outcome(status="BLOCKED", message="calibration has no run-owned feedback scope")
    base = build_rerank_weights(container.settings)
    candidate, sample_count = await CalibrationService(
        container.retrieval_read
    ).calibrate_with_count(
        group_ids=[str(value) for value in group_ids],
        half_life_s=base.half_life_s,
        fallback=base,
    )
    holdout_ids = inputs.get("holdout_query_ids_ref")
    queries = _labeled_queries(str(request["case_id"]))
    if not isinstance(holdout_ids, list):
        return _Outcome(status="BLOCKED", message="calibration holdout query IDs are missing")
    principal = _principal(current, "default")
    events: list[dict[str, Any]] = []
    for query_id in holdout_ids:
        query = queries.get(str(query_id))
        if query is None:
            raise AdapterBlocked(f"holdout query {query_id!r} was not seeded")
        result = await _search_mcp(
            container.settings,
            principal_id=str(principal["principal_id"]),
            query=str(query.get("text", "")),
            limit=10,
            project=str(principal["group_id"]),
        )
        events.append(
            {
                "query_id": str(query_id),
                "result_ids": [str(item["id"]) for item in result["facts"]],
                "results": result["facts"],
            }
        )
    current_hit, current_mrr = _ranking_quality(events, current, queries)
    candidate_hit, candidate_mrr = _ranking_quality(events, current, queries, weights=candidate)
    hit_delta = candidate_hit - current_hit
    mrr_delta = candidate_mrr - current_mrr
    async with container.sessionmaker() as session:
        unrelated = int(
            await session.scalar(
                text(
                    "SELECT count(*) FROM retrieval_feedback "
                    "WHERE group_id = ANY(CAST(:group_ids AS text[])) "
                    "AND principal_id <> :principal_id"
                ),
                {
                    "group_ids": [str(value) for value in group_ids],
                    "principal_id": UUID(str(principal["principal_id"])),
                },
            )
            or 0
        )
    return _Outcome(
        observations={
            "candidate": {
                "applied": False,
                "sample_count": sample_count,
                "weights": {
                    name: float(getattr(candidate, name))
                    for name in (
                        "relevance",
                        "authority",
                        "verification",
                        "recency",
                        "feedback",
                        "confidence",
                    )
                },
            },
            "holdout": {
                "hit_at_5_delta": hit_delta,
                "mrr_delta": mrr_delta,
                "quality_delta": min(hit_delta, mrr_delta),
                "events": events,
            },
            "calibration": {
                "read_scope": {
                    "group_ids": [str(value) for value in group_ids],
                    "unrelated_row_count": unrelated,
                }
            },
        },
        boundaries=("mcp", "database"),
    )


def _text_hit(facts: list[Any], expected: dict[str, Any] | None) -> bool:
    product_facts = [cast(dict[str, Any], item) for item in facts[:5] if isinstance(item, dict)]
    if expected is None:
        return not product_facts
    expected_text = " ".join(
        str(expected.get(key, "")) for key in ("subject", "predicate", "object")
    ).casefold()
    return any(
        expected_text in " ".join(str(item.get("fact", "")).split()).casefold()
        for item in product_facts
    )


def _caused_by_timeout(error: Exception) -> bool:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, TimeoutError) or "timeout" in type(current).__name__.casefold():
            return True
        current = current.__cause__
    return False


async def _load_search(
    container: Container, request: dict[str, Any], current: dict[str, Any]
) -> _Outcome:
    matrix = request["inputs"].get("matrix_ref")
    if not isinstance(matrix, dict):
        raise AdapterBlocked("load.search matrix_ref must resolve to an object")
    for key, expected in _SEARCH_MATRIX.items():
        _validated_matrix_list(matrix, key, expected)
    duration_s = matrix.get("duration_s")
    if isinstance(duration_s, bool) or not isinstance(duration_s, (int, float)) or duration_s <= 0:
        raise AdapterBlocked("load.search duration_s must be a positive number")

    load_fixture = current.get("load_fixture")
    if not isinstance(load_fixture, dict):
        raise AdapterBlocked("load.search requires the PERF-001 generated fixture state")
    scope_count = int(load_fixture.get("scope_count", 0))
    facts_per_scope = int(load_fixture.get("facts_per_scope", 0))
    query_count = int(load_fixture.get("query_count", 0))
    seed = int(load_fixture.get("seed", 0))
    aliases = load_fixture.get("aliases")
    if (
        scope_count != 20
        or facts_per_scope != 200
        or query_count != 200
        or not isinstance(aliases, list)
    ):
        raise AdapterBlocked("load.search generated fixture state is incomplete")
    scope_aliases = [str(value) for value in aliases]
    if len(scope_aliases) != scope_count:
        raise AdapterBlocked("load.search scope list does not match the generated fixture")
    principals = [_principal(current, alias) for alias in scope_aliases]
    group_ids = [str(principal["group_id"]) for principal in principals]
    tokens_before = await _groups_token_count(container, group_ids, request_kind="search")

    accumulators: dict[tuple[str, str, int, int, int], dict[str, Any]] = {}
    cold_queries: dict[tuple[str, int, int, int], list[tuple[str, dict[str, Any] | None, int]]] = {}
    used_query_texts: set[str] = set()
    fact_cursors = [0] * scope_count
    query_serial = 0

    def cold_query_batch(
        count: int, selected_scope_count: int
    ) -> list[tuple[str, dict[str, Any] | None, int]]:
        nonlocal query_serial
        batch: list[tuple[str, dict[str, Any] | None, int]] = []
        while len(batch) < count:
            serial = query_serial
            query_serial += 1
            scope_index = serial % selected_scope_count
            if serial % 10 == 0:
                batch.append((f"Unknown canary question {serial}", None, scope_index))
                continue
            generated: dict[str, Any] | None = None
            for _ in range(facts_per_scope):
                fact_index = fact_cursors[scope_index] % facts_per_scope
                fact_cursors[scope_index] += 1
                candidate = load_fact(scope_index, fact_index, seed, facts_per_scope)
                text_value = query_for(candidate)
                if text_value not in used_query_texts:
                    generated = candidate
                    used_query_texts.add(text_value)
                    break
            if generated is None:
                fact_index = fact_cursors[scope_index] % facts_per_scope
                fact_cursors[scope_index] += 1
                generated = load_fact(scope_index, fact_index, seed, facts_per_scope)
            batch.append(
                (query_for(generated), cast(dict[str, Any], generated["triple"]), scope_index)
            )
        return batch

    combinations = [
        cast(tuple[str, str, int, int, int], values)
        for values in product(
            _SEARCH_MATRIX["entrypoints"],
            _SEARCH_MATRIX["cache_states"],
            _SEARCH_MATRIX["scope_counts"],
            _SEARCH_MATRIX["result_limits"],
            _SEARCH_MATRIX["virtual_users"],
        )
    ]

    async def run_combination(key: tuple[str, str, int, int, int]) -> None:
        entrypoint, cache_state, selected_scope_count, result_limit, virtual_users = key
        request_count = (
            query_count
            if entrypoint == "http"
            and selected_scope_count == 20
            and result_limit == 10
            and virtual_users == 20
            else max(3, virtual_users, selected_scope_count)
        )
        cold_key = (entrypoint, selected_scope_count, result_limit, virtual_users)
        if cache_state == "cold":
            query_batch = cold_query_batch(request_count, selected_scope_count)
            cold_queries[cold_key] = query_batch
        else:
            available = cold_queries.get(cold_key)
            if not available:
                raise AdapterBlocked("warm search profile has no comparable cold query batch")
            query_batch = [available[index % len(available)] for index in range(request_count)]

        samples: list[dict[str, Any] | None] = [None] * request_count
        semaphore = asyncio.Semaphore(virtual_users)

        async def run_one(
            index: int, query: str, expected: dict[str, Any] | None, scope_index: int
        ) -> None:
            principal = principals[scope_index]
            started = time.perf_counter()
            try:
                async with semaphore:
                    if entrypoint == "http":
                        status, result = await _search_http(
                            api_key=str(principal["api_key"]),
                            query=query,
                            limit=result_limit,
                            project=str(principal["group_id"]),
                        )
                    else:
                        result = await _search_mcp(
                            container.settings,
                            principal_id=str(principal["principal_id"]),
                            query=query,
                            limit=result_limit,
                            project=str(principal["group_id"]),
                        )
                        status = 200
                facts = result.get("facts")
                actual_facts = facts if isinstance(facts, list) else []
                samples[index] = {
                    "latency_ms": float(
                        result.get("latency_ms", (time.perf_counter() - started) * 1000)
                    ),
                    "error": not 200 <= status < 300,
                    "timeout": result.get("bounded_outcome") == "timeout" or status == 504,
                    "hit": 200 <= status < 300 and _text_hit(actual_facts, expected),
                }
            except Exception as exc:
                samples[index] = {
                    "latency_ms": (time.perf_counter() - started) * 1000,
                    "error": True,
                    "timeout": _caused_by_timeout(exc),
                    "hit": False,
                }

        profile_started = time.perf_counter()
        async with asyncio.TaskGroup() as task_group:
            for index, (query, expected, scope_index) in enumerate(query_batch):
                task_group.create_task(run_one(index, query, expected, scope_index))
        elapsed_s = max(time.perf_counter() - profile_started, 1e-9)
        observed = [cast(dict[str, Any], sample) for sample in samples]
        accumulator = accumulators.setdefault(
            key,
            {
                "latencies": [],
                "errors": 0,
                "timeouts": 0,
                "hits": 0,
                "elapsed_s": 0.0,
            },
        )
        cast(list[float], accumulator["latencies"]).extend(
            float(sample["latency_ms"]) for sample in observed
        )
        accumulator["errors"] = int(accumulator["errors"]) + sum(
            bool(sample["error"]) for sample in observed
        )
        accumulator["timeouts"] = int(accumulator["timeouts"]) + sum(
            bool(sample["timeout"]) for sample in observed
        )
        accumulator["hits"] = int(accumulator["hits"]) + sum(
            bool(sample["hit"]) for sample in observed
        )
        accumulator["elapsed_s"] = float(accumulator["elapsed_s"]) + elapsed_s

    workload_started = time.monotonic()
    deadline = workload_started + float(duration_s)
    for combination in combinations:
        await run_combination(combination)
    while time.monotonic() < deadline:
        started_profile = False
        for combination in combinations:
            if time.monotonic() >= deadline:
                break
            started_profile = True
            await run_combination(combination)
        if not started_profile:
            break

    tokens_after = await _groups_token_count(container, group_ids, request_kind="search")
    if tokens_after < tokens_before:
        raise AdapterFailed("durable search token usage decreased during the workload")
    profiles: list[dict[str, Any]] = []
    all_latencies: list[float] = []
    total_errors = 0
    total_timeouts = 0
    total_hits = 0
    total_elapsed_s = 0.0
    for key in combinations:
        accumulator = accumulators[key]
        latencies = cast(list[float], accumulator["latencies"])
        sample_count = len(latencies)
        entrypoint, cache_state, selected_scope_count, result_limit, virtual_users = key
        profile = {
            "profile_id": (
                f"{entrypoint}-{cache_state}-s{selected_scope_count}-"
                f"l{result_limit}-vu{virtual_users}"
            ),
            "entrypoint": entrypoint,
            "cache_state": cache_state,
            "scope_count": selected_scope_count,
            "scope_semantics": "active_isolated_scopes",
            "result_limit": result_limit,
            "virtual_users": virtual_users,
            "p50_ms": round(_nearest_rank(latencies, 0.50), 3),
            "p95_ms": round(_nearest_rank(latencies, 0.95), 3),
            "p99_ms": round(_nearest_rank(latencies, 0.99), 3),
            "error_count": int(accumulator["errors"]),
            "error_rate": int(accumulator["errors"]) / sample_count,
            "timeout_count": int(accumulator["timeouts"]),
            "timeout_rate": int(accumulator["timeouts"]) / sample_count,
            "throughput_rps": sample_count / float(accumulator["elapsed_s"]),
            "hit_at_5": int(accumulator["hits"]) / sample_count,
            "sample_count": sample_count,
        }
        profiles.append(profile)
        all_latencies.extend(latencies)
        total_errors += int(accumulator["errors"])
        total_timeouts += int(accumulator["timeouts"])
        total_hits += int(accumulator["hits"])
        total_elapsed_s += float(accumulator["elapsed_s"])

    total_samples = len(all_latencies)
    reference = next(
        profile
        for profile in profiles
        if profile["entrypoint"] == "http"
        and profile["cache_state"] == "warm"
        and profile["scope_count"] == 20
        and profile["result_limit"] == 10
        and profile["virtual_users"] == 20
    )
    token_delta = tokens_after - tokens_before
    if token_delta <= 0:
        raise AdapterFailed("search workload produced no provider-reported token usage")
    return _Outcome(
        observations={
            "profiles": {
                "matrix": profiles,
                "http_reference": copy.deepcopy(reference),
                "max_error_rate": max(float(profile["error_rate"]) for profile in profiles),
                "max_hit_at_5_delta_pp": None,
            }
        },
        metrics=[
            _metric(
                "p50_ms",
                round(_nearest_rank(all_latencies, 0.50), 3),
                sample_size=total_samples,
                unit="ms",
            ),
            _metric(
                "p95_ms",
                float(reference["p95_ms"]),
                sample_size=int(reference["sample_count"]),
                unit="ms",
            ),
            _metric(
                "p99_ms",
                round(_nearest_rank(all_latencies, 0.99), 3),
                sample_size=total_samples,
                unit="ms",
            ),
            _metric(
                "error_rate",
                total_errors / total_samples,
                sample_size=total_samples,
                unit="ratio",
            ),
            _metric(
                "timeout_rate",
                total_timeouts / total_samples,
                sample_size=total_samples,
                unit="ratio",
            ),
            _metric(
                "throughput_rps",
                total_samples / total_elapsed_s,
                sample_size=total_samples,
                unit="requests/s",
            ),
            _metric(
                "hit_at_5",
                total_hits / total_samples,
                sample_size=total_samples,
                unit="ratio",
            ),
            _metric(
                "tokens_per_search",
                token_delta / total_samples,
                sample_size=total_samples,
                unit="tokens/search",
                dimensions={"source": "llm_usage"},
            ),
        ],
        boundaries=("api", "mcp", "database"),
    )


def _load_ingestion_record(
    *,
    profile_index: int,
    record_index: int,
    claims_per_record: int,
    facts_per_scope: int,
    seed: int,
) -> dict[str, Any]:
    generated = [
        load_fact(
            profile_index,
            record_index * claims_per_record + claim_index,
            seed,
            facts_per_scope,
        )
        for claim_index in range(claims_per_record)
    ]
    return {
        "external_id": f"load-{profile_index:02d}-{record_index:04d}",
        "knowledge_type": "fact_triple",
        "source_event_time": generated[0]["source_event_time"],
        "metadata": {"triples": [item["triple"] for item in generated]},
    }


def _canonical_active_facts(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "subject": str(item["subject"]),
            "predicate": str(item["predicate"]),
            "object": str(item["object"]),
        }
        for item in snapshot.get("facts_state", [])
        if isinstance(item, dict)
        and item.get("lifecycle_state") == "active"
        and all(item.get(key) is not None for key in ("subject", "predicate", "object"))
    ]


async def _artifact_durable_times(
    container: Container, group_id: str, artifact_version_ids: list[str]
) -> dict[str, datetime]:
    if not artifact_version_ids:
        return {}
    async with container.sessionmaker() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT av.id::text AS id, av.observed_at AS completed_at "
                    "FROM artifact_versions av "
                    "JOIN artifacts a ON a.id=av.artifact_id "
                    "JOIN knowledge_sources s ON s.id=a.source_id "
                    "LEFT JOIN projects p ON p.id=s.project_id "
                    "WHERE p.group_id=:group_id "
                    "AND av.id = ANY(CAST(:artifact_version_ids AS uuid[]))"
                ),
                {
                    "group_id": group_id,
                    "artifact_version_ids": artifact_version_ids,
                },
            )
        ).mappings()
    result = {
        str(row["id"]): row["completed_at"]
        for row in rows
        if isinstance(row["completed_at"], datetime)
    }
    if len(result) != len(artifact_version_ids):
        raise AdapterFailed("durable artifact completion timestamps are incomplete")
    return result


async def _load_ingestion(
    container: Container, request: dict[str, Any], current: dict[str, Any]
) -> _Outcome:
    matrix = request["inputs"].get("matrix_ref")
    if not isinstance(matrix, dict):
        raise AdapterBlocked("load.ingestion matrix_ref must resolve to an object")
    for key, expected in _INGESTION_MATRIX.items():
        _validated_matrix_list(matrix, key, expected)
    claims_per_record = matrix.get("claims_per_record")
    if isinstance(claims_per_record, bool) or claims_per_record != 2:
        raise AdapterBlocked("load.ingestion claims_per_record must equal 2")
    fixture = fixture_data(_CASES[str(request["case_id"])])
    scope_count, _facts_per_scope, _query_count, seed, _expected_sha = _generator_parameters(
        fixture
    )
    combinations = [
        cast(tuple[int, int], values)
        for values in product(_INGESTION_MATRIX["record_counts"], _INGESTION_MATRIX["concurrency"])
    ]
    expected_fixture: list[dict[str, str]] = []
    final_state: list[dict[str, str]] = []
    profiles: list[dict[str, Any]] = []
    queue_wait_samples: list[float] = []
    searchable_samples: list[float] = []
    total_tokens = 0
    total_records = 0
    total_ingest_s = 0.0
    total_failures = 0
    created: list[str] = []

    for profile_index, (record_count, concurrency) in enumerate(combinations):
        alias = f"ingestion-{record_count}-c{concurrency}"
        principal = await _ensure_scope(request, current, alias)
        source_id = await _ensure_source(container, request, current, principal_alias=alias)
        group_id = str(principal["group_id"])
        created.extend((f"group:{group_id}", f"source:{source_id}"))
        generated_records = [
            _load_ingestion_record(
                # Keep the ingestion corpus disjoint from PERF-001's cached search corpus.
                profile_index=scope_count + profile_index,
                record_index=index,
                claims_per_record=claims_per_record,
                facts_per_scope=record_count * claims_per_record,
                seed=seed,
            )
            for index in range(record_count)
        ]
        profile_expected = [
            {
                "subject": str(triple["subject"]),
                "predicate": str(triple["predicate"]),
                "object": str(triple["object"]),
            }
            for record in generated_records
            for triple in cast(list[dict[str, Any]], record["metadata"]["triples"])
        ]
        expected_fixture.extend(profile_expected)
        tokens_before = await _groups_token_count(container, [group_id], request_kind="ingest")
        artifact_version_ids: list[str | None] = [None] * record_count
        ingest_failures: list[str | None] = [None] * record_count
        semaphore = asyncio.Semaphore(concurrency)

        async def ingest_one(
            index: int,
            generated_record: dict[str, Any],
            profile_alias: str,
            profile_source_id: str,
            limiter: asyncio.Semaphore,
            failures: list[str | None],
            artifacts: list[str | None],
        ) -> None:
            task_current = _load_task_current(
                current, alias=profile_alias, source_id=profile_source_id
            )
            record = _record_payload({"fixture": generated_record}, task_current)
            try:
                async with limiter:
                    result, _source, _queue = await _ingest(
                        container,
                        request,
                        task_current,
                        record,
                        principal_alias=profile_alias,
                        allow_projection_lag=True,
                        require_search_visibility=False,
                    )
            except (AdapterBlocked, AdapterFailed) as exc:
                failures[index] = type(exc).__name__
            else:
                artifacts[index] = str(result.artifact_version_id)

        ingest_started = time.perf_counter()
        async with asyncio.TaskGroup() as task_group:
            for index, generated_record in enumerate(generated_records):
                task_group.create_task(
                    ingest_one(
                        index,
                        generated_record,
                        alias,
                        source_id,
                        semaphore,
                        ingest_failures,
                        artifact_version_ids,
                    )
                )
        ingest_elapsed_s = max(time.perf_counter() - ingest_started, 1e-9)
        queue_state = await _wait_for_group_jobs(
            container,
            group_id,
            timeout_s=_timeout("VERA_EVAL_LOAD_SETTLE_TIMEOUT_S", _DEFAULT_TIMEOUT_S * 20),
        )
        queue_confirmed_at = datetime.now(UTC)
        completed_ids = [value for value in artifact_version_ids if isinstance(value, str)]
        durable_times = await _artifact_durable_times(container, group_id, completed_ids)

        visible: list[bool] = [False] * record_count
        search_semaphore = asyncio.Semaphore(concurrency)

        async def probe_one(
            index: int,
            profile_records: list[dict[str, Any]],
            limiter: asyncio.Semaphore,
            profile_principal: dict[str, Any],
            profile_group_id: str,
            visibility: list[bool],
        ) -> None:
            triples = cast(list[dict[str, Any]], profile_records[index]["metadata"]["triples"])
            query = " ".join(
                str(triples[0].get(key, "")) for key in ("subject", "predicate", "object")
            )
            try:
                async with limiter:
                    status, result = await _search_http(
                        api_key=str(profile_principal["api_key"]),
                        query=query,
                        limit=5,
                        project=profile_group_id,
                    )
            except Exception:
                return
            facts = result.get("facts")
            visibility[index] = (
                200 <= status < 300 and isinstance(facts, list) and _text_hit(facts, triples[0])
            )

        async with asyncio.TaskGroup() as task_group:
            for index, artifact_version_id in enumerate(artifact_version_ids):
                if isinstance(artifact_version_id, str):
                    task_group.create_task(
                        probe_one(
                            index,
                            generated_records,
                            search_semaphore,
                            principal,
                            group_id,
                            visible,
                        )
                    )
        search_confirmed_at = datetime.now(UTC)
        snapshot = await _database_snapshot(container, group_id)
        profile_final = _canonical_active_facts(snapshot)
        final_state.extend(profile_final)
        tokens_after = await _groups_token_count(container, [group_id], request_kind="ingest")
        if tokens_after < tokens_before:
            raise AdapterFailed("durable ingestion token usage decreased during the profile")
        token_delta = tokens_after - tokens_before
        if token_delta <= 0:
            raise AdapterFailed("ingestion profile produced no provider-reported token usage")

        profile_queue_samples = [
            max(0.0, (queue_confirmed_at - durable_times[value]).total_seconds() * 1000)
            for value in completed_ids
        ]
        profile_searchable_samples = [
            max(0.0, (search_confirmed_at - durable_times[value]).total_seconds() * 1000)
            for index, value in enumerate(artifact_version_ids)
            if isinstance(value, str) and visible[index]
        ]
        if not profile_queue_samples or not profile_searchable_samples:
            raise AdapterFailed(
                "ingestion profile produced no confirmed durable visibility samples"
            )
        failure_count = sum(value is not None for value in ingest_failures)
        visibility_failure_count = sum(
            isinstance(value, str) and not visible[index]
            for index, value in enumerate(artifact_version_ids)
        )
        profiles.append(
            {
                "profile_id": f"records-{record_count}-c{concurrency}",
                "record_count": record_count,
                "claims_per_record": claims_per_record,
                "concurrency": concurrency,
                "records_per_second": record_count / ingest_elapsed_s,
                "queue_wait_p95_ms": round(_nearest_rank(profile_queue_samples, 0.95), 3),
                "time_to_searchable_p95_ms": round(
                    _nearest_rank(profile_searchable_samples, 0.95), 3
                ),
                "tokens_per_artifact": token_delta / record_count,
                "ingest_failure_count": failure_count,
                "visibility_failure_count": visibility_failure_count,
                "queue_state": queue_state,
                "sample_count": record_count,
            }
        )
        queue_wait_samples.extend(profile_queue_samples)
        searchable_samples.extend(profile_searchable_samples)
        total_tokens += token_delta
        total_records += record_count
        total_ingest_s += ingest_elapsed_s
        total_failures += failure_count + visibility_failure_count

    current["group_id"] = str(
        _principal(current, f"ingestion-{combinations[0][0]}-c{combinations[0][1]}")["group_id"]
    )
    return _Outcome(
        status="PASS" if total_failures == 0 else "FAIL",
        observations={
            "profiles": {
                "matrix": profiles,
                "expected_fixture": expected_fixture,
                "final_state": final_state,
                "p95_time_to_searchable_relative_delta": None,
            }
        },
        metrics=[
            _metric(
                "records_per_second",
                total_records / total_ingest_s,
                sample_size=total_records,
                unit="records/s",
            ),
            _metric(
                "queue_wait_p95_ms",
                round(_nearest_rank(queue_wait_samples, 0.95), 3),
                sample_size=len(queue_wait_samples),
                unit="ms",
            ),
            _metric(
                "time_to_searchable_p95_ms",
                round(_nearest_rank(searchable_samples, 0.95), 3),
                sample_size=len(searchable_samples),
                unit="ms",
            ),
            _metric(
                "tokens_per_artifact",
                total_tokens / total_records,
                sample_size=total_records,
                unit="tokens/artifact",
                dimensions={"source": "llm_usage"},
            ),
        ],
        created=created,
        message=("" if total_failures == 0 else f"{total_failures} ingestion boundaries failed"),
        boundaries=("database", "api", "graph"),
    )


async def _handle_action(
    container: Container,
    request: dict[str, Any],
    state: dict[str, Any],
    current: dict[str, Any],
) -> _Outcome:
    action = str(request["action"])
    inputs = cast(dict[str, Any], request["inputs"])
    if action == "source.create":
        fixture = inputs.get("fixture") if isinstance(inputs.get("fixture"), dict) else {}
        source_id = await _ensure_source(
            container,
            request,
            current,
            trust_tier=int(fixture.get("trust_tier", 1)),
            kind=str(fixture.get("kind", "filesystem")),
        )
        return _Outcome(
            observations={
                "source": {"id": source_id},
                "scope": {"group_id": current["group_id"]},
            },
            created=[f"group:{current['group_id']}", f"source:{source_id}"],
            boundaries=("api", "database"),
        )
    if action == "record.ingest":
        await _ensure_scope(request, current)
        record = _record_payload(inputs, current)
        result, source_id, queue = await _ingest(
            container,
            request,
            current,
            record,
            require_search_visibility=(
                inputs.get("require_search_visibility") is not False
                and str(request["case_id"]) != "PROJ-001"
            ),
            allow_projection_lag=(
                str(request["case_id"]) == "PROJ-001" and str(request["step_id"]) == "S4"
            ),
        )
        snapshot = await _database_snapshot(container, str(current["group_id"]))
        observations = _ingest_observations(
            request,
            result,
            source_id,
            str(current["group_id"]),
            queue,
            snapshot,
            record,
        )
        return _Outcome(
            observations=observations,
            created=[f"artifact-version:{result.artifact_version_id}"],
        )
    if action == "projection.wait":
        await _ensure_scope(request, current)
        return await _projection_observation(container, request, current)
    if action in {"search.http", "search.mcp"}:
        await _ensure_scope(request, current)
        return await _handle_search(container, request, current)
    if action == "fixture.seed":
        return await _seed(container, request, current)
    if action == "load.search":
        return await _load_search(container, request, current)
    if action == "load.ingestion":
        return await _load_ingestion(container, request, current)
    if action == "state.snapshot":
        await _ensure_scope(request, current)
        snapshot = await _database_snapshot(container, str(current["group_id"]))
        full: dict[str, Any] = {}
        for path in request["observe"]:
            if path == "counts.after":
                _set_path(full, path, snapshot)
            elif path == "lineage":
                versions = snapshot["versions_state"]
                order_valid = all(
                    item["predecessor_version_id"] == versions[index - 1]["id"]
                    for index, item in enumerate(versions[1:], start=1)
                    if item["artifact_id"] == versions[index - 1]["artifact_id"]
                )
                _set_path(
                    full,
                    path,
                    {
                        "artifact_version_count": snapshot["versions"],
                        "version_order_valid": order_valid,
                    },
                )
            elif path == "routing":
                _set_path(full, path, _routing(snapshot))
            elif path == "processed":
                external_ids = [item["external_id"] for item in snapshot["versions_state"]]
                _set_path(
                    full,
                    path,
                    {
                        "external_ids": external_ids,
                        "duplicate_count": len(external_ids) - len(set(external_ids)),
                    },
                )
            elif path == "provenance":
                active_fact_ids = {
                    str(item["id"])
                    for item in snapshot["facts_state"]
                    if item["lifecycle_state"] == "active"
                }
                active_source_ids = {
                    str(item["knowledge_source_id"])
                    for item in snapshot["assertions_state"]
                    if str(item["fact_id"]) in active_fact_ids
                    and item["state"] == "active"
                    and item["knowledge_source_id"] is not None
                }
                active_sources = [
                    item["name"]
                    for item in snapshot["sources_state"]
                    if str(item["id"]) in active_source_ids
                ]
                _set_path(
                    full,
                    path,
                    {
                        "active_source_count": len(active_sources),
                        "active_sources": active_sources,
                    },
                )
            else:
                _set_path(full, path, _temporal_snapshot(snapshot))
        return _Outcome(observations=full)
    if action == "agent.run":
        await _ensure_scope(request, current)
        return _Outcome(
            observations=await _agent(container, request, current),
            boundaries=("mcp", "api"),
        )
    if action == "extractor.configure":
        extractor_state = str(inputs.get("state"))
        if extractor_state not in {"disabled", "enabled"}:
            return _Outcome(status="FAIL", message=f"invalid extractor state: {extractor_state}")
        current["extractor_state"] = extractor_state
        current["extractor_version"] = str(inputs.get("extractor_version", ""))
        return _Outcome(
            observations={
                "extractor": {
                    "state": extractor_state,
                    "version": current["extractor_version"],
                }
            }
        )
    if action == "dependency.configure":
        if inputs.get("dependency") != "graph":
            return _Outcome(
                status="BLOCKED",
                message=f"unsupported dependency control target: {inputs.get('dependency')}",
            )
        dependency_state = str(inputs.get("state"))
        controlled = await _configure_graph_dependency(dependency_state)
        if dependency_state == "unavailable":
            current["graph_outage_started_at"] = time.time()
        else:
            current["graph_recovery_started_at"] = time.time()
        return _Outcome(
            observations={"graph": {"state": dependency_state, **controlled}},
            boundaries=("graph",),
        )
    if action == "artifact.reextract":
        await _ensure_scope(request, current)
        version_id = str(inputs["artifact_version_ref"])
        started = time.perf_counter()
        async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
            await uow.use_tenant(str(current["group_id"]))
            result = await _curation_service(container, uow).reextract_artifact(
                UUID(version_id), group_id=str(current["group_id"])
            )
            if not isinstance(result, Ok):
                raise AdapterFailed(f"re-extraction failed: {result.error}")
            await uow.commit()
        queue = await _wait_for_group_jobs(
            container,
            str(current["group_id"]),
            timeout_s=_timeout("VERA_EVAL_INGEST_TIMEOUT_S"),
        )
        snapshot = await _database_snapshot(container, str(current["group_id"]))
        claims = [
            {
                "id": claim["id"],
                "subject": claim["subject"],
                "predicate": claim["predicate"],
                "object": claim["object"],
            }
            for claim in snapshot["claims_state"]
            if claim["artifact_version_id"] == version_id
        ]
        return _Outcome(
            observations={
                "reextract": {
                    "available": True,
                    "claims": claims,
                    "claim_ids": list(result.value.claim_ids),
                    "queue_state": queue,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                }
            }
        )
    if action == "source.tombstone":
        await _ensure_scope(request, current)
        current["deletion_started_at"] = time.time()
        record = {
            "external_id": str(inputs["external_id"]),
            "body": "",
            "knowledge_type": "text",
            "metadata": {},
            "reference_time": _parse_time(inputs.get("source_event_time")),
            "source_revision": None,
            "source_updated_at": None,
            "source_version_id": None,
            "trust_tier": 1,
            "title": None,
        }
        result, _source, queue = await _ingest(container, request, current, record, tombstone=True)
        lifecycle = {
            "accepted": result.action == "tombstone",
            "artifact_version_id": result.artifact_version_id,
            "queue_state": queue,
        }
        return _Outcome(observations={"tombstone": lifecycle, "removal": lifecycle})
    if action == "source.retract":
        await _ensure_scope(request, current)
        principal = _principal(current, "default")
        source_ref = str(inputs["source_ref"])
        erase = bool(inputs.get("erase"))
        started = time.perf_counter()
        await _api_json(
            "DELETE",
            f"/memory/sources/{source_ref}?erase={'true' if erase else 'false'}",
            api_key=str(principal["api_key"]),
            expected={204},
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        current["retraction_started_at"] = time.time() - (elapsed_ms / 1000)
        snapshot = await _database_snapshot(container, str(current["group_id"]))
        episode = next(
            (item for item in snapshot["episodes_state"] if item.get("source_id") == source_ref),
            None,
        )
        audit = next(
            (
                item
                for item in reversed(snapshot["audits_state"])
                if item.get("target") == source_ref
                and item.get("action") == ("erase" if erase else "retract")
            ),
            None,
        )
        lineage_complete = audit is not None and (
            episode is None
            if erase
            else episode is not None and episode.get("retracted_at") is not None
        )
        return _Outcome(
            observations={
                "retraction": {
                    "accepted": True,
                    "source_id": source_ref,
                    "erased": erase,
                    "latency_ms": elapsed_ms,
                },
                "audit": {
                    "lineage_complete": lineage_complete,
                    "event_id": audit.get("id") if audit else None,
                    "action": audit.get("action") if audit else None,
                    "target": audit.get("target") if audit else None,
                },
            },
            boundaries=("api", "database", "graph"),
        )
    if action == "source.sync":
        await _ensure_scope(request, current)
        connector = inputs.get("fixture") if isinstance(inputs.get("fixture"), dict) else {}
        raw_pages = connector.get("pages", [])
        pages = tuple(
            tuple(str(item) for item in page) for page in raw_pages if isinstance(page, list)
        )
        configured_failure = connector.get("fail_once_on_page")
        fail_on_page = (
            int(configured_failure)
            if isinstance(configured_failure, int) and not inputs.get("failure_cleared")
            else None
        )
        source_id = UUID(await _ensure_source(container, request, current))
        settings = container.settings
        embedding_model, embedding_dimension = active_embedding(settings)
        runner = SyncRunner(
            uow_factory=lambda: SqlAlchemyUnitOfWork(container.sessionmaker),
            extractor=container.extractor,
            state=container.sync_state,
            object_store=container.object_store,
            judge=container.judge,
            embedder=(container.embedder if settings.memory.vector_search_enabled else None),
            embedding_provider=settings.memory.embedder,
            embedding_model=embedding_model,
            embedding_model_version=settings.memory.embedding_model_version,
            embedding_dimension=embedding_dimension,
        )
        failed = False
        outcome = None
        try:
            outcome = await runner.sync(
                source_id=source_id,
                group_id=str(current["group_id"]),
                connector=_FixtureConnector(pages, fail_on_page=fail_on_page),
            )
            cursor = outcome.cursor
        except _FixtureSyncFailure:
            failed = True
            cursor = await container.sync_state.get_cursor(source_id) or {"page": 0}
        if failed:
            current["sync_failure_started_at"] = time.time()
        elif isinstance(current.get("sync_failure_started_at"), (int, float)):
            current["sync_recovery_ms"] = round(
                (time.time() - float(current["sync_failure_started_at"])) * 1000,
                3,
            )
        queue = await _wait_for_group_jobs(
            container,
            str(current["group_id"]),
            timeout_s=_timeout("VERA_EVAL_INGEST_TIMEOUT_S"),
        )
        cursor_label = f"after-page-{int(cursor.get('page', 0))}"
        values: dict[str, Any] = {
            "sync.failed": failed,
            "sync.recovered": not failed,
            "cursor.after_failure": cursor_label,
            "cursor.final": cursor_label,
        }
        full: dict[str, Any] = {}
        for path in request["observe"]:
            if path in values:
                _set_path(full, path, values[path])
        full.setdefault("sync", {})["queue_state"] = queue
        if outcome is not None:
            full["sync"].update(
                processed=outcome.processed, unchanged=outcome.unchanged, cursor=outcome.cursor
            )
        return _Outcome(
            observations=full,
            created=[f"group:{current['group_id']}", f"source:{source_id}"],
            message="fixture connector failure was durably checkpointed" if failed else "",
        )
    if action == "projection.rebuild":
        await _ensure_scope(request, current)
        projection = container.fact_projection
        if projection is None:
            return _Outcome(
                status="BLOCKED", message="active runtime has no Graphiti fact projection"
            )
        service = FactProjectionService(
            source=SqlAlchemyProjectionSource(container.reads), projection=projection
        )
        started = time.perf_counter()
        count = await service.rebuild_group(str(current["group_id"]))
        drift = await service.verify_group(str(current["group_id"]))
        duration_ms = round((time.perf_counter() - started) * 1000, 3)
        return _Outcome(
            observations={
                "rebuild": {
                    "duration_ms": duration_ms,
                    "projected_fact_count": count,
                    "missing_in_graph": sorted(drift.missing_in_graph),
                    "extra_in_graph": sorted(drift.extra_in_graph),
                    "verified": not drift.missing_in_graph and not drift.extra_in_graph,
                }
            },
            boundaries=("database", "graph"),
        )
    if action == "search.transaction_as_of":
        await _ensure_scope(request, current)
        fixture = inputs.get("fixture") if isinstance(inputs.get("fixture"), dict) else {}
        principal = _principal(current, "default")
        status, result = await _search_http(
            api_key=str(principal["api_key"]),
            query=str(inputs.get("query", "")),
            limit=10,
            project=str(principal["group_id"]),
            as_of=_parse_time(fixture.get("valid_as_of")),
            known_as_of=_parse_time(fixture.get("known_as_of")),
        )
        return _Outcome(
            status="PASS" if 200 <= status < 300 else "FAIL",
            observations={
                "transaction_query": {
                    "supported": 200 <= status < 300,
                    "results": result["results"],
                    "status": status,
                }
            },
            boundaries=("api",),
        )
    if action == "claim.review":
        await _ensure_scope(request, current)
        claim_id = UUID(str(inputs["claim_ref"]))
        principal = _principal(current, "default")
        approve = bool(inputs.get("approve"))
        started = time.perf_counter()
        async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
            await uow.use_tenant(str(current["group_id"]))
            result = await _curation_service(container, uow).review_claim(
                claim_id=claim_id,
                reviewer_principal_id=UUID(str(principal["principal_id"])),
                approve=approve,
                notes=str(inputs.get("notes")) if inputs.get("notes") is not None else None,
            )
            if not isinstance(result, Ok):
                raise AdapterFailed(f"claim review failed: {result.error}")
            await uow.commit()
        await _wait_for_group_jobs(
            container,
            str(current["group_id"]),
            timeout_s=_timeout("VERA_EVAL_INGEST_TIMEOUT_S"),
        )
        snapshot = await _database_snapshot(container, str(current["group_id"]))
        claim = next(item for item in snapshot["claims_state"] if item["id"] == str(claim_id))
        if approve:
            await _wait_for_search_visibility(
                api_key=str(principal["api_key"]),
                query=" ".join(
                    str(claim.get(key, "")) for key in ("subject", "predicate", "object")
                ),
                project=str(current["group_id"]),
                artifact_version_id=str(claim["artifact_version_id"]),
            )
            current["review_to_searchable_ms"] = round(
                (time.perf_counter() - started) * 1000,
                3,
            )
        return _Outcome(
            observations={
                "approved" if approve else "rejected": {
                    "status": result.value.status if approve else claim["verification_status"],
                    "claim_id": str(claim_id),
                    "review_ids": [
                        item["id"]
                        for item in snapshot["reviews_state"]
                        if item["candidate_claim_id"] == str(claim_id)
                    ],
                }
            }
        )
    if action == "feedback.submit":
        return await _feedback_submit(container, request, current)
    if action == "calibration.evaluate":
        return await _calibration_evaluate(container, request, current)
    if action == "cleanup.run_scope":
        return await _cleanup(container, request, state)
    return _Outcome(
        status="BLOCKED",
        message=f"local adapter action has no truthful product boundary: {action}",
    )


def _facts_with_text(observation: Any, text_value: str) -> list[dict[str, Any]]:
    if not isinstance(observation, dict) or not isinstance(observation.get("facts"), list):
        return []
    needle = text_value.casefold()
    return [
        cast(dict[str, Any], item)
        for item in observation["facts"]
        if isinstance(item, dict) and needle in str(item.get("fact", "")).casefold()
    ]


def _nearest_rank(values: list[float], percentile: float) -> float:
    if not values:
        raise AdapterBlocked("percentile samples are missing")
    if not 0 < percentile <= 1:
        raise ValueError("percentile must be in (0, 1]")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def _nearest_rank_p95(values: list[float]) -> float:
    return _nearest_rank(values, 0.95)


async def _group_token_count(container: Container, group_id: str) -> int:
    async with container.sessionmaker() as session:
        value = await session.scalar(
            text(
                "SELECT coalesce(sum(prompt_tokens + completion_tokens), 0) "
                "FROM llm_usage WHERE group_id=:group_id"
            ),
            {"group_id": group_id},
        )
    return int(value or 0)


async def _groups_token_count(
    container: Container, group_ids: list[str], *, request_kind: str
) -> int:
    if not group_ids:
        return 0
    async with container.sessionmaker() as session:
        value = await session.scalar(
            text(
                "SELECT coalesce(sum(prompt_tokens + completion_tokens), 0) "
                "FROM llm_usage WHERE group_id = ANY(CAST(:group_ids AS text[])) "
                "AND request_kind = :request_kind"
            ),
            {"group_ids": group_ids, "request_kind": request_kind},
        )
    return int(value or 0)


async def _usage_tokens_by_ref(
    container: Container,
    *,
    group_id: str,
    request_kind: str,
    refs: list[str],
) -> dict[str, int]:
    if not refs:
        return {}
    async with container.sessionmaker() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT ref, sum(prompt_tokens + completion_tokens) AS tokens "
                    "FROM llm_usage WHERE group_id = :group_id "
                    "AND request_kind = :request_kind "
                    "AND ref = ANY(CAST(:refs AS text[])) GROUP BY ref"
                ),
                {
                    "group_id": group_id,
                    "request_kind": request_kind,
                    "refs": refs,
                },
            )
        ).mappings()
    return {str(row["ref"]): int(row["tokens"]) for row in rows}


async def _daily_metrics(
    container: Container,
    request: dict[str, Any],
    current: dict[str, Any],
    outcome: _Outcome,
) -> list[dict[str, Any]]:
    case = _CASES.get(str(request["case_id"]))
    if outcome.status != "PASS" or case is None or not case.get("steps"):
        return []
    case_id = str(request["case_id"])
    action = str(request["action"])
    final_step = str(request["step_id"]) == str(case["steps"][-1]["id"])
    operational_step = (case_id, action) in {
        ("ANS-001", "agent.run"),
        ("OUT-001", "agent.run"),
        ("PERF-003", "agent.run"),
        ("RET-001", "search.http"),
        ("RES-001", "projection.wait"),
    }
    if not final_step and not operational_step:
        return []
    declared = set(cast(list[str], case.get("metrics", [])))
    history = current.get("observations", {})
    if not isinstance(history, dict):
        return []
    metrics: list[dict[str, Any]] = []

    def emit(
        name: str,
        value: float | int,
        *,
        sample_size: int,
        unit: str,
        dimensions: dict[str, str] | None = None,
    ) -> None:
        if name in declared:
            metrics.append(
                _metric(
                    name,
                    value,
                    sample_size=sample_size,
                    unit=unit,
                    dimensions=dimensions,
                )
            )

    if case_id == "ING-003":
        reextract = cast(dict[str, Any], history.get("reextract", {}))
        emit(
            "reextraction_success",
            float(reextract.get("available") is True),
            sample_size=1,
            unit="ratio",
        )
        emit(
            "reextraction_duration_ms",
            float(reextract.get("duration_ms", 0.0)),
            sample_size=1,
            unit="ms",
        )
    elif case_id == "PERF-003":
        runs = [
            cast(dict[str, Any], run)
            for run in cast(list[Any], history.get("runs", []))
            if isinstance(run, dict)
        ]
        mcp_latencies = [
            sum(
                float(call.get("latency_ms", 0.0))
                for call in cast(list[Any], run.get("tool_calls", []))
                if isinstance(call, dict)
            )
            for run in runs
        ]
        agent_latencies = [float(run.get("latency_ms", 0.0)) for run in runs]
        if runs:
            emit(
                "mcp_p95_ms",
                _nearest_rank(mcp_latencies, 0.95),
                sample_size=len(runs),
                unit="ms",
            )
            emit(
                "agent_p95_ms",
                _nearest_rank(agent_latencies, 0.95),
                sample_size=len(runs),
                unit="ms",
            )
    elif case_id == "ANS-001":
        runs = [
            cast(dict[str, Any], run)
            for run in cast(list[Any], history.get("runs", []))
            if isinstance(run, dict)
        ]
        emit(
            "agent_latency_ms",
            sum(float(run.get("latency_ms", 0.0)) for run in runs),
            sample_size=len(runs),
            unit="ms",
        )
        emit(
            "agent_token_count",
            sum(
                int(cast(dict[str, Any], run.get("token_usage", {})).get("total_tokens", 0))
                for run in runs
            ),
            sample_size=len(runs),
            unit="tokens",
        )
    elif case_id == "OUT-001":
        agent = cast(dict[str, Any], history.get("agent", {}))
        emit(
            "time_to_task_completion_ms",
            float(agent.get("latency_ms", 0.0)),
            sample_size=1,
            unit="ms",
        )
        emit(
            "accepted_answer_token_count",
            int(cast(dict[str, Any], agent.get("token_usage", {})).get("total_tokens", 0)),
            sample_size=1,
            unit="tokens",
        )
    elif case_id == "RET-001":
        latencies = [float(value) for value in cast(list[Any], history.get("latencies", []))]
        ranked = cast(dict[str, Any], history.get("ranked_results", {}))
        result_count = sum(len(value) for value in ranked.values() if isinstance(value, list))
        emit(
            "latency_ms",
            _nearest_rank_p95(latencies),
            sample_size=len(latencies),
            unit="ms",
        )
        emit(
            "search_result_count",
            result_count,
            sample_size=len(ranked),
            unit="count",
        )
    elif case_id == "E2E-001":
        search = cast(dict[str, Any], history.get("search", {}))
        http = cast(dict[str, Any], search.get("http", {}))
        mcp = cast(dict[str, Any], search.get("mcp", {}))
        snapshot = await _database_snapshot(container, str(current["group_id"]))
        version = next(
            item
            for item in snapshot["versions_state"]
            if item["id"] == str(current["last_artifact_version_id"])
        )
        reference = _parse_time(version.get("reference_time"))
        observed = _parse_time(version.get("observed_at"))
        source_lag = (
            max(0.0, (observed - reference).total_seconds() * 1000)
            if reference is not None and observed is not None
            else 0.0
        )
        emit("source_lag_ms", source_lag, sample_size=1, unit="ms")
        emit(
            "time_to_searchable_ms",
            float(cast(list[float], current.get("visibility_ms", [0.0]))[-1]),
            sample_size=1,
            unit="ms",
        )
        emit("search_http_ms", float(http["latency_ms"]), sample_size=1, unit="ms")
        emit("search_mcp_ms", float(mcp["latency_ms"]), sample_size=1, unit="ms")
        emit(
            "llm_token_count",
            await _group_token_count(container, str(current["group_id"])),
            sample_size=1,
            unit="tokens",
            dimensions={"source": "llm_usage"},
        )
    elif case_id == "ING-001":
        counts = cast(dict[str, Any], history.get("counts", {}))
        before = cast(dict[str, Any], counts.get("before", {}))
        after = cast(dict[str, Any], counts.get("after", {}))
        for name, key in (
            ("duplicate_artifact_versions", "versions"),
            ("duplicate_claims", "claims"),
            ("duplicate_episodes", "episodes"),
            ("duplicate_edges", "facts"),
        ):
            emit(
                name,
                max(0, int(after.get(key, 0)) - int(before.get(key, 0))),
                sample_size=1,
                unit="count",
            )
    elif case_id == "ING-002":
        search = cast(dict[str, Any], history.get("search", {}))
        current_search = cast(dict[str, Any], search.get("current", {}))
        visible = _visible_artifact(current_search, str(current["last_artifact_version_id"]))
        emit("source_change_recall", float(visible), sample_size=1, unit="ratio")
        emit(
            "time_to_searchable_ms",
            float(cast(list[float], current.get("visibility_ms", [0.0]))[-1]),
            sample_size=1,
            unit="ms",
        )
    elif case_id == "ING-004":
        processed = cast(dict[str, Any], history.get("processed", {}))
        external_ids = [str(value) for value in cast(list[Any], processed.get("external_ids", []))]
        fixture = cast(dict[str, Any], case.get("fixture", {})).get("connector", {})
        pages = cast(dict[str, Any], fixture).get("pages", [])
        expected_ids = {
            str(value)
            for page in cast(list[Any], pages)
            if isinstance(page, list)
            for value in page
        }
        unique_ids = set(external_ids)
        emit(
            "record_loss_count",
            len(expected_ids - unique_ids),
            sample_size=len(expected_ids),
            unit="count",
        )
        emit(
            "duplicate_rate",
            (len(external_ids) - len(unique_ids)) / len(external_ids) if external_ids else 0.0,
            sample_size=len(external_ids),
            unit="ratio",
        )
        emit(
            "recovery_time_ms",
            float(current.get("sync_recovery_ms", 0.0)),
            sample_size=1,
            unit="ms",
        )
    elif case_id == "ING-005":
        tombstone = cast(dict[str, Any], history.get("tombstone", {}))
        search = cast(dict[str, Any], history.get("search", {})).get("current")
        hidden = not _facts_with_text(search, "Legacy Tax API")
        emit(
            "deletion_reconciliation_rate",
            float(tombstone.get("accepted") is True and hidden),
            sample_size=1,
            unit="ratio",
        )
        emit(
            "deletion_to_hidden_ms",
            round(
                (time.time() - float(current.get("deletion_started_at", time.time()))) * 1000,
                3,
            ),
            sample_size=1,
            unit="ms",
        )
    elif case_id == "ING-006":
        snapshot = await _database_snapshot(container, str(current["group_id"]))
        hashes = [str(item["content_hash"]) for item in snapshot["versions_state"]]
        duplicates = len(hashes) - len(set(hashes))
        emit("artifact_versions_created", len(hashes), sample_size=len(hashes), unit="count")
        emit(
            "duplicate_rate",
            duplicates / len(hashes) if hashes else 0.0,
            sample_size=len(hashes),
            unit="ratio",
        )
    elif case_id == "CUR-001":
        routing = cast(dict[str, Any], history.get("routing", {}))
        correct = sum(
            (
                int(routing.get("tier1_2_published_count", 0) == 2),
                int(routing.get("tier3_status") == "pending"),
                int(routing.get("tier4_status") == "unverified"),
                int(routing.get("shared_unverified_count", 0) == 0),
            )
        )
        emit("routing_accuracy", correct / 4, sample_size=4, unit="ratio")
        emit(
            "shared_contamination_count",
            int(routing.get("shared_unverified_count", 0)),
            sample_size=4,
            unit="count",
        )
    elif case_id == "CUR-002":
        approved = cast(dict[str, Any], history.get("approved", {}))
        rejected = cast(dict[str, Any], history.get("rejected", {}))
        search = history.get("search")
        correctly_routed = (
            approved.get("status") == "published"
            and rejected.get("status") == "disputed"
            and bool(_facts_with_text(search, "Approved Service"))
            and not _facts_with_text(search, "Rejected Service")
        )
        review_ids = [
            *cast(list[Any], approved.get("review_ids", [])),
            *cast(list[Any], rejected.get("review_ids", [])),
        ]
        emit(
            "review_routing_accuracy",
            float(correctly_routed),
            sample_size=2,
            unit="ratio",
        )
        emit(
            "review_to_searchable_ms",
            float(current.get("review_to_searchable_ms", 0.0)),
            sample_size=1,
            unit="ms",
        )
        emit(
            "audit_coverage",
            min(len(review_ids), 2) / 2,
            sample_size=2,
            unit="ratio",
        )
    elif case_id == "TEMP-001":
        search = cast(dict[str, Any], history.get("search", {}))
        current_search = search.get("current")
        as_of_search = search.get("as_of")
        current_facts = cast(dict[str, Any], current_search or {}).get("facts", [])
        top = current_facts[0] if isinstance(current_facts, list) and current_facts else {}
        emit(
            "current_truth_at_1",
            float("cluster-b" in str(cast(dict[str, Any], top).get("fact", "")).casefold()),
            sample_size=1,
            unit="ratio",
        )
        emit(
            "stale_fact_leakage",
            len(_facts_with_text(current_search, "cluster-a")),
            sample_size=max(1, len(cast(list[Any], current_facts))),
            unit="count",
        )
        emit(
            "as_of_accuracy",
            float(bool(_facts_with_text(as_of_search, "cluster-a"))),
            sample_size=1,
            unit="ratio",
        )
    elif case_id == "TEMP-002":
        search = cast(dict[str, Any], history.get("search", {}))
        current_search = search.get("current")
        as_of_search = search.get("as_of")
        accurate = bool(_facts_with_text(current_search, "cluster-new")) and bool(
            _facts_with_text(as_of_search, "cluster-old")
        )
        emit("out_of_order_accuracy", float(accurate), sample_size=2, unit="ratio")
        emit(
            "stale_overwrite_count",
            len(_facts_with_text(current_search, "cluster-old")),
            sample_size=2,
            unit="count",
        )
    elif case_id == "TEMP-003":
        search = cast(dict[str, Any], history.get("search", {})).get("current")
        authoritative = bool(_facts_with_text(search, "prod-primary"))
        weaker = len(_facts_with_text(search, "prod-secondary"))
        emit(
            "authority_conflict_accuracy",
            float(authoritative and weaker == 0),
            sample_size=1,
            unit="ratio",
        )
        emit(
            "silent_weaker_overwrite_count",
            weaker,
            sample_size=1,
            unit="count",
        )
    elif case_id == "TEMP-004":
        first = cast(dict[str, Any], history.get("first", {}))
        second = cast(dict[str, Any], history.get("second", {}))
        search = cast(dict[str, Any], history.get("search", {})).get("current")
        snapshot = await _database_snapshot(container, str(current["group_id"]))
        active_slots: dict[tuple[str, str], int] = {}
        for fact in snapshot["facts_state"]:
            if fact["lifecycle_state"] != "active":
                continue
            slot = (str(fact["subject_entity_id"]), str(fact["predicate"]).casefold())
            active_slots[slot] = active_slots.get(slot, 0) + 1
        contradictory = sum(max(0, count - 1) for count in active_slots.values())
        accurate = (
            first.get("canonical_id") == second.get("canonical_id")
            and bool(_facts_with_text(search, "cluster-b"))
            and not _facts_with_text(search, "cluster-a")
            and contradictory == 0
        )
        emit("alias_conflict_accuracy", float(accurate), sample_size=1, unit="ratio")
        emit(
            "contradictory_current_count",
            contradictory,
            sample_size=max(1, len(active_slots)),
            unit="count",
        )
    elif case_id == "TEMP-005":
        search = cast(dict[str, Any], history.get("search", {}))
        before = search.get("before")
        after = search.get("after")
        before_correct = bool(_facts_with_text(before, "Inventory API")) and bool(
            _facts_with_text(before, "Tax API")
        )
        removal_correct = bool(_facts_with_text(after, "Inventory API")) and not _facts_with_text(
            after, "Tax API"
        )
        emit("multi_value_accuracy", float(before_correct), sample_size=2, unit="ratio")
        emit("removal_accuracy", float(removal_correct), sample_size=2, unit="ratio")
    elif case_id == "TEMP-006":
        search = cast(dict[str, Any], history.get("search", {}))
        current_search = search.get("current")
        as_of_search = search.get("as_of")
        audit = cast(dict[str, Any], history.get("audit", {}))
        emit(
            "retraction_to_hidden_ms",
            round(
                (time.time() - float(current.get("retraction_started_at", time.time()))) * 1000,
                3,
            ),
            sample_size=1,
            unit="ms",
        )
        emit(
            "historical_recall",
            float(bool(_facts_with_text(as_of_search, "Migration Team"))),
            sample_size=1,
            unit="ratio",
        )
        emit(
            "audit_coverage",
            float(audit.get("lineage_complete") is True),
            sample_size=1,
            unit="ratio",
        )
    elif case_id == "TEMP-008":
        transaction = cast(dict[str, Any], history.get("transaction_query", {}))
        accurate = (
            transaction.get("supported") is True
            and "retroactive-cluster" not in str(transaction.get("results", [])).casefold()
        )
        emit("transaction_time_accuracy", float(accurate), sample_size=1, unit="ratio")
    elif case_id == "TEMP-009":
        decision = cast(dict[str, Any], history.get("decision", {}))
        accurate = (
            decision.get("silent_overwrite") is False
            and decision.get("uncertainty_visible") is True
        )
        emit(
            "unknown_time_conflict_accuracy",
            float(accurate),
            sample_size=1,
            unit="ratio",
        )
    elif case_id == "TEMP-010":
        search = cast(dict[str, Any], history.get("search", {})).get("current")
        provenance = cast(dict[str, Any], history.get("provenance", {}))
        accurate = (
            bool(_facts_with_text(search, "prod-cluster"))
            and int(provenance.get("active_source_count", 0)) == 1
        )
        emit("corroboration_accuracy", float(accurate), sample_size=1, unit="ratio")
        emit(
            "provenance_coverage",
            float(int(provenance.get("active_source_count", 0)) == 1),
            sample_size=1,
            unit="ratio",
        )
    elif case_id == "PROJ-001":
        search = cast(dict[str, Any], history.get("search", {})).get("during_lag")
        emit(
            "stale_leakage_during_lag",
            len(_facts_with_text(search, "cluster-old")),
            sample_size=1,
            unit="count",
        )
        emit(
            "lag_window_ms",
            round(
                (time.time() - float(current.get("graph_outage_started_at", time.time()))) * 1000,
                3,
            ),
            sample_size=1,
            unit="ms",
        )
    elif case_id == "RES-001":
        recovery = cast(dict[str, Any], history.get("recovery", {}))
        emit(
            "recovery_time_ms",
            round(
                (time.time() - float(current.get("graph_recovery_started_at", time.time()))) * 1000,
                3,
            ),
            sample_size=1,
            unit="ms",
        )
        emit(
            "retry_count",
            int(recovery.get("retry_count", 0)),
            sample_size=max(1, len(cast(list[Any], recovery.get("expected", [])))),
            unit="count",
        )
        emit(
            "dead_job_count",
            int(recovery.get("dead_jobs", 0)),
            sample_size=max(1, len(cast(list[Any], recovery.get("expected", [])))),
            unit="count",
        )
    elif case_id == "RET-002":
        search = cast(dict[str, Any], history.get("search", {}))
        agent = cast(dict[str, Any], history.get("agent", {}))
        emit(
            "no_answer_precision",
            float(int(search.get("answerable_result_count", 0)) == 0),
            sample_size=1,
            unit="ratio",
        )
        emit(
            "abstention_accuracy",
            float(agent.get("abstained") is True),
            sample_size=1,
            unit="ratio",
        )
    elif case_id == "SEC-001":
        a = cast(dict[str, Any], history.get("a", {}))
        b = cast(dict[str, Any], history.get("b", {}))
        leakage = (
            len(_facts_with_text(a.get("http"), "CANARY-B-ONLY"))
            + len(_facts_with_text(a.get("mcp"), "CANARY-B-ONLY"))
            + len(_facts_with_text(b.get("http"), "CANARY-A-ONLY"))
        )
        emit("cross_tenant_leakage_count", leakage, sample_size=3, unit="count")
    elif case_id == "LEARN-001":
        feedback = cast(dict[str, Any], history.get("feedback", {}))
        joins = cast(dict[str, Any], feedback.get("joins", {}))
        candidate = cast(dict[str, Any], history.get("candidate", {}))
        holdout = cast(dict[str, Any], history.get("holdout", {}))
        emit("feedback_join_rate", float(joins.get("rate", 0.0)), sample_size=1, unit="ratio")
        emit(
            "calibration_sample_count",
            int(candidate.get("sample_count", 0)),
            sample_size=int(candidate.get("sample_count", 0)),
            unit="count",
        )
        emit(
            "holdout_hit_at_5_delta",
            float(holdout.get("hit_at_5_delta", 0.0)),
            sample_size=len(cast(list[Any], holdout.get("events", []))),
            unit="ratio",
        )
        emit(
            "holdout_mrr_delta",
            float(holdout.get("mrr_delta", 0.0)),
            sample_size=len(cast(list[Any], holdout.get("events", []))),
            unit="ratio",
        )
    elif case_id.startswith("REAL-"):
        ingestion = cast(dict[str, Any], history.get("ingestion", {}))
        agent = cast(dict[str, Any], history.get("agent", {}))
        visibility = [float(value) for value in current.get("visibility_ms", [])]
        accepted = int(ingestion.get("accepted_document_count", 0))
        failed = int(ingestion.get("failed_document_count", 0))
        emit("documents_ingested", accepted, sample_size=accepted + failed, unit="count")
        emit(
            "document_ingest_failure_count",
            failed,
            sample_size=accepted + failed,
            unit="count",
        )
        emit(
            "time_to_searchable_p95_ms",
            _nearest_rank_p95(visibility),
            sample_size=len(visibility),
            unit="ms",
        )
        emit(
            "candidate_latency_ms",
            float(agent["latency_ms"]),
            sample_size=1,
            unit="ms",
            dimensions={"model": str(agent.get("model_id", "unknown"))},
        )
        emit(
            "candidate_token_count",
            float(agent.get("token_usage", {}).get("total_tokens", 0)),
            sample_size=1,
            unit="tokens",
            dimensions={"model": str(agent.get("model_id", "unknown"))},
        )
    return metrics


def _evidence(
    request: dict[str, Any], outcome: _Outcome, observations: dict[str, Any]
) -> list[dict[str, Any]]:
    descriptors: list[dict[str, Any]] = []
    serialized = json.dumps(
        observations, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    observations_sha256 = hashlib.sha256(serialized).hexdigest()
    for label in _step_labels(request):
        for boundary in outcome.boundaries:
            descriptors.append(
                {
                    "label": label,
                    "kind": boundary,
                    "action": request["action"],
                    "observation_roots": sorted(observations),
                    "observations_sha256": observations_sha256,
                }
            )
    return descriptors


def _json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdapterBlocked(f"{label} is unreadable") from exc
    if not isinstance(value, dict):
        raise AdapterBlocked(f"{label} must be a JSON object")
    return cast(dict[str, Any], value)


def _bound_json(binding: Any, *, label: str, eval_root: Path) -> tuple[dict[str, Any], str]:
    if not isinstance(binding, dict):
        raise AdapterBlocked(f"{label} binding is invalid")
    ref = binding.get("ref")
    expected_sha256 = binding.get("sha256")
    if not isinstance(ref, str) or not isinstance(expected_sha256, str):
        raise AdapterBlocked(f"{label} binding is incomplete")
    path = Path(ref).resolve()
    try:
        path.relative_to(eval_root)
    except ValueError as exc:
        raise AdapterBlocked(f"{label} escapes the evaluation root") from exc
    try:
        actual_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise AdapterBlocked(f"{label} does not exist") from exc
    if actual_sha256 != expected_sha256:
        raise AdapterBlocked(f"{label} SHA-256 mismatch")
    return _json_object(path, label), actual_sha256


def _inspection_slices(fixture: dict[str, Any], ended_at: datetime) -> dict[str, Any]:
    workflows = fixture.get("workflows")
    if not isinstance(workflows, dict):
        raise AdapterBlocked("drift input fixture lacks workflows")
    source_counts: dict[str, int] = {}
    source_types: Counter[str] = Counter()
    languages: Counter[str] = Counter()
    fact_age = {"0_2_days": 0, "3_7_days": 0, "8_30_days": 0, "over_30_days": 0}
    query_count = 0
    for workflow_id, raw_workflow in workflows.items():
        if not isinstance(workflow_id, str) or not isinstance(raw_workflow, dict):
            raise AdapterBlocked("drift input fixture has an invalid workflow")
        workflow = cast(dict[str, Any], raw_workflow)
        documents = workflow.get("documents")
        task = workflow.get("task")
        language = workflow.get("language")
        if not isinstance(documents, list) or not isinstance(task, dict):
            raise AdapterBlocked(f"drift workflow {workflow_id} lacks documents or task")
        if not isinstance(language, str):
            raise AdapterBlocked(f"drift workflow {workflow_id} lacks language")
        source_counts[workflow_id] = len(documents)
        languages[language] += 1
        query_count += 1
        for raw_document in documents:
            if not isinstance(raw_document, dict):
                raise AdapterBlocked(f"drift workflow {workflow_id} has an invalid document")
            document = cast(dict[str, Any], raw_document)
            source_type = document.get("type")
            created_at = _parse_time(document.get("created_at"))
            if not isinstance(source_type, str) or created_at is None:
                raise AdapterBlocked(f"drift workflow {workflow_id} lacks source metadata")
            source_types[source_type] += 1
            age_days = max(0, int((ended_at - created_at).total_seconds() // 86400))
            if age_days <= 2:
                fact_age["0_2_days"] += 1
            elif age_days <= 7:
                fact_age["3_7_days"] += 1
            elif age_days <= 30:
                fact_age["8_30_days"] += 1
            else:
                fact_age["over_30_days"] += 1
    return {
        "source": {
            "sample_size": sum(source_counts.values()),
            "workflow_counts": source_counts,
            "type_counts": dict(sorted(source_types.items())),
        },
        "query": {"sample_size": query_count},
        "language": {"sample_size": query_count, "counts": dict(sorted(languages.items()))},
        "fact_age": {"sample_size": sum(fact_age.values()), "counts": fact_age},
    }


def _validated_search_metrics(snapshot: dict[str, Any], *, label: str, eval_root: Path) -> str:
    evidence, evidence_sha256 = _bound_json(
        snapshot["metric_evidence"], label=label, eval_root=eval_root
    )
    if (
        evidence.get("schema_version") != "1.0"
        or evidence.get("run_id") != snapshot["run_id"]
        or evidence.get("source_report_sha256") != snapshot["report_sha256"]
    ):
        raise AdapterBlocked(f"{label} provenance mismatch")
    metrics = evidence.get("metrics")
    if not isinstance(metrics, list):
        raise AdapterBlocked(f"{label} lacks source metrics")
    metrics_by_name = {
        item.get("name"): item
        for item in metrics
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    expected = {
        "hit_at_5": ("ratio", snapshot["hit_at_5"]),
        "p95_ms": ("ms", snapshot["p95_ms"]),
    }
    for name, (unit, declared) in expected.items():
        metric = metrics_by_name.get(name)
        if (
            metric is None
            or metric.get("owner_id") != "PERF-001"
            or metric.get("unit") != unit
            or metric.get("status") != "PASS"
            or metric.get("value") != declared["value"]
            or metric.get("sample_size") != declared["sample_size"]
        ):
            raise AdapterBlocked(f"{label} does not match the source metric record")
    return evidence_sha256


def _check_inspection(request: dict[str, Any]) -> dict[str, Any]:
    check = request.get("inputs", {}).get("check")
    if not isinstance(check, dict) or check.get("id") != "LEARN-004":
        raise AdapterBlocked("no independent inspection contract exists for this check")
    run_context = request.get("run_context")
    artifacts = run_context.get("inspection_artifacts") if isinstance(run_context, dict) else None
    artifact_ref = artifacts.get("LEARN-004") if isinstance(artifacts, dict) else None
    if not isinstance(artifact_ref, str):
        raise AdapterBlocked("LEARN-004 requires a versioned drift artifact")
    eval_root = Path(__file__).resolve().parent
    artifact_path = Path(artifact_ref).resolve()
    try:
        artifact_path.relative_to(eval_root)
    except ValueError as exc:
        raise AdapterBlocked("LEARN-004 drift artifact escapes the evaluation root") from exc
    artifact = _json_object(artifact_path, "LEARN-004 drift artifact")
    schema = _json_object(eval_root / "schemas" / "drift-artifact.schema.json", "drift schema")
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    schema_errors = sorted(error.message for error in validator.iter_errors(artifact))
    if schema_errors:
        raise AdapterBlocked(f"LEARN-004 drift artifact schema: {schema_errors[0]}")

    fixture, fixture_sha256 = _bound_json(
        artifact["input_fixture"], label="drift input fixture", eval_root=eval_root
    )
    panel, panel_sha256 = _bound_json(
        artifact["quality_panel"], label="drift quality panel", eval_root=eval_root
    )
    human_label, human_label_sha256 = _bound_json(
        artifact["human_label"], label="drift human label", eval_root=eval_root
    )
    ended_at = _parse_time(artifact["window"]["ended_at"])
    if ended_at is None:
        raise AdapterBlocked("LEARN-004 drift window is invalid")
    slices = _inspection_slices(fixture, ended_at)

    baseline = artifact["search_trend"]["baseline"]
    current = artifact["search_trend"]["current"]
    baseline_metric_sha256 = _validated_search_metrics(
        baseline, label="drift baseline metrics", eval_root=eval_root
    )
    current_metric_sha256 = _validated_search_metrics(
        current, label="drift current metrics", eval_root=eval_root
    )
    baseline_no_hit = 1.0 - float(baseline["hit_at_5"]["value"])
    current_no_hit = 1.0 - float(current["hit_at_5"]["value"])
    no_hit_delta = current_no_hit - baseline_no_hit
    latency_delta = float(current["p95_ms"]["value"]) - float(baseline["p95_ms"]["value"])
    thresholds = artifact["thresholds"]
    feedback = artifact["feedback"]
    panel_passed = (
        panel.get("status") == "PASS"
        and panel.get("quality_status") == "PASS"
        and isinstance(panel.get("judge_count"), int)
        and panel["judge_count"] >= thresholds["minimum_panel_judges"]
        and not panel.get("critical_failures")
    )
    human_label_passed = (
        human_label.get("decision") == "ACCEPT"
        and human_label.get("packet_id") == panel.get("packet_id")
        and human_label.get("panel_result_sha256") == panel_sha256
        and bool(human_label.get("rationale"))
    )
    feedback_addressed = feedback["downvote_sample_size"] > 0 or (
        feedback["disposition"] == "converted_to_labeled_scenario"
        and feedback["replacement_label_count"] >= 1
        and human_label_passed
    )
    no_hit_passed = abs(no_hit_delta) <= thresholds["max_no_hit_delta"]
    latency_passed = (
        float(current["p95_ms"]["value"]) <= thresholds["search_p95_slo_ms"]
        and abs(latency_delta) <= thresholds["max_search_p95_delta_ms"]
    )
    passed = all(
        (
            slices["source"]["sample_size"] > 0,
            slices["query"]["sample_size"] > 0,
            panel_passed,
            human_label_passed,
            feedback_addressed,
            no_hit_passed,
            latency_passed,
        )
    )
    observations = {
        "check": {
            "passed": passed,
            "artifact_id": artifact["artifact_id"],
            "window": artifact["window"],
        },
        "slices": slices,
        "trends": {
            "no_hit": {
                "baseline": baseline_no_hit,
                "current": current_no_hit,
                "delta": no_hit_delta,
                "sample_size": current["hit_at_5"]["sample_size"],
                "passed": no_hit_passed,
            },
            "downvote": {
                "sample_size": feedback["downvote_sample_size"],
                "disposition": feedback["disposition"],
                "replacement_label_count": feedback["replacement_label_count"],
                "passed": feedback_addressed,
            },
            "latency_p95_ms": {
                "baseline": baseline["p95_ms"]["value"],
                "current": current["p95_ms"]["value"],
                "delta": latency_delta,
                "sample_size": current["p95_ms"]["sample_size"],
                "passed": latency_passed,
            },
        },
        "labels": {
            "panel_judges": panel.get("judge_count"),
            "panel_status": panel.get("quality_status"),
            "human_decision": human_label.get("decision"),
            "human_label": human_label.get("label"),
        },
    }
    return {
        "schema_version": "1.0",
        "request_nonce": request["request_nonce"],
        "status": "PASS",
        "observations": observations,
        "message": (
            "drift window is quantified and accepted"
            if passed
            else "material drift or label coverage requires investigation"
        ),
        "evidence": [
            {
                "label": "source/query/language/fact-age slices",
                "kind": "file",
                "artifact_id": artifact["artifact_id"],
                "fixture_sha256": fixture_sha256,
                "sample_size": slices["source"]["sample_size"],
            },
            {
                "label": "no-hit/downvote/latency trends",
                "kind": "metric",
                "artifact_id": artifact["artifact_id"],
                "baseline_run_id": baseline["run_id"],
                "current_run_id": current["run_id"],
                "baseline_metric_sha256": baseline_metric_sha256,
                "current_metric_sha256": current_metric_sha256,
            },
            {
                "label": "weekly human labels",
                "kind": "human_label",
                "panel_sha256": panel_sha256,
                "human_label_sha256": human_label_sha256,
                "decision": human_label.get("decision"),
            },
        ],
    }


async def _run(request: dict[str, Any]) -> dict[str, Any]:
    run_id = str(request["run_id"])
    state = _load_state(run_id)
    action = str(request["action"])
    if action == "__check__":
        try:
            return _check_inspection(request)
        except AdapterBlocked as exc:
            return {
                "schema_version": "1.0",
                "request_nonce": request["request_nonce"],
                "status": "BLOCKED",
                "message": str(exc),
            }
    settings = get_settings()
    container = build_container(settings)
    try:
        if action == "safety.preflight":
            try:
                return await _preflight(container, request, state)
            except AdapterBlocked as exc:
                return {
                    "schema_version": "1.0",
                    "request_nonce": request["request_nonce"],
                    "status": "BLOCKED",
                    "observations": {
                        "safety": {
                            "scope_run_owned": False,
                            "production_writable": False,
                            "cost_bounded": False,
                            "cleanup_supported": False,
                        }
                    },
                    "message": str(exc),
                }
            except AdapterFailed as exc:
                return {
                    "schema_version": "1.0",
                    "request_nonce": request["request_nonce"],
                    "status": "FAIL",
                    "observations": {
                        "safety": {
                            "scope_run_owned": False,
                            "production_writable": False,
                            "cost_bounded": False,
                            "cleanup_supported": False,
                        }
                    },
                    "message": str(exc),
                }
        current = _case_state(request, state)
        try:
            outcome = await _handle_action(container, request, state, current)
        except AdapterBlocked as exc:
            outcome = _Outcome(status="BLOCKED", message=str(exc))
        except AdapterFailed as exc:
            outcome = _Outcome(status="FAIL", message=str(exc))
        history = cast(dict[str, Any], current.setdefault("observations", {}))
        _deep_merge(history, outcome.observations)
        outcome.metrics.extend(await _daily_metrics(container, request, current, outcome))
        state["resources"] = sorted({*state.get("resources", []), *outcome.created})
        _save_state(run_id, state)
        observations = (
            copy.deepcopy(outcome.observations)
            if action == "cleanup.run_scope"
            else _declared_observations(outcome.observations, request["observe"])
        )
        if outcome.status == "PASS":
            missing = sorted(
                {
                    re.split(r"[.[]", path, maxsplit=1)[0]
                    for path in request["observe"]
                    if re.split(r"[.[]", path, maxsplit=1)[0] not in observations
                }
            )
            if missing:
                outcome.status = "BLOCKED"
                outcome.message = "truthful adapter could not observe: " + ", ".join(missing)
        response = {
            "schema_version": "1.0",
            "request_nonce": request["request_nonce"],
            "status": outcome.status,
            "observations": observations,
            "message": outcome.message,
            "metrics": outcome.metrics,
            "evidence": _evidence(request, outcome, observations),
            "created_resources": outcome.created,
            "removed_resources": outcome.removed,
        }
        if action == "cleanup.run_scope" and outcome.status == "PASS":
            path = _state_path(run_id)
            if path.exists():
                path.unlink()
        return response
    finally:
        await dispose_container(container)


def main() -> None:
    request: Any = None
    try:
        request = json.loads(sys.stdin.buffer.read())
        if not isinstance(request, dict):
            raise ValueError("request must be an object")
        with redirect_stdout(sys.stderr):
            response = asyncio.run(_run(cast(dict[str, Any], request)))
    except Exception as exc:
        nonce = request.get("request_nonce") if isinstance(request, dict) else None
        response = {
            "schema_version": "1.0",
            "request_nonce": nonce,
            "status": "BLOCKED",
            "message": f"local adapter failed closed: {_exception_type_summary(exc)}",
        }
    sys.stdout.write(json.dumps(response, ensure_ascii=True, separators=(",", ":"), default=str))


if __name__ == "__main__":
    main()
