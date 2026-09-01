from __future__ import annotations

import asyncio
import hashlib
import json
from collections import Counter
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from evals import vera_local_adapter as adapter
from evals.adapters import ActionResponse
from evals.generate_load_fixture import records
from evals.validate import ROOT, load_jsonl
from vera.config.settings import Settings
from vera.entrypoints.rollout_control import configuration_sha256, normalize_control_environment


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "db": {"dsn": "postgresql+asyncpg://vera:vera@localhost/vera"},
            "memory": {
                "provider": "graphiti",
                "graph_backend": "neo4j",
                "openai_api_key": "model-secret",
                "openai_base_url": "https://model.test/v1",
                "llm_model": "candidate-model",
                "small_llm_model": "extractor-model",
            },
            "mcp": {"jwt_secret": "mcp-secret-for-tests-at-least-32-bytes"},
            "rerank": {
                "cross_encoder_enabled": True,
                "cross_encoder_provider": "voyage",
            },
        }
    )


def test_http_client_reuses_verified_ssl_context(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    sentinel = object()

    def factory(**kwargs: Any) -> object:
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(adapter.httpx, "AsyncClient", factory)

    assert adapter._http_client() is sentinel
    assert captured["verify"] is adapter._HTTP_SSL_CONTEXT


def test_plain_http_url_resolves_once_and_preserves_host(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def resolve(host: str) -> str:
        nonlocal calls
        calls += 1
        assert host == "api"
        return "172.18.0.7"

    adapter._resolved_http_url.cache_clear()
    monkeypatch.setattr(adapter.socket, "gethostbyname", resolve)

    first = adapter._resolved_http_url("http://api:8000/v2/knowledge/search")
    second = adapter._resolved_http_url("http://api:8000/v2/knowledge/search")
    secure = adapter._resolved_http_url("https://api:8443/v2/knowledge/search")

    assert first == ("http://172.18.0.7:8000/v2/knowledge/search", "api:8000")
    assert second == first
    assert secure == ("https://api:8443/v2/knowledge/search", None)
    assert calls == 1
    adapter._resolved_http_url.cache_clear()


def test_rollout_attestation_rejects_tampered_evidence() -> None:
    transition = "role_enforcement_off_to_on"
    definition = adapter.TRANSITIONS[transition]
    environments = {
        service: normalize_control_environment({definition.environment_key: definition.rollout})
        for service in definition.services
    }
    evidence: dict[str, Any] = {
        "transition": transition,
        "direction": "rollout",
        "group_id": "p:canary",
        "before_revision": 1,
        "after_revision": 2,
        "process_ids": {
            "before": {service: f"old-{service}" for service in definition.services},
            "after": {service: f"new-{service}" for service in definition.services},
        },
        "service_environments": environments,
        "effective_state": {definition.environment_key: definition.rollout},
        "configuration_sha256": configuration_sha256(environments),
        "configuration_applied": True,
        "process_restarted": True,
        "state_changed": True,
        "invariants_preserved": True,
        "baseline_restored": False,
    }

    assert adapter._rollout_attestation_verified(
        evidence, transition=transition, direction="rollout", group_id="p:canary"
    )
    evidence["configuration_sha256"] = "0" * 64
    assert not adapter._rollout_attestation_verified(
        evidence, transition=transition, direction="rollout", group_id="p:canary"
    )


def test_rollout_preparation_is_fully_attested() -> None:
    transition = "community_build_off_to_on"
    definition = adapter.TRANSITIONS[transition]
    environments = {
        service: normalize_control_environment({definition.environment_key: definition.baseline})
        for service in definition.services
    }
    evidence: dict[str, Any] = {
        "operation": "prepare",
        "transition": transition,
        "group_id": "p:canary",
        "before_revision": 1,
        "after_revision": 2,
        "process_ids": {
            "before": {service: f"old-{service}" for service in definition.services},
            "after": {service: f"new-{service}" for service in definition.services},
        },
        "service_environments": environments,
        "effective_state": {definition.environment_key: definition.baseline},
        "configuration_sha256": configuration_sha256(environments),
        "configuration_applied": True,
        "process_restarted": True,
        "state_changed": False,
        "invariants_preserved": True,
    }

    assert adapter._rollout_preparation_verified(
        evidence, transition=transition, group_id="p:canary"
    )
    evidence["process_restarted"] = False
    assert not adapter._rollout_preparation_verified(
        evidence, transition=transition, group_id="p:canary"
    )


def test_authoritative_preservation_allows_additions_and_rejects_mutation() -> None:
    before = {
        "rows": {"facts": [{"id": "fact-1", "authority": 1.0}]},
        "objects": {"artifact-1": {"sha256": "abc", "size": 3}},
    }
    extended = {
        "rows": {
            "facts": [
                {"id": "fact-1", "authority": 1.0},
                {"id": "fact-2", "authority": 1.0},
            ]
        },
        "objects": {
            "artifact-1": {"sha256": "abc", "size": 3},
            "artifact-2": {"sha256": "def", "size": 3},
        },
    }
    mutated = {
        "rows": {"facts": [{"id": "fact-1", "authority": 0.1}]},
        "objects": {"artifact-1": {"sha256": "changed", "size": 3}},
    }

    assert adapter._authoritative_preservation(before, extended)["preserved"] is True
    report = adapter._authoritative_preservation(before, mutated)
    assert report["preserved"] is False
    assert report["missing_or_mutated_row_count"] == 1
    assert report["missing_or_mutated_object_count"] == 1


@pytest.mark.parametrize(
    ("mode", "fact", "jobs", "edges"),
    [
        ("legacy", None, [], [{"published_episode_id": "episode-1"}]),
        (
            "dual",
            {"id": "fact-1"},
            [
                {
                    "artifact_version_id": "version-1",
                    "job_kind": "ingest_graph",
                    "status": "done",
                }
            ],
            [{"published_episode_id": "episode-1"}],
        ),
        ("fabric", {"id": "fact-1"}, [], []),
    ],
)
def test_rollout_mode_evidence_requires_real_write_path(
    mode: str,
    fact: dict[str, str] | None,
    jobs: list[dict[str, str]],
    edges: list[dict[str, str]],
) -> None:
    snapshot = {"jobs_state": jobs, "graph_edges_state": edges}
    probe = {
        "artifact_version_id": "version-1",
        "episode_id": "episode-1",
        "fact": fact,
    }

    assert adapter._rollout_mode_evidence(snapshot, probe, mode)["verified"] is True


def test_curation_service_uses_active_voyage_embedding_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    settings = settings.model_copy(
        update={
            "memory": settings.memory.model_copy(update={"embedder": "voyage"}),
            "voyage": settings.voyage.model_copy(
                update={"embedding_model": "voyage-4-lite", "embedding_dim": 1024}
            ),
        }
    )
    captured: dict[str, Any] = {}

    def factory(*_args: Any, **kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(adapter, "CurationService", factory)
    container = SimpleNamespace(
        settings=settings,
        extractor=object(),
        object_store=object(),
        judge=object(),
        embedder=object(),
    )

    adapter._curation_service(container, object())  # type: ignore[arg-type]

    assert captured["embedding_provider"] == "voyage"
    assert captured["embedding_model"] == "voyage-4-lite"
    assert captured["embedding_dimension"] == 1024


def test_extractor_control_disables_and_restores_derivation() -> None:
    current: dict[str, Any] = {}
    disabled = asyncio.run(
        adapter._handle_action(
            object(),
            {
                "action": "extractor.configure",
                "inputs": {"state": "disabled", "extractor_version": "none"},
            },
            {},
            current,
        )
    )
    enabled = asyncio.run(
        adapter._handle_action(
            object(),
            {
                "action": "extractor.configure",
                "inputs": {"state": "enabled", "extractor_version": "recovered-v1"},
            },
            {},
            current,
        )
    )

    assert disabled.status == "PASS"
    assert disabled.observations["extractor"] == {"state": "disabled", "version": "none"}
    assert enabled.status == "PASS"
    assert current == {"extractor_state": "enabled", "extractor_version": "recovered-v1"}


def test_production_actions_dispatch_to_truthful_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actions = {
        "load.soak": "_production_load_soak",
        "security.database_roles": "_production_database_roles",
        "security.mcp_authorization": "_production_mcp_authorization",
        "security.content_attacks": "_production_content_attacks",
        "governance.retention_drill": "_production_retention_drill",
        "recovery.backup_restore": "_production_backup_restore",
        "observability.exercise": "_production_observability",
        "incident.recovery_drill": "_production_incident_recovery",
        "rollout.exercise": "_production_rollout",
        "benchmark.production": "_production_benchmark",
    }

    for action, helper_name in actions.items():
        called: list[str] = []

        async def boundary(
            *_args: Any,
            helper: str = helper_name,
            calls: list[str] = called,
        ) -> adapter._Outcome:
            calls.append(helper)
            return adapter._Outcome(observations={"boundary": helper})

        monkeypatch.setattr(adapter, helper_name, boundary)
        outcome = asyncio.run(
            adapter._handle_action(
                object(),
                {"action": action, "inputs": {}},
                {},
                {},
            )
        )

        assert called == [helper_name]
        assert outcome.observations == {"boundary": helper_name}


def test_production_benchmark_uses_uniquely_answerable_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    aliases = [f"scope-{index:02d}" for index in range(20)]
    perf_state = {
        "load_fixture": {
            "scope_count": 20,
            "facts_per_scope": 200,
            "query_count": 200,
            "seed": 20260828,
            "aliases": aliases,
        },
        "observations": {},
        "principals": {
            alias: {"api_key": f"key-{alias}", "group_id": f"group-{alias}"} for alias in aliases
        },
    }
    queries: list[str] = []

    async def search_http(**kwargs: Any) -> tuple[int, dict[str, Any]]:
        query = str(kwargs["query"])
        queries.append(query)
        return 200, {
            "facts": [
                {
                    "fact": query,
                    "citation": {"structured_record": {"subject": query}},
                }
            ],
            "latency_ms": 10.0,
        }

    monkeypatch.setattr(adapter, "_search_http", search_http)
    outcome = asyncio.run(
        adapter._production_benchmark(
            SimpleNamespace(),  # type: ignore[arg-type]
            {
                "inputs": {
                    "matrix_ref": {
                        "corpus_size": 4000,
                        "query_count": 200,
                        "scope_distribution": [1, 5, 20],
                    },
                    "targets_ref": {
                        "search_p95_ms": 800,
                        "search_p99_ms": 3000,
                        "search_error_rate": 0.01,
                    },
                }
            },
            {"cases": {"PERF-001": perf_state}},
        )
    )

    assert len(queries) == 200
    expected_queries = []
    for index in range(200):
        selected_scope_count = [1, 5, 20][index % 3]
        scope_index = (index // 3) % selected_scope_count
        triple = adapter.load_fact(scope_index, (index * 17) % 200, 20260828, 200)["triple"]
        expected_queries.append(
            " ".join(str(triple[key]) for key in ("subject", "predicate", "object"))
        )
    assert Counter(queries) == Counter(expected_queries)
    assert outcome.observations["quality"]["critical_miss_count"] == 0
    assert outcome.observations["decision"]["value"] == "GO"


def test_disabled_extractor_derives_no_claims() -> None:
    extracted = asyncio.run(
        adapter._DisabledExtractor().extract(
            body="raw artifact", knowledge_type="text", metadata={}
        )
    )

    assert extracted == []


def test_exception_summary_preserves_nested_leaf_types_without_messages() -> None:
    nested = ExceptionGroup(
        "sensitive outer message",
        [TimeoutError("sensitive timeout"), ExceptionGroup("nested", [ValueError("secret")])],
    )

    assert adapter._exception_type_summary(nested) == "ExceptionGroup[TimeoutError,ValueError]"


def test_adapter_source_has_no_repository_search_or_deterministic_extractor() -> None:
    source = Path(adapter.__file__).read_text(encoding="utf-8")

    assert "_DeterministicExtractor" not in source
    assert "SqlAlchemyFactCandidateSource" not in source
    assert "run_until_empty" not in source
    assert "_build_pool" not in source
    assert "vera-local-deterministic" not in source
    assert '"behavior_bounded": True' not in source
    assert '"unsupported_claim_count": 0' not in source
    assert '"sample_size": 1000' not in source


def test_load_generator_keeps_dependencies_inside_the_declared_corpus() -> None:
    generated = list(records(scopes=1, facts_per_scope=7, queries=1, seed=20260828))
    facts = [item for item in generated if item["record_type"] == "fact"]
    services = {f"service-00-{index:04d}" for index in range(7)}

    assert {
        item["triple"]["object"] for item in facts if item["triple"]["predicate"] == "DEPENDS_ON"
    } <= services


def test_outcome_workflow_names_its_target_service() -> None:
    cases = load_jsonl(ROOT / "scenarios" / "core.jsonl")
    outcome = next(case for case in cases if case["case_id"] == "OUT-001")
    task = outcome["fixture"]["task"]

    assert "Payment API" in task


def test_graph_fault_scenarios_restore_the_dependency_during_cleanup() -> None:
    cases = load_jsonl(ROOT / "scenarios" / "core.jsonl")

    for case_id in ("PROJ-001", "RES-001"):
        case = next(case for case in cases if case["case_id"] == case_id)
        cleanup = case["cleanup"]

        assert len(cleanup) == 1
        assert cleanup[0]["id"].startswith("S")
        assert cleanup[0]["action"] == "dependency.configure"
        assert cleanup[0]["input"] == {"dependency": "graph", "state": "available"}
        assert cleanup[0]["observe"] == ["graph.state"]


def test_http_search_uses_public_api_and_only_normalizes_actual_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["authorization"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "query": "owner",
                "results": [
                    {
                        "kind": "fact",
                        "ref": "fact-key-1",
                        "text": "Platform Team OWNS Payment API",
                        "citation": {
                            "evidence_id": "evidence-1",
                            "excerpt": "owns",
                            "structured_record": {
                                "subject": "Platform Team",
                                "predicate": "OWNS",
                                "object": "Payment API",
                            },
                        },
                    }
                ],
                "conflicts": 0,
            },
        )

    transport = httpx.MockTransport(handler)

    def client(**kwargs: Any) -> httpx.AsyncClient:
        timeout = kwargs.pop("timeout_s", None)
        return httpx.AsyncClient(transport=transport, timeout=timeout, **kwargs)

    monkeypatch.setenv("VERA_EVAL_API_URL", "https://vera.test")
    monkeypatch.setattr(adapter, "_http_client", client)

    status, result = asyncio.run(
        adapter._search_http(
            api_key="case-api-key",
            query="owner",
            limit=5,
            project="p:case",
        )
    )

    assert status == 200
    assert seen == {
        "url": "https://vera.test/v2/knowledge/search",
        "authorization": "Bearer case-api-key",
        "body": {"query": "owner", "limit": 5, "project": "p:case"},
    }
    assert result["facts"][0]["id"] == "fact-key-1"
    assert result["facts"][0]["fact"] == "Platform Team OWNS Payment API"
    assert result["results"] == [
        {
            "id": "fact-key-1",
            "kind": "fact",
            "fact": "Platform Team OWNS Payment API",
        }
    ]
    assert "citation" not in result["results"][0]
    assert result["facts"][0]["citation"]["excerpt"] == "owns"
    assert "owns" not in result["results"]
    assert "trace_id" not in result
    assert result["behavior_bounded"] is True
    assert result["bounded_outcome"] == "response"


def test_search_normalization_preserves_every_result_without_provenance_leakage() -> None:
    facts, results = adapter._normalize_product_search(
        {
            "results": [
                {
                    "kind": "fact",
                    "ref": "fact-a",
                    "text": "Service A RUNS_ON cluster-a",
                    "citation": {"excerpt": "historical cluster-z"},
                },
                {
                    "kind": "fact",
                    "ref": "fact-b",
                    "text": "Service B RUNS_ON cluster-b",
                    "citation": {"excerpt": "historical cluster-y"},
                },
            ]
        }
    )

    assert [item["id"] for item in facts] == ["fact-a", "fact-b"]
    assert results == [
        {"id": "fact-a", "kind": "fact", "fact": "Service A RUNS_ON cluster-a"},
        {"id": "fact-b", "kind": "fact", "fact": "Service B RUNS_ON cluster-b"},
    ]


@pytest.mark.parametrize("state", ["unavailable", "available"])
def test_graph_dependency_control_verifies_effective_state(
    monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    toxic_present = state == "available"

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal toxic_present
        if request.method == "DELETE":
            toxic_present = False
            return httpx.Response(204)
        if request.method == "POST":
            toxic_present = True
            return httpx.Response(201, json={"name": "eval-graph-outage"})
        assert request.method == "GET"
        return httpx.Response(
            200,
            json={
                "name": "neo4j",
                "enabled": True,
                "toxics": ([{"name": "eval-graph-outage"}] if toxic_present else []),
            },
        )

    transport = httpx.MockTransport(handler)

    def client(**kwargs: Any) -> httpx.AsyncClient:
        timeout = kwargs.pop("timeout_s", None)
        return httpx.AsyncClient(transport=transport, timeout=timeout, **kwargs)

    monkeypatch.setenv("VERA_EVAL_DEPENDENCY_CONTROL_URL", "http://toxiproxy:8474")
    monkeypatch.setattr(adapter, "_http_client", client)

    result = asyncio.run(adapter._configure_graph_dependency(state))

    assert result["effective_state"] == state
    assert result["changed_at"]


def test_projection_lag_ingest_returns_after_the_durable_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class UnitOfWork:
        async def __aenter__(self) -> UnitOfWork:
            return self

        async def __aexit__(self, *_args: Any) -> None:
            return None

        async def use_tenant(self, group_id: str) -> None:
            assert group_id == "p:case"

        async def commit(self) -> None:
            calls.append("commit")

    class Curation:
        async def ingest_artifact(self, _command: Any) -> Any:
            return adapter.Ok(
                SimpleNamespace(artifact_version_id="version-1", claim_ids=("claim-1",))
            )

    async def ensure_source(*_args: Any, **_kwargs: Any) -> str:
        return "00000000-0000-0000-0000-000000000001"

    async def queue_state(*_args: Any, **_kwargs: Any) -> dict[str, int]:
        calls.append("queue")
        return {"pending": 1}

    async def unexpected_wait(*_args: Any, **_kwargs: Any) -> dict[str, int]:
        raise AssertionError("projection-lag ingest waited for worker reconciliation")

    monkeypatch.setattr(adapter, "_ensure_source", ensure_source)
    monkeypatch.setattr(adapter, "SqlAlchemyUnitOfWork", lambda _sessionmaker: UnitOfWork())
    monkeypatch.setattr(adapter, "_curation_service", lambda *_args, **_kwargs: Curation())
    monkeypatch.setattr(adapter, "_group_queue_state", queue_state)
    monkeypatch.setattr(adapter, "_wait_for_group_jobs", unexpected_wait)
    monkeypatch.setattr(adapter, "_wait_for_search_visibility", unexpected_wait)
    current: dict[str, Any] = {
        "principals": {
            "default": {
                "group_id": "p:case",
                "api_key": "case-key",
            }
        }
    }
    record: dict[str, Any] = {
        "external_id": "record-1",
        "body": "Service A RUNS_ON node-a",
        "knowledge_type": "fact",
        "metadata": {},
    }

    result, source_id, queue = asyncio.run(
        adapter._ingest(
            SimpleNamespace(sessionmaker=object()),  # type: ignore[arg-type]
            {},
            current,
            record,
            allow_projection_lag=True,
        )
    )

    assert result.artifact_version_id == "version-1"
    assert source_id == "00000000-0000-0000-0000-000000000001"
    assert queue == {"pending": 1}
    assert calls == ["commit", "queue"]


def test_graph_failure_observation_outlives_driver_retry_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = 0.0

    class Result:
        def mappings(self) -> list[dict[str, Any]]:
            if clock <= 61.0:
                return []
            return [{"status": "pending", "attempts": 1, "last_error": "graph unavailable"}]

    class Session:
        async def __aenter__(self) -> Session:
            return self

        async def __aexit__(self, *_args: Any) -> None:
            return None

        async def execute(self, *_args: Any, **_kwargs: Any) -> Result:
            return Result()

    async def advance(delay: float) -> None:
        nonlocal clock
        clock += delay

    monkeypatch.setattr(adapter.time, "monotonic", lambda: clock)
    monkeypatch.setattr(adapter.asyncio, "sleep", advance)

    result = asyncio.run(
        adapter._wait_for_graph_failure(
            SimpleNamespace(sessionmaker=lambda: Session()),  # type: ignore[arg-type]
            "p:case",
        )
    )

    assert clock > 61.0
    assert result == {
        "failure_observable": True,
        "retry_count": 0,
        "attempt_count": 1,
        "dead_job_count": 0,
    }


def test_reextraction_emits_declared_metrics() -> None:
    metrics = asyncio.run(
        adapter._daily_metrics(
            SimpleNamespace(),  # type: ignore[arg-type]
            {
                "case_id": "ING-003",
                "action": "artifact.reextract",
                "step_id": "S4",
            },
            {
                "observations": {
                    "reextract": {"available": True, "duration_ms": 12.5},
                }
            },
            adapter._Outcome(),
        )
    )

    assert [(metric["name"], metric["value"], metric["unit"]) for metric in metrics] == [
        ("reextraction_success", 1.0, "ratio"),
        ("reextraction_duration_ms", 12.5, "ms"),
    ]


def test_http_failure_cannot_produce_synthetic_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = httpx.MockTransport(lambda _request: httpx.Response(503, json={"detail": "down"}))

    def client(**kwargs: Any) -> httpx.AsyncClient:
        timeout = kwargs.pop("timeout_s", None)
        return httpx.AsyncClient(transport=transport, timeout=timeout, **kwargs)

    monkeypatch.setenv("VERA_EVAL_API_URL", "https://vera.test")
    monkeypatch.setattr(adapter, "_http_client", client)

    status, result = asyncio.run(
        adapter._search_http(
            api_key="case-api-key",
            query="owner",
            limit=5,
            project="p:case",
        )
    )

    assert status == 503
    assert result["results"] == []
    assert result["facts"] == []
    assert result["answerable_result_count"] == 0


def test_search_equivalence_ignores_volatile_scores_but_keeps_provenance() -> None:
    left = [
        {
            "id": "fact-1",
            "kind": "fact",
            "fact": "Platform Team OWNS Payment API",
            "score": 0.901,
            "signals": {"recency": 0.4},
            "citation": {
                "artifact_version_id": "version-1",
                "assertion_id": "assertion-1",
                "evidence_id": "evidence-1",
                "structured_record": {
                    "subject": "Platform Team",
                    "predicate": "OWNS",
                    "object": "Payment API",
                },
            },
        }
    ]
    right = [{**left[0], "score": 0.899, "signals": {"recency": 0.39}}]

    assert adapter._search_equivalence_key(left) == adapter._search_equivalence_key(right)

    right[0]["citation"] = {**right[0]["citation"], "evidence_id": "evidence-2"}
    assert adapter._search_equivalence_key(left) != adapter._search_equivalence_key(right)


def test_matching_search_equivalence_ignores_unrelated_ranked_results() -> None:
    expected = {
        "subject": "Production Service rollout 1",
        "predicate": "RUNS_ON",
        "object": "production-cluster-rollout-1",
    }
    seeded = {
        "id": "fact-1",
        "kind": "fact",
        "fact": "Production Service rollout 1 RUNS_ON production-cluster-rollout-1",
        "citation": {
            "artifact_version_id": "version-1",
            "assertion_id": "assertion-1",
            "evidence_id": "evidence-1",
        },
    }
    baseline = adapter._matching_search_equivalence_keys([seeded], expected)
    after_probe = [
        {
            "id": "fact-2",
            "kind": "fact",
            "fact": "OPS-009 dual_to_fabric rollout write-path probe",
            "citation": {"artifact_version_id": "version-2"},
        },
        seeded,
    ]

    assert adapter._matching_search_equivalence_keys(after_probe, expected) == baseline

    after_probe[1] = {
        **seeded,
        "citation": {**seeded["citation"], "evidence_id": "evidence-changed"},
    }
    assert adapter._matching_search_equivalence_keys(after_probe, expected) != baseline


def test_labeled_search_joins_product_fact_keys_to_fixture_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def search(**_kwargs: Any) -> tuple[int, dict[str, Any]]:
        return 200, {
            "facts": [
                {
                    "id": "product-fact-key",
                    "signals": {"relevance": 1.0},
                    "citation": {
                        "structured_record": {
                            "subject": "Platform Team",
                            "predicate": "OWNS",
                            "object": "Payment API",
                        }
                    },
                }
            ],
            "latency_ms": 12.5,
        }

    monkeypatch.setattr(adapter, "_search_http", search)
    current: dict[str, Any] = {
        "principals": {"default": {"api_key": "case-key", "group_id": "p:case"}},
        "fixture_facts": [
            {
                "fact_id": "f-payment-owner",
                "triple": {
                    "subject": "Platform Team",
                    "predicate": "OWNS",
                    "object": "Payment API",
                },
            }
        ],
        "queries": [],
    }

    outcome = asyncio.run(
        adapter._handle_search(
            SimpleNamespace(settings=None),  # type: ignore[arg-type]
            {
                "action": "search.http",
                "inputs": {
                    "queries_ref": [{"query_id": "q-owner", "text": "Who owns Payment API?"}],
                    "limit": 5,
                },
            },
            current,
        )
    )

    assert outcome.observations["ranked_results"] == {"q-owner": ["f-payment-owner"]}
    assert outcome.observations["retrieval"]["events"][0]["result_ids"] == ["product-fact-key"]


def test_visibility_poll_waits_for_the_ingested_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def search(**_kwargs: Any) -> tuple[int, dict[str, Any]]:
        nonlocal calls
        calls += 1
        facts = (
            []
            if calls == 1
            else [
                {
                    "id": "fact-key-1",
                    "citation": {"artifact_version_id": "artifact-version-1"},
                }
            ]
        )
        return 200, {"facts": facts}

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(adapter, "_search_http", search)
    monkeypatch.setattr(adapter.asyncio, "sleep", no_sleep)

    result = asyncio.run(
        adapter._wait_for_search_visibility(
            api_key="case-api-key",
            query="owner",
            project="p:case",
            artifact_version_id="artifact-version-1",
        )
    )

    assert calls == 2
    assert result["facts"][0]["citation"]["artifact_version_id"] == "artifact-version-1"
    assert result["visibility_poll_s"] >= 0


def test_mcp_search_uses_streamable_http_sdk_and_actual_tool_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    @asynccontextmanager
    async def fake_transport(url: str, *, http_client: Any) -> Any:
        seen["url"] = url
        seen["http_client_type"] = type(http_client).__name__
        seen["host"] = http_client.headers.get("host")
        yield object()

    class FakeClient:
        def __init__(self, transport: Any, **kwargs: Any) -> None:
            self._transport = transport
            self._transport_context: Any = None
            seen["client_options"] = kwargs

        async def __aenter__(self) -> FakeClient:
            self._transport_context = self._transport
            seen["transport"] = await self._transport_context.__aenter__()
            return self

        async def __aexit__(self, *_args: Any) -> None:
            await self._transport_context.__aexit__(*_args)

        async def call_tool(self, name: str, arguments: dict[str, Any], **kwargs: Any) -> Any:
            seen["tool"] = name
            seen["arguments"] = arguments
            seen["call_options"] = kwargs
            return SimpleNamespace(
                is_error=False,
                structured_content={
                    "results": [
                        {
                            "kind": "fact",
                            "ref": "fact-key-1",
                            "text": "Platform Team OWNS Payment API",
                            "citation": {"evidence_id": "evidence-1"},
                        }
                    ]
                },
                content=[],
            )

    monkeypatch.setenv("VERA_EVAL_MCP_URL", "https://mcp.test/mcp")
    monkeypatch.setenv("VERA_EVAL_MCP_JWT_SECRET", "mcp-secret-for-tests-at-least-32-bytes")
    monkeypatch.setenv("VERA_EVAL_MCP_HOST_HEADER", "localhost:8080")
    monkeypatch.setattr(adapter, "streamable_http_client", fake_transport)
    monkeypatch.setattr(adapter, "Client", FakeClient)

    result = asyncio.run(
        adapter._search_mcp(
            _settings(),
            principal_id="00000000-0000-0000-0000-000000000123",
            query="owner",
            limit=5,
            project="p:case",
        )
    )

    assert seen["url"] == "https://mcp.test/mcp"
    assert seen["http_client_type"] == "AsyncClient"
    assert seen["host"] == "localhost:8080"
    assert seen["tool"] == "knowledge_search"
    assert seen["arguments"] == {"query": "owner", "limit": 5, "project": "p:case"}
    assert result["facts"][0]["id"] == "fact-key-1"
    assert "trace_id" not in result


def test_mcp_token_uses_the_frozen_runtime_secret_without_a_duplicate_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VERA_EVAL_MCP_JWT_SECRET", raising=False)

    token = adapter._mcp_token(_settings(), "00000000-0000-0000-0000-000000000123")
    claims = adapter.jwt.decode(
        token,
        "mcp-secret-for-tests-at-least-32-bytes",
        algorithms=["HS256"],
        audience="https://mcp.vera.local",
        issuer="https://auth.vera.local",
    )

    assert claims["sub"] == "00000000-0000-0000-0000-000000000123"


def test_model_call_records_provider_model_latency_and_usage_without_fake_cost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["authorization"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "model": "candidate-model-2026-08-01",
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "answer": "Grounded answer",
                                    "used_result_ids": ["fact-key-1"],
                                    "citations": [{"result_id": "fact-key-1"}],
                                    "abstained": False,
                                }
                            )
                        }
                    }
                ],
                "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17},
            },
        )

    transport = httpx.MockTransport(handler)

    def client(**kwargs: Any) -> httpx.AsyncClient:
        timeout = kwargs.pop("timeout_s", None)
        return httpx.AsyncClient(transport=transport, timeout=timeout, **kwargs)

    monkeypatch.setattr(adapter, "_http_client", client)

    result = asyncio.run(
        adapter._call_model(
            _settings(),
            model="candidate-model",
            messages=[{"role": "user", "content": "question"}],
        )
    )

    assert seen["url"] == "https://model.test/v1/chat/completions"
    assert seen["authorization"] == "Bearer model-secret"
    assert seen["body"]["model"] == "candidate-model"
    assert seen["body"]["stream"] is False
    assert result.model_id == "candidate-model-2026-08-01"
    assert result.latency_ms >= 0
    assert result.usage == {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17}
    assert result.cost_usd is None


def test_model_call_decodes_actual_sse_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [
        {
            "model": "candidate-model-2026-08-01",
            "choices": [{"index": 0, "delta": {"role": "assistant"}}],
        },
        {
            "model": "candidate-model-2026-08-01",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "content": json.dumps(
                            {
                                "answer": "Grounded answer",
                                "used_result_ids": [],
                                "citations": [],
                                "abstained": False,
                            }
                        )
                    },
                }
            ],
        },
        {
            "model": "candidate-model-2026-08-01",
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17},
        },
    ]
    body = "".join(f"data: {json.dumps(event)}\n\n" for event in events) + "data: [DONE]\n\n"
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream; charset=utf-8"},
            text=body,
        )
    )

    def client(**kwargs: Any) -> httpx.AsyncClient:
        timeout = kwargs.pop("timeout_s", None)
        return httpx.AsyncClient(transport=transport, timeout=timeout, **kwargs)

    monkeypatch.setattr(adapter, "_http_client", client)

    result = asyncio.run(
        adapter._call_model(
            _settings(),
            model="candidate-model",
            messages=[{"role": "user", "content": "question"}],
        )
    )

    assert result.payload["answer"] == "Grounded answer"
    assert result.model_id == "candidate-model-2026-08-01"
    assert result.usage == {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17}
    assert result.cost_usd is None


def test_agent_uses_only_mcp_product_ids_and_citations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retrieval_queries: list[str] = []
    answer_system_prompt = ""
    model_calls = 0

    async def fake_mcp(*_args: Any, **_kwargs: Any) -> tuple[dict[str, Any], float]:
        retrieval_queries.append(str(_kwargs["arguments"]["query"]))
        return (
            {
                "pack_id": "pack-1",
                "result_references": ["fact-key-1"],
                "results": [
                    {
                        "ref": "fact-key-1",
                        "text": "Platform Team OWNS Payment API",
                        "citation": {"evidence_id": "evidence-1"},
                    }
                ],
            },
            4.5,
        )

    async def fake_model(*_args: Any, **_kwargs: Any) -> adapter._ModelResult:
        nonlocal answer_system_prompt, model_calls
        model_calls += 1
        if model_calls == 1:
            return adapter._ModelResult(
                payload={"queries": ["Where does Payment API run?"]},
                model_id="actual-model",
                latency_ms=3.0,
                usage={"total_tokens": 2},
                cost_usd=None,
            )
        answer_system_prompt = str(_kwargs["messages"][0]["content"])
        return adapter._ModelResult(
            payload={
                "answer": "Platform Team owns it.",
                "used_result_ids": ["fact-key-1"],
                "citations": [{"result_id": "fact-key-1"}],
                "abstained": False,
            },
            model_id="actual-model",
            latency_ms=7.0,
            usage={"total_tokens": 9},
            cost_usd=None,
        )

    monkeypatch.setattr(adapter, "_call_mcp_tool", fake_mcp)
    monkeypatch.setattr(adapter, "_call_model", fake_model)

    result = asyncio.run(
        adapter._agent_answer(
            _settings(),
            principal={
                "principal_id": "00000000-0000-0000-0000-000000000123",
                "group_id": "p:case",
            },
            question="Who owns Payment API?",
        )
    )

    assert result["used_result_ids"] == ["fact-key-1"]
    assert result["citations"] == [
        {"result_id": "fact-key-1", "citation": {"evidence_id": "evidence-1"}}
    ]
    assert retrieval_queries == ["Who owns Payment API?", "Where does Payment API run?"]
    assert result["model_id"] == "actual-model"
    assert result["latency_ms"] >= 0.0
    assert result["mcp_latency_ms"] == 9.0
    assert result["model_latency_ms"] == 10.0
    assert result["token_usage"] == {"total_tokens": 11}
    assert result["unsupported_claim_count"] == 0
    assert "cost_usd" not in result
    assert "later evidence may qualify or supersede earlier evidence" in answer_system_prompt


def test_agent_uses_task_specific_retrieval_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments_seen: list[dict[str, Any]] = []

    async def fake_answer(
        _settings: Any,
        *,
        principal: dict[str, Any],
        question: str,
        retrieval_limit: int = 10,
        token_budget: int = 4000,
    ) -> dict[str, Any]:
        arguments_seen.append(
            {
                "principal": principal,
                "question": question,
                "retrieval_limit": retrieval_limit,
                "token_budget": token_budget,
            }
        )
        return {"answer": "brief"}

    monkeypatch.setattr(adapter, "_agent_answer", fake_answer)
    container = SimpleNamespace(settings=_settings())
    request = {
        "inputs": {
            "question_ref": {
                "prompt": "Review all documents.",
                "retrieval_limit": 25,
                "token_budget": 8000,
            }
        }
    }
    current = {
        "principals": {
            "default": {
                "principal_id": "00000000-0000-0000-0000-000000000123",
                "group_id": "p:case",
                "api_key": "test-key",
            }
        }
    }

    result = asyncio.run(adapter._agent(container, request, current))  # type: ignore[arg-type]

    assert result == {"agent": {"answer": "brief"}}
    assert arguments_seen == [
        {
            "principal": current["principals"]["default"],
            "question": "Review all documents.",
            "retrieval_limit": 25,
            "token_budget": 8000,
        }
    ]


def test_weekly_drift_inspection_uses_bound_panel_and_human_label() -> None:
    artifact_path = ROOT / "fixtures" / "weekly_drift.json"
    request = {
        "run_id": "weekly-inspection-test",
        "case_id": "LEARN-004",
        "step_id": "check",
        "action": "__check__",
        "inputs": {"check": {"id": "LEARN-004"}},
        "request_nonce": "inspection-nonce",
        "run_context": {
            "inspection_artifacts": {"LEARN-004": str(artifact_path)},
        },
    }

    response = asyncio.run(adapter._run(request))
    ActionResponse.from_dict(response, expected_request_nonce="inspection-nonce")

    assert response["status"] == "PASS"
    assert response["observations"]["check"]["passed"] is True
    assert response["observations"]["slices"]["source"]["sample_size"] == 53
    assert response["observations"]["slices"]["language"]["counts"] == {
        "en": 4,
        "mixed": 1,
    }
    assert response["observations"]["trends"]["downvote"] == {
        "sample_size": 0,
        "disposition": "converted_to_labeled_scenario",
        "replacement_label_count": 1,
        "passed": True,
    }
    assert {item["label"] for item in response["evidence"]} == {
        "source/query/language/fact-age slices",
        "no-hit/downvote/latency trends",
        "weekly human labels",
    }


def test_weekly_drift_inspection_blocks_without_versioned_artifact() -> None:
    response = asyncio.run(
        adapter._run(
            {
                "run_id": "weekly-inspection-test",
                "case_id": "LEARN-004",
                "step_id": "check",
                "action": "__check__",
                "inputs": {"check": {"id": "LEARN-004"}},
                "request_nonce": "inspection-nonce",
                "run_context": {},
            }
        )
    )

    assert response["status"] == "BLOCKED"
    assert response["message"] == "LEARN-004 requires a versioned drift artifact"


def test_weekly_drift_metrics_must_match_bound_source_records(tmp_path: Path) -> None:
    source = ROOT / "fixtures" / "drift" / "20260831-search-metrics.json"
    evidence = json.loads(source.read_text(encoding="utf-8"))
    evidence["metrics"][0]["value"] = 1.0
    evidence_path = tmp_path / "search-metrics.json"
    evidence_path.write_text(
        json.dumps(evidence, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    snapshot = {
        "run_id": evidence["run_id"],
        "report_sha256": evidence["source_report_sha256"],
        "metric_evidence": {
            "ref": str(evidence_path),
            "sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
        },
        "hit_at_5": {"value": 0.9872881355932204, "sample_size": 1416},
        "p95_ms": {"value": 657.171, "sample_size": 200},
    }

    with pytest.raises(adapter.AdapterBlocked, match="source metric record"):
        adapter._validated_search_metrics(
            snapshot, label="drift current metrics", eval_root=tmp_path
        )


def test_agent_passes_explicit_question_time_to_mcp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[dict[str, Any]] = []
    model_calls = 0

    async def fake_mcp(*_args: Any, **kwargs: Any) -> tuple[dict[str, Any], float]:
        seen.append(kwargs["arguments"])
        return {"pack_id": "pack-1", "result_references": [], "results": []}, 1.0

    async def fake_model(*_args: Any, **_kwargs: Any) -> adapter._ModelResult:
        nonlocal model_calls
        model_calls += 1
        if model_calls == 1:
            return adapter._ModelResult(
                payload={"queries": ["Where did it run?"]},
                model_id="actual-model",
                latency_ms=1.0,
                usage={"total_tokens": 1},
                cost_usd=None,
            )
        return adapter._ModelResult(
            payload={
                "answer": "No historical fact was found.",
                "used_result_ids": [],
                "citations": [],
                "abstained": True,
            },
            model_id="actual-model",
            latency_ms=2.0,
            usage={"total_tokens": 3},
            cost_usd=None,
        )

    monkeypatch.setattr(adapter, "_call_mcp_tool", fake_mcp)
    monkeypatch.setattr(adapter, "_call_model", fake_model)

    asyncio.run(
        adapter._agent_answer(
            _settings(),
            principal={
                "principal_id": "00000000-0000-0000-0000-000000000123",
                "group_id": "p:case",
            },
            question="Where did it run at 2026-04-01T12:00:00Z?",
        )
    )

    assert len(seen) == 2
    assert all(call["as_of"] == "2026-04-01T12:00:00Z" for call in seen)


def test_agent_fails_closed_when_mcp_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    model_called = False

    async def unavailable(*_args: Any, **_kwargs: Any) -> tuple[dict[str, Any], float]:
        raise adapter.AdapterBlocked("MCP unavailable")

    async def model(*_args: Any, **_kwargs: Any) -> adapter._ModelResult:
        nonlocal model_called
        model_called = True
        raise AssertionError("model must not run without MCP context")

    monkeypatch.setattr(adapter, "_call_mcp_tool", unavailable)
    monkeypatch.setattr(adapter, "_call_model", model)

    with pytest.raises(adapter.AdapterBlocked, match="MCP unavailable"):
        asyncio.run(
            adapter._agent_answer(
                _settings(),
                principal={
                    "principal_id": "00000000-0000-0000-0000-000000000123",
                    "group_id": "p:case",
                },
                question="Who owns Payment API?",
            )
        )

    assert model_called is False


def test_preflight_requires_exact_configured_ephemeral_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERA_EVAL_SCOPE_ID", "eval-owned")
    request = {
        "request_nonce": "nonce",
        "inputs": {
            "evaluation_scope": {
                "id": "eval-other",
                "kind": "ephemeral_stack",
                "run_owned": True,
                "production_writable": False,
            }
        },
    }

    response = asyncio.run(adapter._preflight(object(), request, {}))  # type: ignore[arg-type]

    assert response["status"] == "FAIL"
    assert response["observations"]["safety"]["scope_run_owned"] is False


def test_preflight_registers_the_principal_used_for_mcp_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    principal_id = "00000000-0000-0000-0000-000000000123"
    saved: list[dict[str, Any]] = []

    async def database_counts(_container: Any) -> dict[str, int]:
        return {}

    async def graph_counts(_settings: Settings) -> dict[str, int]:
        return {"nodes": 0, "edges": 0}

    async def zero_count(_settings: Settings) -> int:
        return 0

    async def api_readiness() -> dict[str, str]:
        return {"status": "ok"}

    async def register_principal() -> str:
        return principal_id

    async def mcp_readiness(_settings: Settings, actual_principal_id: str) -> int:
        assert saved[0]["preflight"]["mcp_principal_id"] == actual_principal_id
        assert actual_principal_id == principal_id
        return 12

    async def provider_preflight(_container: Any) -> dict[str, str]:
        return {"candidate-model": "candidate-model"}

    monkeypatch.setenv("VERA_EVAL_SCOPE_ID", "eval-owned")
    monkeypatch.setattr(adapter, "_assert_disposable_endpoints", lambda _settings: None)
    monkeypatch.setattr(adapter, "_runtime_manifest_errors", lambda *_args: ([], {}))
    monkeypatch.setattr(adapter, "_database_counts", database_counts)
    monkeypatch.setattr(adapter, "_graph_counts", graph_counts)
    monkeypatch.setattr(adapter, "_object_count", zero_count)
    monkeypatch.setattr(adapter, "_valkey_count", zero_count)
    monkeypatch.setattr(adapter, "_api_readiness", api_readiness)
    monkeypatch.setattr(adapter, "_register_mcp_readiness_principal", register_principal)
    monkeypatch.setattr(adapter, "_mcp_readiness", mcp_readiness)
    monkeypatch.setattr(adapter, "_provider_preflight", provider_preflight)
    monkeypatch.setattr(adapter, "_save_state", lambda _run_id, state: saved.append(state.copy()))
    state: dict[str, Any] = {}
    request = {
        "run_id": "preflight-run",
        "request_nonce": "nonce",
        "inputs": {
            "evaluation_scope": {
                "id": "eval-owned",
                "kind": "ephemeral_stack",
                "run_owned": True,
                "production_writable": False,
            }
        },
        "run_context": {},
    }

    response = asyncio.run(
        adapter._preflight(SimpleNamespace(settings=_settings()), request, state)  # type: ignore[arg-type]
    )

    assert response["status"] == "PASS"
    assert response["observations"]["safety"]["mcp_tool_count"] == 12
    assert state["preflight"]["mcp_principal_id"] == principal_id


def test_routing_joins_published_episode_durable_source_to_claim() -> None:
    snapshot = {
        "sources_state": [
            {"id": "source-1", "trust_tier": 1},
            {"id": "source-2", "trust_tier": 2},
            {"id": "source-3", "trust_tier": 3},
            {"id": "source-4", "trust_tier": 4},
        ],
        "versions_state": [
            {"id": f"version-{tier}", "source_id": f"source-{tier}"} for tier in range(1, 5)
        ],
        "claims_state": [
            {
                "id": f"claim-{tier}",
                "artifact_version_id": f"version-{tier}",
                "verification_status": status,
            }
            for tier, status in (
                (1, "verified"),
                (2, "verified"),
                (3, "pending"),
                (4, "unverified"),
            )
        ],
        "episodes_state": [
            {"source_id": "p:case:claim-1"},
            {"source_id": "p:case:claim-2"},
        ],
    }

    assert adapter._routing(snapshot) == {
        "tier1_2_published_count": 2,
        "tier3_status": "pending",
        "tier4_status": "unverified",
        "shared_unverified_count": 0,
    }


def test_decision_includes_review_attached_to_the_conflicting_slot() -> None:
    snapshot = {
        "claims_state": [
            {
                "id": "authoritative-claim",
                "artifact_version_id": "version-1",
                "subject": "Payment API",
                "predicate": "RUNS_ON",
                "needs_review": False,
                "verification_status": "verified",
            },
            {
                "id": "weaker-claim",
                "artifact_version_id": "version-2",
                "subject": "Payment API",
                "predicate": "RUNS_ON",
                "needs_review": False,
                "verification_status": "rejected",
            },
        ],
        "reviews_state": [
            {"id": "review-1", "candidate_claim_id": "authoritative-claim"},
        ],
        "facts_state": [
            {
                "subject": "Payment API",
                "predicate": "RUNS_ON",
                "lifecycle_state": "active",
            }
        ],
    }

    decision = adapter._decision_observation(snapshot, "version-2")

    assert decision["reviewable"] is True
    assert decision["uncertainty_visible"] is True
    assert decision["review_ids"] == ["review-1"]


def test_runtime_manifest_covers_every_active_model_and_embedding_setting() -> None:
    settings = _settings()
    settings = settings.model_copy(
        update={
            "memory": settings.memory.model_copy(
                update={"embedder": "openai", "fabric_write_mode": "fabric"}
            )
        }
    )
    container = SimpleNamespace(
        settings=settings,
        extractor=SimpleNamespace(model="extractor-model", provider="openai-compatible"),
        judge=object(),
        entity_judge=object(),
        reranker=object(),
    )
    models = {
        "candidate": "candidate-model",
        "extractor": "extractor-model",
        "contradiction_judge": "extractor-model",
        "entity_judge": "candidate-model",
        "embedder": "openai",
        "embedding": "text-embedding-3-small",
        "embedding_dimension": "1536",
        "reranker": "rerank-2.5",
    }
    request = {
        "run_context": {
            "manifest": {"graph_backend": "graphiti/neo4j", "models": models},
            "quality_config": {"fabric_write_mode": "fabric"},
        }
    }

    errors, actual = adapter._runtime_manifest_errors(container, request)  # type: ignore[arg-type]

    assert errors == []
    assert actual == models

    request["run_context"]["manifest"]["models"]["embedding_dimension"] = "3072"
    errors, _actual = adapter._runtime_manifest_errors(container, request)  # type: ignore[arg-type]
    assert errors == ["manifest model 'embedding_dimension' does not match the active runtime"]


def test_provider_preflight_rejects_non_model_extraction() -> None:
    container = SimpleNamespace(
        settings=_settings(),
        extractor=SimpleNamespace(model="deterministic", provider="structured"),
        judge=None,
        embedder=None,
    )

    with pytest.raises(adapter.AdapterBlocked, match="extraction"):
        asyncio.run(adapter._provider_preflight(container))  # type: ignore[arg-type]


def test_record_ingest_can_skip_current_visibility_for_a_stale_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def ensure_scope(*_args: Any) -> None:
        return None

    async def ingest(*_args: Any, **kwargs: Any) -> tuple[Any, str, dict[str, int]]:
        captured["require_search_visibility"] = kwargs["require_search_visibility"]
        return SimpleNamespace(artifact_version_id="version-1"), "source-1", {"pending": 0}

    async def snapshot(*_args: Any) -> dict[str, Any]:
        return {}

    monkeypatch.setattr(adapter, "_ensure_scope", ensure_scope)
    monkeypatch.setattr(adapter, "_ingest", ingest)
    monkeypatch.setattr(adapter, "_database_snapshot", snapshot)
    monkeypatch.setattr(adapter, "_ingest_observations", lambda *_args: {"late": {}})
    request = {
        "action": "record.ingest",
        "case_id": "TEMP-002",
        "inputs": {
            "fixture": {
                "source_event_time": "2026-03-03T10:00:00Z",
                "triple": {
                    "subject": "Payment API",
                    "predicate": "RUNS_ON",
                    "object": "cluster-old",
                },
            },
            "require_search_visibility": False,
        },
    }

    outcome = asyncio.run(
        adapter._handle_action(
            SimpleNamespace(),  # type: ignore[arg-type]
            request,
            {},
            {"ingest_count": 0, "group_id": "p:case"},
        )
    )

    assert outcome.status == "PASS"
    assert captured["require_search_visibility"] is False


def test_decision_seed_does_not_require_pending_claims_in_current_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    visibility_requirements: list[bool] = []

    async def ensure_scope(*_args: Any) -> None:
        return None

    async def ingest(*_args: Any, **kwargs: Any) -> tuple[Any, str, dict[str, int]]:
        visibility_requirements.append(kwargs["require_search_visibility"])
        claim_id = f"claim-{len(visibility_requirements)}"
        return SimpleNamespace(claim_ids=[claim_id]), "source-1", {"pending": 1}

    async def snapshot(*_args: Any) -> dict[str, Any]:
        return {
            "claims_state": [
                {
                    "id": "claim-1",
                    "subject": "Payments",
                    "predicate": "RUNS_ON",
                    "object": "cluster-a",
                },
                {
                    "id": "claim-2",
                    "subject": "Payments",
                    "predicate": "RUNS_ON",
                    "object": "cluster-b",
                },
            ],
            "facts_state": [],
            "episodes_state": [],
            "graph_edges_state": [],
            "reviews_state": [],
        }

    monkeypatch.setattr(adapter, "_ensure_scope", ensure_scope)
    monkeypatch.setattr(adapter, "_ingest", ingest)
    monkeypatch.setattr(adapter, "_database_snapshot", snapshot)
    request = {
        "case_id": "CUR-002",
        "inputs": {
            "trust_tier": 3,
            "fixture": [
                {
                    "decision": "approve",
                    "triple": {
                        "subject": "Payments",
                        "predicate": "RUNS_ON",
                        "object": "cluster-a",
                    },
                },
                {
                    "decision": "reject",
                    "triple": {
                        "subject": "Payments",
                        "predicate": "RUNS_ON",
                        "object": "cluster-b",
                    },
                },
            ],
        },
    }
    current = {
        "group_id": "p:case",
        "ingest_count": 0,
        "principals": {
            "default": {
                "principal_id": "00000000-0000-0000-0000-000000000123",
                "group_id": "p:case",
            }
        },
    }

    outcome = asyncio.run(
        adapter._seed(SimpleNamespace(), request, current)  # type: ignore[arg-type]
    )

    assert outcome.status == "PASS"
    assert visibility_requirements == [False, False]
    assert outcome.observations["pending_claims"] == ["claim-1", "claim-2"]


def test_ingest_observation_exposes_the_retractable_published_source_id() -> None:
    result = SimpleNamespace(
        artifact_version_id="version-1",
        action="created",
        claim_ids=["claim-1"],
    )
    snapshot = {
        "versions_state": [
            {
                "id": "version-1",
                "content_hash": "hash-1",
                "reference_time": "2026-03-11T10:00:00Z",
                "observed_at": "2026-03-11T10:01:00Z",
            }
        ],
        "episodes_state": [
            {
                "id": "episode-1",
                "artifact_version_id": "version-1",
                "source_id": "p:case:00000000-0000-0000-0000-000000000456",
            }
        ],
        "jobs_state": [],
        "reviews_state": [],
        "facts_state": [],
        "versions": 1,
    }

    observations = adapter._ingest_observations(
        {"observe": ["source.id"]},
        result,  # type: ignore[arg-type]
        "knowledge-source-1",
        "p:case",
        {},
        snapshot,
        {"metadata": {}},
    )

    assert observations["source"]["id"] == ("p:case:00000000-0000-0000-0000-000000000456")


def test_ingest_observation_resolves_alias_canonical_id_through_assertion_lineage() -> None:
    result = SimpleNamespace(
        artifact_version_id="version-2",
        action="created",
        claim_ids=["claim-2"],
    )
    snapshot = {
        "versions_state": [
            {
                "id": "version-2",
                "content_hash": "hash-2",
                "reference_time": "2026-03-08T10:00:00Z",
                "observed_at": "2026-03-08T10:01:00Z",
            }
        ],
        "episodes_state": [],
        "jobs_state": [],
        "reviews_state": [],
        "assertions_state": [
            {
                "artifact_version_id": "version-2",
                "fact_id": "fact-1",
            }
        ],
        "facts_state": [
            {
                "id": "fact-1",
                "subject": "Payment API",
                "subject_entity_id": "entity-1",
            }
        ],
        "versions": 1,
    }

    observations = adapter._ingest_observations(
        {"observe": ["second.canonical_id"]},
        result,  # type: ignore[arg-type]
        "knowledge-source-1",
        "p:case",
        {},
        snapshot,
        {
            "metadata": {
                "triples": [
                    {
                        "subject": "paymentapi",
                        "predicate": "runs_on",
                        "object": "cluster-b",
                    }
                ]
            }
        },
    )

    assert observations["second"]["canonical_id"] == "entity-1"


def test_source_retract_uses_public_api_and_verifies_audit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}
    source_id = "p:case:00000000-0000-0000-0000-000000000456"

    async def ensure_scope(*_args: Any) -> None:
        return None

    async def api_json(method: str, path: str, **kwargs: Any) -> tuple[Any, dict[str, Any]]:
        seen.update(method=method, path=path, api_key=kwargs["api_key"])
        return SimpleNamespace(status_code=204), {}

    async def snapshot(*_args: Any) -> dict[str, Any]:
        return {
            "episodes_state": [{"source_id": source_id, "retracted_at": "2026-08-29T00:00:00Z"}],
            "audits_state": [
                {
                    "id": "audit-1",
                    "action": "retract",
                    "target": source_id,
                }
            ],
        }

    monkeypatch.setattr(adapter, "_ensure_scope", ensure_scope)
    monkeypatch.setattr(adapter, "_api_json", api_json)
    monkeypatch.setattr(adapter, "_database_snapshot", snapshot)
    request = {
        "action": "source.retract",
        "inputs": {"source_ref": source_id, "erase": False},
    }
    current = {
        "group_id": "p:case",
        "principals": {
            "default": {
                "api_key": "case-api-key",
                "principal_id": "00000000-0000-0000-0000-000000000123",
            }
        },
    }

    outcome = asyncio.run(
        adapter._handle_action(
            SimpleNamespace(),  # type: ignore[arg-type]
            request,
            {},
            current,
        )
    )

    assert outcome.status == "PASS"
    assert seen == {
        "method": "DELETE",
        "path": f"/memory/sources/{source_id}?erase=false",
        "api_key": "case-api-key",
    }
    assert outcome.observations["audit"]["lineage_complete"] is True


def test_feedback_joins_blinded_queries_to_every_returned_fixture_fact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner_result = {
        "id": "owner-result",
        "citation": {
            "structured_record": {
                "subject": "Platform Team",
                "predicate": "OWNS",
                "object": "Payment API",
            }
        },
        "signals": {"relevance": 1.0, "authority": 1.0},
    }
    runtime_result = {
        "id": "runtime-result",
        "citation": {
            "structured_record": {
                "subject": "Payment API",
                "predicate": "RUNS_ON",
                "object": "prod-cluster",
            }
        },
        "signals": {"relevance": 0.5, "authority": 1.0},
    }
    current = {
        "queries": [
            {"query_id": "q-owner-en-1", "text": "Who owns Payment API?"},
            {"query_id": "q-negative-salary", "text": "What is Alice's salary?"},
        ],
        "fixture_facts": [
            {
                "fact_id": "f-payment-owner",
                "triple": {
                    "subject": "Platform Team",
                    "predicate": "OWNS",
                    "object": "Payment API",
                },
            },
            {
                "fact_id": "f-payment-runtime",
                "triple": {
                    "subject": "Payment API",
                    "predicate": "RUNS_ON",
                    "object": "prod-cluster",
                },
            },
        ],
        "observations": {
            "retrieval": {
                "events": [
                    {
                        "query_id": "q-owner-en-1",
                        "results": [owner_result, runtime_result],
                    },
                    {"query_id": "q-negative-salary", "results": []},
                ]
            }
        },
        "principals": {
            "default": {
                "api_key": "case-api-key",
                "principal_id": "00000000-0000-0000-0000-000000000123",
            }
        },
    }
    submitted: list[dict[str, Any]] = []

    async def api_json(*_args: Any, **kwargs: Any) -> tuple[int, dict[str, Any]]:
        submitted.append(kwargs["body"])
        return 200, {"status": "recorded"}

    class Result:
        def mappings(self) -> Result:
            return self

        def __iter__(self) -> Any:
            return iter(
                [
                    {
                        "id": "00000000-0000-0000-0000-000000000456",
                        "group_id": "p:case",
                        "result_ref": "owner-result",
                        "signal": "up",
                    }
                ]
            )

    class Session:
        async def __aenter__(self) -> Session:
            return self

        async def __aexit__(self, *_args: Any) -> None:
            return None

        async def execute(self, *_args: Any, **_kwargs: Any) -> Result:
            return Result()

    monkeypatch.setattr(adapter, "_api_json", api_json)
    request = {
        "case_id": "LEARN-001",
        "inputs": {
            "labels_ref": ["q-owner-en-1", "q-negative-salary"],
            "retrieval_events_ref": [],
        },
    }

    outcome = asyncio.run(
        adapter._feedback_submit(
            SimpleNamespace(sessionmaker=Session),  # type: ignore[arg-type]
            request,
            current,
        )
    )

    assert outcome.status == "PASS"
    assert outcome.observations["feedback"]["joins"] == {
        "rate": 1.0,
        "ambiguity_count": 0,
    }
    assert len(submitted) == 10
    assert [body["signal"] for body in submitted] == ["up"] * 5 + ["down"] * 5
    assert submitted[0]["signals"] == {"relevance": 1.0, "authority": 1.0}
    assert submitted[-1]["result_ref"] == "runtime-result"


def test_load_search_validates_matrix_and_uses_only_boundary_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    matrix = {
        **adapter._SEARCH_MATRIX,
        "duration_s": 1e-9,
    }
    aliases = [f"scope-{index:02d}" for index in range(20)]
    current = {
        "load_fixture": {
            "scope_count": 20,
            "facts_per_scope": 200,
            "query_count": 200,
            "seed": 20260828,
            "aliases": aliases,
        },
        "principals": {
            alias: {
                "api_key": f"key-{alias}",
                "principal_id": f"principal-{alias}",
                "group_id": f"group-{alias}",
            }
            for alias in aliases
        },
    }
    calls = {"http": 0, "mcp": 0, "tokens": 0}

    async def search_http(**_kwargs: Any) -> tuple[int, dict[str, Any]]:
        calls["http"] += 1
        return 503, {
            "facts": [{"id": "boundary-result", "fact": "boundary failure payload"}],
            "latency_ms": 4.0,
            "bounded_outcome": "response",
        }

    async def search_mcp(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        calls["mcp"] += 1
        raise adapter.AdapterFailed("observed MCP failure")

    async def token_count(*_args: Any, **_kwargs: Any) -> int:
        calls["tokens"] += 1
        return 10 if calls["tokens"] == 1 else 1426

    monkeypatch.setattr(adapter, "_search_http", search_http)
    monkeypatch.setattr(adapter, "_search_mcp", search_mcp)
    monkeypatch.setattr(adapter, "_groups_token_count", token_count)
    outcome = asyncio.run(
        adapter._load_search(
            SimpleNamespace(settings=SimpleNamespace()),  # type: ignore[arg-type]
            {"inputs": {"matrix_ref": matrix}},
            current,
        )
    )

    metrics = {metric["name"]: metric for metric in outcome.metrics}
    profiles = outcome.observations["profiles"]
    assert outcome.status == "PASS"
    assert calls == {"http": 888, "mcp": 528, "tokens": 2}
    assert len(profiles["matrix"]) == 72
    assert profiles["max_error_rate"] == 1.0
    assert profiles["max_hit_at_5_delta_pp"] is None
    assert all(profile["hit_at_5"] == 0.0 for profile in profiles["matrix"])
    assert all(
        "results" not in profile and "facts" not in profile for profile in profiles["matrix"]
    )
    assert set(metrics) == {
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "error_rate",
        "timeout_rate",
        "throughput_rps",
        "hit_at_5",
        "tokens_per_search",
    }
    assert metrics["p95_ms"]["sample_size"] == 200
    assert {metric["sample_size"] for name, metric in metrics.items() if name != "p95_ms"} == {1416}
    assert metrics["tokens_per_search"]["value"] == 1.0

    invalid = {**matrix, "scope_counts": [1, 5]}
    with pytest.raises(adapter.AdapterBlocked, match="scope_counts"):
        asyncio.run(
            adapter._load_search(
                SimpleNamespace(settings=SimpleNamespace()),  # type: ignore[arg-type]
                {"inputs": {"matrix_ref": invalid}},
                current,
            )
        )


def test_load_ingestion_builds_expected_and_actual_canonical_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facts_by_group: dict[str, list[dict[str, Any]]] = {}
    visibility_queries: list[str] = []
    ingest_calls = 0

    async def ensure_scope(
        _request: dict[str, Any], current: dict[str, Any], alias: str = "default"
    ) -> dict[str, Any]:
        principal = {
            "api_key": f"key-{alias}",
            "principal_id": f"principal-{alias}",
            "workspace_id": "00000000-0000-0000-0000-000000000001",
            "project_id": "00000000-0000-0000-0000-000000000002",
            "group_id": f"group-{alias}",
        }
        current.setdefault("principals", {})[alias] = principal
        return principal

    async def ensure_source(
        _container: Any,
        _request: dict[str, Any],
        _current: dict[str, Any],
        **kwargs: Any,
    ) -> str:
        return f"source-{kwargs['principal_alias']}"

    async def ingest(
        _container: Any,
        _request: dict[str, Any],
        task_current: dict[str, Any],
        record: dict[str, Any],
        **_kwargs: Any,
    ) -> tuple[Any, str, dict[str, int]]:
        nonlocal ingest_calls
        ingest_calls += 1
        principal = next(iter(task_current["principals"].values()))
        group_id = str(principal["group_id"])
        triples = record["metadata"]["triples"]
        assert len(triples) == 2
        facts_by_group.setdefault(group_id, []).extend(triples)
        return (
            SimpleNamespace(artifact_version_id=f"artifact-{ingest_calls}"),
            "source",
            {"pending": 1},
        )

    async def settle(*_args: Any, **_kwargs: Any) -> dict[str, int]:
        return {"done": 1}

    async def durable_times(
        _container: Any, _group_id: str, artifact_ids: list[str]
    ) -> dict[str, datetime]:
        completed = datetime.now(UTC) - timedelta(milliseconds=10)
        return dict.fromkeys(artifact_ids, completed)

    async def search_http(**kwargs: Any) -> tuple[int, dict[str, Any]]:
        visibility_queries.append(str(kwargs["query"]))
        return 200, {"facts": [{"id": "actual-result", "fact": "visible"}]}

    async def snapshot(_container: Any, group_id: str) -> dict[str, Any]:
        return {
            "facts_state": [
                {**triple, "lifecycle_state": "active"} for triple in facts_by_group[group_id]
            ]
        }

    async def token_count(_container: Any, group_ids: list[str], *, request_kind: str) -> int:
        assert request_kind == "ingest"
        return sum(len(facts_by_group.get(group_id, [])) * 3 for group_id in group_ids)

    monkeypatch.setattr(adapter, "_ensure_scope", ensure_scope)
    monkeypatch.setattr(adapter, "_ensure_source", ensure_source)
    monkeypatch.setattr(adapter, "_ingest", ingest)
    monkeypatch.setattr(adapter, "_wait_for_group_jobs", settle)
    monkeypatch.setattr(adapter, "_artifact_durable_times", durable_times)
    monkeypatch.setattr(adapter, "_search_http", search_http)
    monkeypatch.setattr(adapter, "_text_hit", lambda facts, expected: bool(facts and expected))
    monkeypatch.setattr(adapter, "_database_snapshot", snapshot)
    monkeypatch.setattr(adapter, "_groups_token_count", token_count)
    current: dict[str, Any] = {"slug": "eval-load", "principals": {}, "sources": {}}
    outcome = asyncio.run(
        adapter._load_ingestion(
            SimpleNamespace(),  # type: ignore[arg-type]
            {
                "case_id": "PERF-002",
                "inputs": {
                    "matrix_ref": {
                        **adapter._INGESTION_MATRIX,
                        "claims_per_record": 2,
                    }
                },
            },
            current,
        )
    )

    profiles = outcome.observations["profiles"]
    expected = profiles["expected_fixture"]
    actual = profiles["final_state"]

    def canonical(item: dict[str, str]) -> tuple[str, str, str]:
        return item["subject"], item["predicate"], item["object"]

    assert outcome.status == "PASS"
    assert ingest_calls == 1200
    assert len(visibility_queries) == 1200
    assert all(
        any(predicate in query for predicate in ("RUNS_ON", "DEPENDS_ON", "OWNS", "DEPLOYED_TO"))
        for query in visibility_queries
    )
    assert len(profiles["matrix"]) == 4
    assert len(expected) == len(actual) == 2400
    assert Counter(map(canonical, expected)) == Counter(map(canonical, actual))
    assert all(set(item) == {"subject", "predicate", "object"} for item in expected)
    assert all(set(item) == {"subject", "predicate", "object"} for item in actual)
    assert {metric["name"] for metric in outcome.metrics} == {
        "records_per_second",
        "queue_wait_p95_ms",
        "time_to_searchable_p95_ms",
        "tokens_per_artifact",
    }
    assert "projection_parity" not in {metric["name"] for metric in outcome.metrics}
    assert (
        next(
            metric["value"] for metric in outcome.metrics if metric["name"] == "tokens_per_artifact"
        )
        == 6.0
    )


def test_agent_repetitions_use_bounded_concurrency(monkeypatch: pytest.MonkeyPatch) -> None:
    active = 0
    maximum_active = 0
    answer_count = 0

    async def answer(_settings: Any, *, principal: dict[str, Any], question: str) -> dict[str, Any]:
        nonlocal active, answer_count, maximum_active
        assert principal["group_id"] == "group-case"
        answer_count += 1
        usage_ref = f"run-{answer_count}"
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0)
        active -= 1
        return {
            "answer": question,
            "tool_calls": [],
            "used_result_ids": [],
            "citations": [],
            "latency_ms": 1.0,
            "abstained": False,
            "unsupported_claim_count": 0,
            "usage_ref": usage_ref,
            "token_usage": {"total_tokens": 10},
        }

    async def usage_by_ref(
        _container: Any, *, group_id: str, request_kind: str, refs: list[str]
    ) -> dict[str, int]:
        assert group_id == "group-case"
        assert request_kind == "search"
        return dict.fromkeys(refs, 5)

    monkeypatch.setenv("VERA_EVAL_AGENT_CONCURRENCY", "2")
    monkeypatch.setattr(adapter, "_agent_answer", answer)
    monkeypatch.setattr(adapter, "_usage_tokens_by_ref", usage_by_ref)
    questions = [{"text": "one"}, {"text": "two"}, {"text": "three"}]
    result = asyncio.run(
        adapter._agent(
            SimpleNamespace(settings=SimpleNamespace()),  # type: ignore[arg-type]
            {
                "case_id": "PERF-003",
                "inputs": {"questions_ref": questions, "repetitions": 4},
            },
            {
                "principals": {
                    "default": {
                        "api_key": "key",
                        "principal_id": "principal",
                        "group_id": "group-case",
                    }
                }
            },
        )
    )

    assert len(result["runs"]) == 12
    assert [run["answer"] for run in result["runs"]] == [
        "one",
        "two",
        "three",
    ] * 4
    assert [run["question_index"] for run in result["runs"]] == [0, 1, 2] * 4
    assert [run["repetition_index"] for run in result["runs"]] == [
        0,
        0,
        0,
        1,
        1,
        1,
        2,
        2,
        2,
        3,
        3,
        3,
    ]
    assert maximum_active == 2
    assert result["mcp_token_usage"] == {"total_tokens": 60, "source": "llm_usage"}
    assert all(run["token_usage"]["total_tokens"] == 15 for run in result["runs"])


def test_evidence_references_large_observations_by_digest() -> None:
    observations = {
        "profiles": {
            "expected_fixture": [
                {"subject": f"subject-{index}", "predicate": "uses", "object": "object"}
                for index in range(4400)
            ],
            "final_state": [
                {"subject": f"subject-{index}", "predicate": "uses", "object": "object"}
                for index in range(4400)
            ],
        }
    }
    descriptors = adapter._evidence(
        {
            "action": "load.ingestion",
            "case_id": "PERF-002",
            "evidence_labels": ["parity_report", "visibility_report"],
        },
        adapter._Outcome(boundaries=("api", "database", "graph")),
        observations,
    )
    expected_digest = hashlib.sha256(
        json.dumps(observations, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()

    assert len(descriptors) == 6
    assert all("observations" not in descriptor for descriptor in descriptors)
    assert all(descriptor["observation_roots"] == ["profiles"] for descriptor in descriptors)
    assert all(descriptor["observations_sha256"] == expected_digest for descriptor in descriptors)
    assert len(json.dumps(descriptors).encode()) < 10_000


def test_perf_003_metrics_use_run_latencies_and_measured_tokens() -> None:
    runs = [
        {
            "latency_ms": 100.0,
            "tool_calls": [{"latency_ms": 2.0}, {"latency_ms": 3.0}],
            "token_usage": {"total_tokens": 10},
            "unsupported_claim_count": 0,
        },
        {
            "latency_ms": 200.0,
            "tool_calls": [{"latency_ms": 10.0}],
            "token_usage": {"prompt_tokens": 12, "completion_tokens": 8},
            "unsupported_claim_count": 1,
        },
        {
            "latency_ms": 150.0,
            "tool_calls": [{"latency_ms": 5.0}],
            "token_usage": {"total_tokens": 30},
            "unsupported_claim_count": 0,
        },
    ]
    metrics = asyncio.run(
        adapter._daily_metrics(
            SimpleNamespace(),  # type: ignore[arg-type]
            {"case_id": "PERF-003", "action": "agent.run", "step_id": "S2"},
            {"observations": {"runs": runs}},
            adapter._Outcome(),
        )
    )

    by_name = {metric["name"]: metric for metric in metrics}
    assert set(by_name) == {"mcp_p95_ms", "agent_p95_ms"}
    assert by_name["mcp_p95_ms"]["value"] == 10.0
    assert by_name["agent_p95_ms"]["value"] == 200.0
    assert {metric["sample_size"] for metric in metrics} == {3}
    assert "task_success" not in by_name
