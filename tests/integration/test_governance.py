"""Governance and destructive-authorization over the live database (Phase 7).

Proves the authorization invariants (section 13): retraction is admin-only (a viewer is
denied), and promoting or rejecting a proposed fact requires an admin role, while the review
queue and fact timeline are readable governance surfaces.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.graph.null import NullMemoryEngine
from vera.adapters.identity import ApiKeyAuthenticator
from vera.adapters.persistence.repositories import SqlAlchemyCanonicalEntityRepository
from vera.adapters.persistence.repositories.fabric import SqlAlchemyFactRepository
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.identity import IdentityService
from vera.bootstrap import Container
from vera.domain.identity.models import Role
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Fact, FactLifecycle, ObjectType
from vera.entrypoints.api.main import create_app
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _FactEmbedder:
    async def embed(self, text: str) -> list[float]:
        del text
        return [1.0, 0.0]


@asynccontextmanager
async def _identity(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[IdentityService, None]:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        yield IdentityService(uow)
        await uow.commit()


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


def _auth(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"}


async def _proposed_fact(sessionmaker: async_sessionmaker[AsyncSession], group: str) -> str:
    async with _tenant(sessionmaker, group) as s:
        entity = await SqlAlchemyCanonicalEntityRepository(s).create(
            group_id=group, entity_type="Service", canonical_name="proposedsvc", aliases=[]
        )
        fk = fabric.fact_key(
            scope=group, subject_entity_id=entity.id, predicate="RUNS_ON", object_scalar="lambda"
        )
        await SqlAlchemyFactRepository(s).upsert(
            Fact(
                id=uuid7(),
                group_id=group,
                fact_key=fk,
                slot_key=fabric.slot_key(
                    scope=group, subject_entity_id=entity.id, predicate="RUNS_ON"
                ),
                subject_entity_id=entity.id,
                predicate="RUNS_ON",
                object_type=ObjectType.SCALAR,
                normalized_object=fabric.normalize_object(object_scalar="lambda"),
                object_scalar="lambda",
                lifecycle_state=FactLifecycle.PROPOSED,
                authority=0.4,
                confidence=0.5,
            )
        )
        return fk


@pytest.fixture
def governance_container(make_container: Callable[[object], Container]) -> Container:
    container = make_container(NullMemoryEngine())
    memory = container.settings.memory.model_copy(
        update={
            "vector_search_enabled": True,
            "embedder": "deterministic",
            "embedding_model": "fact-test",
            "embedding_dim": 2,
        }
    )
    return dataclasses.replace(
        container,
        settings=container.settings.model_copy(update={"memory": memory}),
        embedder=_FactEmbedder(),
    )


@pytest_asyncio.fixture
async def app_client(governance_container: Container) -> AsyncIterator[AsyncClient]:
    app = create_app()
    app.state.container = governance_container
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def _tenancy(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> tuple[str, str, str]:
    """Returns (project_group, admin_api_key, viewer_api_key)."""
    async with _identity(sessionmaker) as svc:
        _owner, owner_key = await svc.register(display_name="Owner")
        _admin, admin_key = await svc.register(display_name="Carol")
        _viewer, viewer_key = await svc.register(display_name="Val")
    owner = await ApiKeyAuthenticator(sessionmaker).authenticate(owner_key.api_key)
    assert owner is not None
    async with _identity(sessionmaker) as svc:
        org = await svc.create_organization(name="Acme", slug=f"acme-{uuid4().hex[:8]}")
        ws = await svc.create_workspace(
            actor=owner, org_id=org.id, name="P", slug=f"p-{uuid4().hex[:8]}"
        )
        project = await svc.create_project(
            actor=owner, workspace_id=ws.id, name="Api", slug=f"api-{uuid4().hex[:8]}"
        )
        await svc.add_member(
            actor=owner, workspace_id=ws.id, principal_id=_admin.id, role=Role.ADMIN
        )
        await svc.add_member(
            actor=owner, workspace_id=ws.id, principal_id=_viewer.id, role=Role.VIEWER
        )
    return project.value.group_id, admin_key.api_key, viewer_key.api_key


async def test_retraction_requires_an_admin_role(
    sessionmaker: async_sessionmaker[AsyncSession], app_client: AsyncClient
) -> None:
    group, admin_key, viewer_key = await _tenancy(sessionmaker)
    source_id = f"{group}:{uuid4()}"

    # A viewer is denied before anything is touched.
    denied = await app_client.delete(f"/memory/sources/{source_id}", headers=_auth(viewer_key))
    assert denied.status_code == 403

    # An admin passes authorization; the source simply does not exist, so 404.
    allowed = await app_client.delete(f"/memory/sources/{source_id}", headers=_auth(admin_key))
    assert allowed.status_code == 404


async def test_promote_and_reject_require_admin_and_review_queue_lists_proposals(
    sessionmaker: async_sessionmaker[AsyncSession],
    app_client: AsyncClient,
    governance_container: Container,
) -> None:
    group, admin_key, viewer_key = await _tenancy(sessionmaker)
    fact_key = await _proposed_fact(sessionmaker, group)

    # The proposed fact is visible in the review queue.
    queue = await app_client.get("/v2/knowledge/review", headers=_auth(admin_key))
    assert queue.status_code == 200
    assert any(f["fact_key"] == fact_key for f in queue.json())

    # A viewer may not promote.
    denied = await app_client.post(
        f"/v2/knowledge/review/{fact_key}/promote", headers=_auth(viewer_key)
    )
    assert denied.status_code == 403

    # An admin promotes it to active.
    promoted = await app_client.post(
        f"/v2/knowledge/review/{fact_key}/promote", headers=_auth(admin_key)
    )
    assert promoted.status_code == 200
    assert promoted.json()["lifecycle"] == "active"

    async with _tenant(sessionmaker, group) as s:
        state = await s.scalar(
            text("SELECT lifecycle_state FROM facts WHERE fact_key = :fk"), {"fk": fact_key}
        )
        embedding_jobs = await s.scalar(
            text(
                "SELECT count(*) FROM ingestion_jobs WHERE group_id = :g "
                "AND payload->>'job_kind' = 'embed_facts'"
            ),
            {"g": group},
        )
    assert state == "active"
    assert embedding_jobs == 1

    pool = LanePool(governance_container, lanes=1, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(governance_container, pool, batch_size=10)
    finally:
        await pool.stop()
    async with _tenant(sessionmaker, group) as session:
        assert (
            await session.scalar(
                text("SELECT count(*) FROM fact_embeddings WHERE group_id = :g"), {"g": group}
            )
            == 1
        )

    # The promotion is recorded on the fact's timeline.
    timeline = await app_client.get(
        f"/v2/knowledge/facts/{fact_key}/timeline", headers=_auth(admin_key)
    )
    assert timeline.status_code == 200
    assert any(e["event_type"] == "FACT_ACTIVATED" for e in timeline.json())


async def test_ontology_lists_predicate_policies(
    sessionmaker: async_sessionmaker[AsyncSession], app_client: AsyncClient
) -> None:
    _, admin_key, _ = await _tenancy(sessionmaker)
    resp = await app_client.get("/v2/knowledge/ontology", headers=_auth(admin_key))
    assert resp.status_code == 200
    body = resp.json()
    assert body["ontology_version"] >= 1
    runs_on = next(p for p in body["predicates"] if p["predicate"] == "RUNS_ON")
    assert runs_on["cardinality"] == "one_per_qualifier_set"
    assert runs_on["subject_types"] == ["Service"]
    assert runs_on["object_types"] == ["Environment"]
    assert runs_on["minimum_source_authority"] == 0.7

    diff = await app_client.get("/v2/knowledge/ontology/diff?from=1&to=2", headers=_auth(admin_key))
    assert diff.status_code == 200
    report = diff.json()
    assert report["from_version"] == 1
    assert report["to_version"] == 2
    assert "RUNS_ON" in report["predicate_policies_changed"]


async def test_community_reads_are_scoped_and_missing_lineage_is_not_found(
    app_client: AsyncClient,
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group, admin_key, _ = await _tenancy(sessionmaker)
    summaries = await app_client.get(
        f"/v2/knowledge/communities?project={group}", headers=_auth(admin_key)
    )
    assert summaries.status_code == 200
    assert summaries.json() == []

    lineage = await app_client.get(
        f"/v2/knowledge/communities/{uuid4()}/lineage", headers=_auth(admin_key)
    )
    assert lineage.status_code == 404


async def test_get_evidence_endpoint(
    sessionmaker: async_sessionmaker[AsyncSession], app_client: AsyncClient
) -> None:
    group, admin_key, _ = await _tenancy(sessionmaker)
    fact_key = await _proposed_fact(sessionmaker, group)

    # A known fact returns its (here empty) evidence list, not a 404.
    resp = await app_client.get(
        f"/v2/knowledge/facts/{fact_key}/evidence", headers=_auth(admin_key)
    )
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)

    # An unknown fact is a 404 (distinct from an existing fact with no evidence).
    missing = await app_client.get(
        "/v2/knowledge/facts/deadbeef/evidence", headers=_auth(admin_key)
    )
    assert missing.status_code == 404


async def test_feedback_endpoint_records_and_validates(
    sessionmaker: async_sessionmaker[AsyncSession], app_client: AsyncClient
) -> None:
    group, admin_key, _ = await _tenancy(sessionmaker)

    context = await app_client.post(
        "/v2/knowledge/context",
        headers=_auth(admin_key),
        json={"query": "where does it run", "project": group, "persist": True},
    )
    assert context.status_code == 200
    pack_id = context.json()["pack_id"]

    ok = await app_client.post(
        "/v2/knowledge/feedback",
        headers=_auth(admin_key),
        json={
            "context_pack_id": pack_id,
            "result_ref": pack_id,
            "signal": "up",
        },
    )
    assert ok.status_code == 200
    assert ok.json()["status"] == "recorded"
    assert ok.json()["query"] == "where does it run"
    assert ok.json()["rank"] is None

    replay = await app_client.post(
        "/v2/knowledge/feedback",
        headers=_auth(admin_key),
        json={"context_pack_id": pack_id, "result_ref": pack_id, "signal": "down"},
    )
    assert replay.status_code == 200
    assert replay.json()["status"] == "deduplicated"
    assert replay.json()["signal"] == "up"
    assert replay.json()["requested_signal"] == "down"
    async with sessionmaker() as session:
        stored_signal = await session.scalar(
            text(
                "SELECT signal FROM retrieval_feedback "
                "WHERE context_pack_id = CAST(:pack_id AS uuid)"
            ),
            {"pack_id": pack_id},
        )
    assert stored_signal == "up"

    spoofed = await app_client.post(
        "/v2/knowledge/feedback",
        headers=_auth(admin_key),
        json={"context_pack_id": pack_id, "result_ref": "some-fact-key", "signal": "up"},
    )
    assert spoofed.status_code == 400

    async with sessionmaker.begin() as session:
        expired_pack_id = await session.scalar(
            text(
                "INSERT INTO context_packs "
                "(group_id, query, token_estimate, result_count, omitted, conflicts, "
                "freshness_warnings, results, request_hash, result_references, expires_at, "
                "assembler_version, request) VALUES "
                "(:group_id, 'expired query', 0, 0, 0, 0, 0, '[]'::jsonb, :request_hash, "
                "'[]'::jsonb, now() - interval '1 second', 'context-assembler-v3', '{}'::jsonb) "
                "RETURNING id"
            ),
            {"group_id": group, "request_hash": "e" * 64},
        )
    expired = await app_client.post(
        "/v2/knowledge/feedback",
        headers=_auth(admin_key),
        json={
            "context_pack_id": str(expired_pack_id),
            "result_ref": str(expired_pack_id),
            "signal": "up",
        },
    )
    assert expired.status_code == 410

    # An invalid signal is rejected by request validation.
    bad = await app_client.post(
        "/v2/knowledge/feedback",
        headers=_auth(admin_key),
        json={"context_pack_id": pack_id, "result_ref": pack_id, "signal": "sideways"},
    )
    assert bad.status_code == 422


async def test_personal_proposal_retry_report_and_self_retract_are_idempotent(
    sessionmaker: async_sessionmaker[AsyncSession], app_client: AsyncClient
) -> None:
    _, other_key, caller_key = await _tenancy(sessionmaker)
    proposal = {
        "subject": "checkout-api",
        "predicate": "RUNS_ON",
        "object": "staging",
        "evidence_text": "Observed in the deployment manifest",
        "runtime": "OpenCode",
        "session_ref": "session-7",
        "task_ref": "task-15",
        "repository_ref": "https://token@github.com/Acme/Checkout.git?secret=yes",
    }

    created = await app_client.post(
        "/v2/knowledge/propose", headers=_auth(caller_key), json=proposal
    )
    assert created.status_code == 200
    assert created.json()["operation"] == "created"
    assert created.json()["proposal_context"]["runtime"] == "opencode"
    assert created.json()["proposal_context"]["repository_ref"] == ("github.com/Acme/Checkout")
    fact_key = created.json()["fact_key"]

    retried = await app_client.post(
        "/v2/knowledge/propose", headers=_auth(caller_key), json=proposal
    )
    assert retried.status_code == 200
    assert retried.json()["operation"] == "deduplicated"
    assert retried.json()["proposal_ref"] == created.json()["proposal_ref"]
    async with sessionmaker() as session:
        evidence_count = await session.scalar(
            text("SELECT count(*) FROM evidence WHERE assertion_id = CAST(:proposal_ref AS uuid)"),
            {"proposal_ref": created.json()["proposal_ref"]},
        )
    assert evidence_count == 1

    conflict = await app_client.post(
        "/v2/knowledge/propose",
        headers=_auth(caller_key),
        json={**proposal, "object": "production"},
    )
    assert conflict.status_code == 200
    assert conflict.json()["status"] == "conflicted"

    rejected = await app_client.post(
        "/v2/knowledge/propose",
        headers=_auth(caller_key),
        json={**proposal, "predicate": "NOT_IN_ONTOLOGY"},
    )
    assert rejected.status_code == 400
    invalid_context = await app_client.post(
        "/v2/knowledge/propose",
        headers=_auth(caller_key),
        json={**proposal, "repository_ref": "/home/alice/private/checkout"},
    )
    assert invalid_context.status_code == 400
    credential_context = await app_client.post(
        "/v2/knowledge/propose",
        headers=_auth(caller_key),
        json={**proposal, "repository_ref": "user:secret@github.com/Acme/Checkout.git"},
    )
    assert credential_context.status_code == 400
    assert "secret" not in credential_context.text
    other_context = await app_client.post(
        "/v2/knowledge/propose",
        headers=_auth(caller_key),
        json={
            **proposal,
            "subject": "other-session-api",
            "session_ref": "session-8",
            "repository_ref": "git@github.com:Acme/Other.git",
        },
    )
    assert other_context.status_code == 200
    assert other_context.json()["operation"] == "created"

    concurrent = {
        **proposal,
        "subject": "concurrent-api",
        "evidence_text": None,
    }
    first, second = await asyncio.gather(
        app_client.post(
            "/v2/knowledge/propose",
            headers=_auth(caller_key),
            json={**concurrent, "object": "staging"},
        ),
        app_client.post(
            "/v2/knowledge/propose",
            headers=_auth(caller_key),
            json={**concurrent, "object": "production"},
        ),
    )
    assert {first.json()["status"], second.json()["status"]} == {
        "proposed",
        "conflicted",
    }

    # Historical rows with the same fact key must not duplicate attempt rows in reports.
    group = created.json()["group_id"]
    async with _tenant(sessionmaker, group) as session:
        await session.execute(
            text(
                "INSERT INTO facts (group_id, fact_key, slot_key, subject_entity_id, predicate, "
                "object_type, normalized_object, object_entity_id, object_scalar, qualifiers, "
                "lifecycle_state, authority, confidence, valid_from, valid_to, expires_at, "
                "system_from, system_to, ontology_version_id) "
                "SELECT group_id, fact_key, slot_key, subject_entity_id, predicate, object_type, "
                "normalized_object, object_entity_id, object_scalar, qualifiers, 'retracted', "
                "authority, confidence, valid_from, valid_to, expires_at, "
                "system_from - interval '1 hour', system_from - interval '30 minutes', "
                "ontology_version_id FROM facts WHERE fact_key = :fact_key "
                "ORDER BY system_from DESC LIMIT 1"
            ),
            {"fact_key": fact_key},
        )

    report_params = {
        "runtime": "opencode",
        "session_ref": "session-7",
        "task_ref": "task-15",
        "repository_ref": "git@github.com:Acme/Checkout.git",
    }
    report = await app_client.get(
        "/v2/knowledge/proposals/report",
        headers=_auth(caller_key),
        params=report_params,
    )
    assert report.status_code == 200
    assert report.json()["counts"] == {
        "created": 2,
        "skipped": 2,
        "deduplicated": 1,
        "conflicted": 2,
        "rejected": 1,
    }
    assert report.json()["states"] == {"pending": 2}
    assert len(report.json()["proposals"]) == 6
    assert report.json()["next_cursor"] is None
    assert "/home/alice" not in str(report.json())
    assert all(
        item["proposal_context"].get("session_ref") == "session-7"
        for item in report.json()["proposals"]
    )

    first_page = await app_client.get(
        "/v2/knowledge/proposals/report",
        headers=_auth(caller_key),
        params={**report_params, "limit": 2},
    )
    assert first_page.status_code == 200
    assert len(first_page.json()["proposals"]) == 2
    assert first_page.json()["counts"] == report.json()["counts"]
    assert first_page.json()["states"] == report.json()["states"]
    assert first_page.json()["next_cursor"] is not None
    second_page = await app_client.get(
        "/v2/knowledge/proposals/report",
        headers=_auth(caller_key),
        params={
            **report_params,
            "cursor": first_page.json()["next_cursor"],
            "limit": 2,
        },
    )
    assert second_page.status_code == 200
    assert len(second_page.json()["proposals"]) == 2
    assert {item["attempt_ref"] for item in first_page.json()["proposals"]}.isdisjoint(
        item["attempt_ref"] for item in second_page.json()["proposals"]
    )

    empty_report = await app_client.get("/v2/knowledge/proposals/report", headers=_auth(caller_key))
    assert empty_report.status_code == 400

    denied = await app_client.post(
        f"/v2/knowledge/proposals/{fact_key}/retract", headers=_auth(other_key)
    )
    assert denied.status_code == 403

    retracted = await app_client.post(
        f"/v2/knowledge/proposals/{fact_key}/retract", headers=_auth(caller_key)
    )
    assert retracted.status_code == 200
    assert retracted.json()["operation"] == "retracted"
    repeated = await app_client.post(
        f"/v2/knowledge/proposals/{fact_key}/retract", headers=_auth(caller_key)
    )
    assert repeated.status_code == 200
    assert repeated.json()["operation"] == "already_retracted"
    cannot_reactivate = await app_client.post(
        f"/v2/knowledge/review/{fact_key}/promote", headers=_auth(caller_key)
    )
    assert cannot_reactivate.status_code == 403

    updated_report = await app_client.get(
        "/v2/knowledge/proposals/report",
        headers=_auth(caller_key),
        params={"session_ref": "session-7", "task_ref": "task-15"},
    )
    assert updated_report.status_code == 200
    assert updated_report.json()["counts"]["rejected"] == 3
    assert updated_report.json()["states"] == {"rejected": 1, "pending": 1}

    async with _tenant(sessionmaker, group) as session:
        lifecycle = await session.scalar(
            text(
                "SELECT lifecycle_state FROM facts WHERE fact_key = :fact_key "
                "ORDER BY system_from DESC, id DESC LIMIT 1"
            ),
            {"fact_key": fact_key},
        )
        assertion_state = await session.scalar(
            text("SELECT state FROM assertions WHERE id = CAST(:id AS uuid)"),
            {"id": created.json()["proposal_ref"]},
        )
        retract_events = await session.scalar(
            text(
                "SELECT count(*) FROM knowledge_events "
                "WHERE fact_id IN (SELECT id FROM facts WHERE fact_key = :fact_key) "
                "AND event_type = 'FACT_RETRACTED'"
            ),
            {"fact_key": fact_key},
        )
    assert lifecycle == "retracted"
    assert assertion_state == "withdrawn"
    assert retract_events == 1


async def test_fallback_proposal_quota_serializes_across_repository_contexts(
    sessionmaker: async_sessionmaker[AsyncSession],
    app_client: AsyncClient,
    governance_container: Container,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, caller_key = await _tenancy(sessionmaker)
    monkeypatch.setattr(governance_container.settings.knowledge, "proposals_per_task", 1)

    first, second = await asyncio.gather(
        app_client.post(
            "/v2/knowledge/propose",
            headers=_auth(caller_key),
            json={
                "subject": "quota-api-one",
                "predicate": "RUNS_ON",
                "object": "staging",
                "runtime": "opencode",
                "repository_ref": "git@github.com:Acme/One.git",
            },
        ),
        app_client.post(
            "/v2/knowledge/propose",
            headers=_auth(caller_key),
            json={
                "subject": "quota-api-two",
                "predicate": "RUNS_ON",
                "object": "staging",
                "runtime": "opencode",
                "repository_ref": "git@github.com:Acme/Two.git",
            },
        ),
    )

    assert first.status_code == second.status_code == 200
    assert {first.json()["operation"], second.json()["operation"]} == {"created", "skipped"}


async def test_proposal_retraction_serializes_with_a_concurrent_retry(
    sessionmaker: async_sessionmaker[AsyncSession],
    app_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, caller_key = await _tenancy(sessionmaker)
    proposal = {
        "subject": "race-api",
        "predicate": "RUNS_ON",
        "object": "staging",
        "task_ref": "race-task",
    }
    created = await app_client.post(
        "/v2/knowledge/propose", headers=_auth(caller_key), json=proposal
    )
    assert created.status_code == 200
    target_fact_key = str(created.json()["fact_key"])

    original_lock = SqlAlchemyFactRepository.lock_fact_key
    original_read = SqlAlchemyFactRepository.by_fact_key_for_update
    retraction_holding = asyncio.Event()
    proposal_waiting = asyncio.Event()
    release_retraction = asyncio.Event()
    target_lock_calls = 0

    async def tracked_lock(
        repository: SqlAlchemyFactRepository, *, group_id: str, fact_key: str
    ) -> None:
        nonlocal target_lock_calls
        if fact_key == target_fact_key:
            target_lock_calls += 1
            if target_lock_calls == 2:
                proposal_waiting.set()
        await original_lock(repository, group_id=group_id, fact_key=fact_key)

    async def paused_read(
        repository: SqlAlchemyFactRepository, *, group_id: str, fact_key: str
    ) -> Fact | None:
        fact = await original_read(repository, group_id=group_id, fact_key=fact_key)
        retraction_holding.set()
        await release_retraction.wait()
        return fact

    monkeypatch.setattr(SqlAlchemyFactRepository, "lock_fact_key", tracked_lock)
    monkeypatch.setattr(SqlAlchemyFactRepository, "by_fact_key_for_update", paused_read)

    retraction = asyncio.create_task(
        app_client.post(
            f"/v2/knowledge/proposals/{target_fact_key}/retract",
            headers=_auth(caller_key),
        )
    )
    await asyncio.wait_for(retraction_holding.wait(), timeout=2)
    retry = asyncio.create_task(
        app_client.post("/v2/knowledge/propose", headers=_auth(caller_key), json=proposal)
    )
    try:
        await asyncio.wait_for(proposal_waiting.wait(), timeout=2)
        assert not retry.done()
    finally:
        release_retraction.set()
    retracted, retried = await asyncio.gather(retraction, retry)

    assert retracted.status_code == 200
    assert retracted.json()["operation"] == "retracted"
    assert retried.status_code == 200
    assert retried.json()["status"] == "conflicted"
    async with sessionmaker() as session:
        active_assertions = await session.scalar(
            text(
                "SELECT count(*) FROM assertions a JOIN facts f ON f.id = a.fact_id "
                "WHERE f.fact_key = :fact_key AND a.state = 'active'"
            ),
            {"fact_key": target_fact_key},
        )
    assert active_assertions == 0


async def test_proposal_retraction_serializes_with_a_concurrent_promotion(
    sessionmaker: async_sessionmaker[AsyncSession],
    app_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, caller_key = await _tenancy(sessionmaker)
    created = await app_client.post(
        "/v2/knowledge/propose",
        headers=_auth(caller_key),
        json={"subject": "review-race-api", "predicate": "RUNS_ON", "object": "staging"},
    )
    assert created.status_code == 200
    target_fact_key = str(created.json()["fact_key"])

    original_lock = SqlAlchemyFactRepository.lock_fact_key
    original_read = SqlAlchemyFactRepository.by_fact_key_for_update
    retraction_holding = asyncio.Event()
    promotion_waiting = asyncio.Event()
    release_retraction = asyncio.Event()
    target_lock_calls = 0
    target_read_calls = 0

    async def tracked_lock(
        repository: SqlAlchemyFactRepository, *, group_id: str, fact_key: str
    ) -> None:
        nonlocal target_lock_calls
        if fact_key == target_fact_key:
            target_lock_calls += 1
            if target_lock_calls == 2:
                promotion_waiting.set()
        await original_lock(repository, group_id=group_id, fact_key=fact_key)

    async def paused_read(
        repository: SqlAlchemyFactRepository, *, group_id: str, fact_key: str
    ) -> Fact | None:
        nonlocal target_read_calls
        fact = await original_read(repository, group_id=group_id, fact_key=fact_key)
        if fact_key == target_fact_key:
            target_read_calls += 1
            if target_read_calls == 1:
                retraction_holding.set()
                await release_retraction.wait()
        return fact

    monkeypatch.setattr(SqlAlchemyFactRepository, "lock_fact_key", tracked_lock)
    monkeypatch.setattr(SqlAlchemyFactRepository, "by_fact_key_for_update", paused_read)

    retraction = asyncio.create_task(
        app_client.post(
            f"/v2/knowledge/proposals/{target_fact_key}/retract",
            headers=_auth(caller_key),
        )
    )
    await asyncio.wait_for(retraction_holding.wait(), timeout=2)
    promotion = asyncio.create_task(
        app_client.post(
            f"/v2/knowledge/review/{target_fact_key}/promote",
            headers=_auth(caller_key),
        )
    )
    try:
        await asyncio.wait_for(promotion_waiting.wait(), timeout=2)
        assert not promotion.done()
    finally:
        release_retraction.set()
    retracted, promoted = await asyncio.gather(retraction, promotion)

    assert retracted.status_code == 200
    assert retracted.json()["operation"] == "retracted"
    assert promoted.status_code == 403
    async with sessionmaker() as session:
        lifecycle = await session.scalar(
            text("SELECT lifecycle_state FROM facts WHERE fact_key = :fact_key"),
            {"fact_key": target_fact_key},
        )
        activation_events = await session.scalar(
            text(
                "SELECT count(*) FROM knowledge_events "
                "WHERE fact_id = (SELECT id FROM facts WHERE fact_key = :fact_key) "
                "AND event_type = 'FACT_ACTIVATED'"
            ),
            {"fact_key": target_fact_key},
        )
    assert lifecycle == "retracted"
    assert activation_events == 0
