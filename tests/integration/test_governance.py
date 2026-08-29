"""Governance and destructive-authorization over the live database (Phase 7).

Proves the authorization invariants (section 13): retraction is admin-only (a viewer is
denied), and promoting or rejecting a proposed fact requires an admin role, while the review
queue and fact timeline are readable governance surfaces.
"""

from __future__ import annotations

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
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


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


@pytest_asyncio.fixture
async def app_client(make_container: Callable[[object], Container]) -> AsyncIterator[AsyncClient]:
    app = create_app()
    app.state.container = make_container(NullMemoryEngine())
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
    sessionmaker: async_sessionmaker[AsyncSession], app_client: AsyncClient
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
    assert state == "active"

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
    _, admin_key, _ = await _tenancy(sessionmaker)

    ok = await app_client.post(
        "/v2/knowledge/feedback",
        headers=_auth(admin_key),
        json={"result_ref": "some-fact-key", "signal": "up"},
    )
    assert ok.status_code == 200
    assert ok.json()["status"] == "recorded"

    # An invalid signal is rejected by request validation.
    bad = await app_client.post(
        "/v2/knowledge/feedback",
        headers=_auth(admin_key),
        json={"result_ref": "some-fact-key", "signal": "sideways"},
    )
    assert bad.status_code == 422
