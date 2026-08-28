"""The generic /v2/knowledge REST contracts over the live database (Phase 6).

Drives the real FastAPI app over ASGI. Identity/tenancy is set up through IdentityService and
the fact store is seeded directly; the test then exercises context, explain, conflicts, and
propose through HTTP, and proves the invariants: the server resolves scopes (a caller cannot
reach a project outside its scope) and a proposal lands in personal scope as a PROPOSED fact,
never a published shared fact.
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
from vera.adapters.persistence.repositories.fabric import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyFactRepository,
)
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.identity import IdentityService
from vera.bootstrap import Container
from vera.domain.identity.models import Role
from vera.domain.knowledge import fabric
from vera.domain.knowledge.fabric import Assertion, Fact, FactLifecycle, ObjectType, Polarity
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


async def _seed(sessionmaker: async_sessionmaker[AsyncSession], group: str) -> str:
    """A subject entity, an active fact (RUNS_ON eks) with a supporting assertion, and a
    disputed fact (RUNS_ON ecs). Returns the active fact's fact_key.
    """
    async with _tenant(sessionmaker, group) as s:
        entity = await SqlAlchemyCanonicalEntityRepository(s).create(
            group_id=group, entity_type="Service", canonical_name="paymentapi", aliases=[]
        )
        facts = SqlAlchemyFactRepository(s)
        assertions = SqlAlchemyAssertionRepository(s)
        eks_key = fabric.fact_key(
            scope=group, subject_entity_id=entity.id, predicate="RUNS_ON", object_scalar="eks"
        )
        for obj, lifecycle in (("eks", FactLifecycle.ACTIVE), ("ecs", FactLifecycle.DISPUTED)):
            fact = await facts.upsert(
                Fact(
                    id=uuid7(),
                    group_id=group,
                    fact_key=fabric.fact_key(
                        scope=group,
                        subject_entity_id=entity.id,
                        predicate="RUNS_ON",
                        object_scalar=obj,
                    ),
                    slot_key=fabric.slot_key(
                        scope=group, subject_entity_id=entity.id, predicate="RUNS_ON"
                    ),
                    subject_entity_id=entity.id,
                    predicate="RUNS_ON",
                    object_type=ObjectType.SCALAR,
                    normalized_object=fabric.normalize_object(object_scalar=obj),
                    object_scalar=obj,
                    lifecycle_state=lifecycle,
                    authority=1.0,
                    confidence=0.9,
                )
            )
            await assertions.upsert(
                Assertion(
                    id=uuid7(),
                    group_id=group,
                    fact_id=fact.id,
                    polarity=Polarity.SUPPORTS,
                    source_authority=1.0,
                    extractor_confidence=0.9,
                    verification_state="verified",
                )
            )
        return eks_key


@pytest_asyncio.fixture
async def app_client(
    make_container: Callable[[object], Container],
) -> AsyncIterator[AsyncClient]:
    app = create_app()
    app.state.container = make_container(NullMemoryEngine())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_knowledge_contracts_end_to_end(
    sessionmaker: async_sessionmaker[AsyncSession], app_client: AsyncClient
) -> None:
    # Tenancy: admin builds org/workspace/project; Alice is a workspace member.
    async with _identity(sessionmaker) as svc:
        _admin_p, admin_key = await svc.register(display_name="Admin")
        alice_p, alice_key = await svc.register(display_name="Alice")
    admin = await ApiKeyAuthenticator(sessionmaker).authenticate(admin_key.api_key)
    assert admin is not None
    async with _identity(sessionmaker) as svc:
        org = await svc.create_organization(name="Acme", slug=f"acme-{uuid4().hex[:8]}")
        ws = await svc.create_workspace(
            actor=admin, org_id=org.id, name="Platform", slug=f"plat-{uuid4().hex[:8]}"
        )
        project = await svc.create_project(
            actor=admin, workspace_id=ws.id, name="Api", slug=f"api-{uuid4().hex[:8]}"
        )
        await svc.add_member(
            actor=admin, workspace_id=ws.id, principal_id=alice_p.id, role=Role.MEMBER
        )
    group = project.value.group_id

    fact_key = await _seed(sessionmaker, group)
    alice = _auth(alice_key.api_key)

    # get_context resolves the caller's scope; the project hint picks the right one.
    ctx = await app_client.post(
        "/v2/knowledge/context", json={"query": "eks", "project": group}, headers=alice
    )
    assert ctx.status_code == 200, ctx.text
    body = ctx.json()
    assert body["result_count"] >= 1
    assert any(r["kind"] == "fact" and r["ref"] == fact_key for r in body["results"])

    # explain_fact returns the supporting assertions.
    explain = await app_client.get(f"/v2/knowledge/facts/{fact_key}/explain", headers=alice)
    assert explain.status_code == 200
    assert len(explain.json()["assertions"]) >= 1

    # conflicts surfaces the disputed fact.
    conflicts = await app_client.get("/v2/knowledge/conflicts", headers=alice)
    assert conflicts.status_code == 200
    assert any(c["object"] == "ecs" for c in conflicts.json())

    # propose lands in Alice's personal scope as a PROPOSED (not active, not shared) fact.
    proposed = await app_client.post(
        "/v2/knowledge/propose",
        json={"subject": "newservice", "predicate": "RUNS_ON", "object": "fargate"},
        headers=alice,
    )
    assert proposed.status_code == 200
    assert proposed.json()["lifecycle"] == "proposed"
    assert proposed.json()["group_id"] == alice_p.personal_group_id
    async with _tenant(sessionmaker, alice_p.personal_group_id) as s:
        active = await s.scalar(text("SELECT count(*) FROM facts WHERE lifecycle_state='active'"))
        proposed_count = await s.scalar(
            text("SELECT count(*) FROM facts WHERE lifecycle_state='proposed'")
        )
    assert active == 0 and proposed_count == 1  # never published as shared truth


async def test_scope_is_resolved_server_side(
    sessionmaker: async_sessionmaker[AsyncSession], app_client: AsyncClient
) -> None:
    async with _identity(sessionmaker) as svc:
        _admin_p, admin_key = await svc.register(display_name="Admin")
        _, bob_key = await svc.register(display_name="Bob")  # personal scope only
    admin = await ApiKeyAuthenticator(sessionmaker).authenticate(admin_key.api_key)
    assert admin is not None
    async with _identity(sessionmaker) as svc:
        org = await svc.create_organization(name="Acme", slug=f"acme-{uuid4().hex[:8]}")
        ws = await svc.create_workspace(
            actor=admin, org_id=org.id, name="P", slug=f"p-{uuid4().hex[:8]}"
        )
        project = await svc.create_project(
            actor=admin, workspace_id=ws.id, name="Api", slug=f"api-{uuid4().hex[:8]}"
        )
    group = project.value.group_id
    fact_key = await _seed(sessionmaker, group)

    bob = _auth(bob_key.api_key)
    # Bob may not target a project outside his resolved scopes.
    denied = await app_client.post(
        "/v2/knowledge/context", json={"query": "eks", "project": group}, headers=bob
    )
    assert denied.status_code == 403
    # And cannot read a fact in that project.
    unseen = await app_client.get(f"/v2/knowledge/facts/{fact_key}", headers=bob)
    assert unseen.status_code == 404
