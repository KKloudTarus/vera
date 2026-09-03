"""HTTP end-to-end for the identity and memory surface.

Drives the real FastAPI app over ASGI: self-service registration returns an API key,
an owner builds the tenancy, and search is scoped to the caller. The phase's headline
guarantee is proven here: two principals in different workspaces cannot see each
other's memory through the API.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from dataclasses import replace
from uuid import UUID

import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.adapters.graph.offline import DeterministicEmbedder, NoCrossEncoder, NoLLMClient
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.bootstrap import Container
from vera.entrypoints.api.main import create_app
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_MCP_SECRET = "mcp-token-issuance-secret-long-enough"  # noqa: S105
_MCP_ISSUER = "https://auth.vera.test"
_MCP_AUDIENCE = "https://mcp.vera.test"


@pytest_asyncio.fixture
async def graphiti_engine() -> AsyncIterator[GraphitiMemoryEngine]:
    from dotenv import load_dotenv
    from graphiti_core import Graphiti

    load_dotenv(override=True)
    client = Graphiti(
        uri=os.environ.get("VERA_NEO4J__URI", "bolt://localhost:7687"),
        user=os.environ.get("VERA_NEO4J__USER", "neo4j"),
        password=os.environ.get("VERA_NEO4J__PASSWORD", "vera-local-pass"),
        embedder=DeterministicEmbedder(1024),
        llm_client=NoLLMClient(),
        cross_encoder=NoCrossEncoder(),
    )
    engine = GraphitiMemoryEngine(client)
    if not await engine.health():
        await client.close()
        pytest.skip("Neo4j not reachable")
    await engine.ensure_schema()
    try:
        yield engine
    finally:
        await client.close()


async def _publish_fact(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
    *,
    group_id: str,
    workspace_id: UUID,
    project_id: UUID,
    obj: str,
) -> None:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group_id)
        source_id = await uow.sources.create(
            workspace_id=workspace_id,
            project_id=project_id,
            kind="cmdb",
            name="CMDB",
            trust_tier=1,
        )
        service = CurationService(uow, StructuredClaimExtractor())
        await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=group_id,
                external_id=f"rec-{obj}",
                body="",
                knowledge_type="fact_triple",
                metadata={
                    "triples": [{"subject": "paymentapi", "predicate": "RUNSON", "object": obj}]
                },
            )
        )
        await uow.commit()

    container = make_container(graphiti_engine)
    pool = LanePool(container, lanes=2, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()


def _auth(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"}


async def test_registration_and_me_require_a_valid_credential(
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    app = create_app()
    app.state.container = make_container(graphiti_engine)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        registered = await client.post("/identity/register", json={"display_name": "Solo"})
        assert registered.status_code == 201
        api_key = registered.json()["api_key"]

        anonymous = await client.get("/identity/me")
        assert anonymous.status_code == 401

        rejected = await client.get("/identity/me", headers=_auth("vera_nope.secret"))
        assert rejected.status_code == 401

        me = await client.get("/identity/me", headers=_auth(api_key))
        assert me.status_code == 200
        body = me.json()
        assert body["principal_id"] == registered.json()["principal_id"]
        assert body["group_ids"] == [body["personal_group_id"]]  # only personal scope yet


async def test_regular_user_api_key_issues_own_short_lived_mcp_jwt(
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    base = make_container(graphiti_engine)
    app = create_app()
    app.state.container = base
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        registered = await client.post("/identity/register", json={"display_name": "Claude User"})
        api_key = registered.json()["api_key"]

        unavailable = await client.post("/identity/mcp-token", headers=_auth(api_key))
        assert unavailable.status_code == 503

        mcp = base.settings.mcp.model_copy(
            update={
                "jwt_secret": SecretStr(_MCP_SECRET),
                "auth_issuer": _MCP_ISSUER,
                "auth_audience": _MCP_AUDIENCE,
                "token_ttl_seconds": 900,
            }
        )
        app.state.container = replace(
            base,
            settings=base.settings.model_copy(update={"mcp": mcp}),
        )
        anonymous = await client.post("/identity/mcp-token")
        assert anonymous.status_code == 401

        read_only = await client.post("/identity/mcp-token", headers=_auth(api_key))
        assert read_only.status_code == 200
        assert read_only.json()["scope"] == "memory:read"

        requested_scopes = [
            "memory:read",
            "memory:propose",
            "memory:feedback",
            "memory:snapshot",
        ]
        issued = await client.post(
            "/identity/mcp-token",
            headers=_auth(api_key),
            json={"scopes": requested_scopes},
        )

        assert issued.status_code == 200
        assert issued.headers["cache-control"] == "no-store"
        assert issued.headers["pragma"] == "no-cache"
        body = issued.json()
        assert body["token_type"] == "Bearer"  # noqa: S105 - OAuth token type
        assert body["expires_in"] == 900
        assert body["scope"] == ("memory:read memory:propose memory:feedback memory:snapshot")
        claims = jwt.decode(
            body["access_token"],
            _MCP_SECRET,
            algorithms=["HS256"],
            audience=_MCP_AUDIENCE,
            issuer=_MCP_ISSUER,
        )
        assert claims["sub"] == registered.json()["principal_id"]
        assert claims["exp"] - claims["iat"] == 900

        unsupported = await client.post(
            "/identity/mcp-token",
            headers=_auth(api_key),
            json={"scopes": ["memory:read", "memory:admin"]},
        )
        assert unsupported.status_code == 422

        missing_required = await client.post(
            "/identity/mcp-token",
            headers=_auth(api_key),
            json={"scopes": ["memory:propose"]},
        )
        assert missing_required.status_code == 422


async def test_closed_registration_rejects_signup(
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    from dataclasses import replace

    base = make_container(graphiti_engine)
    closed_api = base.settings.api.model_copy(update={"registration_open": False})
    container = replace(base, settings=base.settings.model_copy(update={"api": closed_api}))

    app = create_app()
    app.state.container = container
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        blocked = await client.post("/identity/register", json={"display_name": "Nope"})
        assert blocked.status_code == 403


async def test_two_workspaces_cannot_see_each_others_memory(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    app = create_app()
    app.state.container = make_container(graphiti_engine)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        admin_key = (
            await client.post("/identity/register", json={"display_name": "Admin"})
        ).json()["api_key"]
        admin = _auth(admin_key)

        org_resp = await client.post(
            "/identity/orgs", json={"name": "Acme", "slug": "acme"}, headers=admin
        )
        org_id = org_resp.json()["id"]

        ws_a = (
            await client.post(
                "/identity/workspaces",
                json={"org_id": org_id, "name": "A", "slug": "a"},
                headers=admin,
            )
        ).json()
        ws_b = (
            await client.post(
                "/identity/workspaces",
                json={"org_id": org_id, "name": "B", "slug": "b"},
                headers=admin,
            )
        ).json()
        proj_a = (
            await client.post(
                "/identity/projects",
                json={"workspace_id": ws_a["id"], "name": "PA", "slug": "pa"},
                headers=admin,
            )
        ).json()
        proj_b = (
            await client.post(
                "/identity/projects",
                json={"workspace_id": ws_b["id"], "name": "PB", "slug": "pb"},
                headers=admin,
            )
        ).json()

        alice = await client.post("/identity/register", json={"display_name": "Alice"})
        bob = await client.post("/identity/register", json={"display_name": "Bob"})
        alice_key = _auth(alice.json()["api_key"])
        bob_key = _auth(bob.json()["api_key"])

        add_alice = await client.post(
            "/identity/memberships",
            json={
                "workspace_id": ws_a["id"],
                "principal_id": alice.json()["principal_id"],
                "role": "member",
            },
            headers=admin,
        )
        add_bob = await client.post(
            "/identity/memberships",
            json={
                "workspace_id": ws_b["id"],
                "principal_id": bob.json()["principal_id"],
                "role": "member",
            },
            headers=admin,
        )
        assert add_alice.status_code == 201
        assert add_bob.status_code == 201

        # A plain member cannot create a project (RBAC through the HTTP layer).
        forbidden = await client.post(
            "/identity/projects",
            json={"workspace_id": ws_a["id"], "name": "Nope", "slug": "nope"},
            headers=alice_key,
        )
        assert forbidden.status_code == 403

        await _publish_fact(
            sessionmaker,
            make_container,
            graphiti_engine,
            group_id=proj_a["group_id"],
            workspace_id=UUID(ws_a["id"]),
            project_id=UUID(proj_a["id"]),
            obj="prodeksmy",
        )
        await _publish_fact(
            sessionmaker,
            make_container,
            graphiti_engine,
            group_id=proj_b["group_id"],
            workspace_id=UUID(ws_b["id"]),
            project_id=UUID(proj_b["id"]),
            obj="secretcluster",
        )

        alice_hits = await client.post(
            "/memory/search", json={"text": "paymentapi"}, headers=alice_key
        )
        bob_hits = await client.post("/memory/search", json={"text": "paymentapi"}, headers=bob_key)
        assert alice_hits.status_code == 200
        assert bob_hits.status_code == 200
        alice_facts = " ".join(h["fact"] for h in alice_hits.json())
        bob_facts = " ".join(h["fact"] for h in bob_hits.json())

        assert "prodeksmy" in alice_facts and "secretcluster" not in alice_facts
        assert "secretcluster" in bob_facts and "prodeksmy" not in bob_facts
