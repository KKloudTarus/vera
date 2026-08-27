"""The MCP service resolves scope server-side and isolates tenants.

Exercises the surface an AI client sees through the MCP tools, but calls the service
directly so the test needs no HTTP transport or JWT. Two projects live in separate
workspaces; the principal is a member of one. Every read must stay inside that scope,
and a proposal must land in the principal's personal scope as an unverified claim.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.graph.graphiti_adapter import GraphitiMemoryEngine
from vera.adapters.graph.offline import DeterministicEmbedder, NoCrossEncoder, NoLLMClient
from vera.adapters.persistence.models.identity import MembershipRow, PrincipalRow
from vera.adapters.persistence.repositories.scope import SqlAlchemyScopeResolver
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.bootstrap import Container
from vera.entrypoints.mcp.service import VeraMcpService
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def graphiti_engine() -> AsyncIterator[GraphitiMemoryEngine]:
    import os

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


async def _provision_and_publish(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
    *,
    group: str,
    obj: str,
) -> UUID:
    """Create org/workspace/project/source, publish one fact, drain the worker.

    Returns the workspace id so the caller can attach a membership to it.
    """
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{group}", name="Org", group_id=f"o:{group}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{group}", name="WS", group_id=f"w:{group}"
        )
        proj = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{group}", name="Proj", group_id=group
        )
        source_id = await uow.sources.create(
            workspace_id=ws.id, project_id=proj.id, kind="cmdb", name="CMDB", trust_tier=1
        )
        service = CurationService(uow, StructuredClaimExtractor())
        await service.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=group,
                external_id=f"rec-{obj}",
                body="",
                knowledge_type="fact_triple",
                metadata={
                    "triples": [{"subject": "paymentapi", "predicate": "RUNSON", "object": obj}]
                },
            )
        )
        await uow.commit()
        workspace_id = ws.id

    container = make_container(graphiti_engine)
    pool = LanePool(container, lanes=2, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()
    return workspace_id


async def _create_member(
    sessionmaker: async_sessionmaker[AsyncSession], *, workspace_id: UUID
) -> tuple[UUID, str]:
    """Create a principal and a workspace-wide membership. Returns (id, personal_group)."""
    personal_group = f"u:{uuid7().hex[:12]}"
    async with sessionmaker() as session:
        principal = PrincipalRow(
            kind="user",
            display_name="Agent Alice",
            personal_group_id=personal_group,
        )
        session.add(principal)
        await session.flush()  # server generates the uuidv7 primary key
        principal_id = principal.id
        session.add(
            MembershipRow(
                principal_id=principal_id,
                workspace_id=workspace_id,
                project_id=None,
                role="member",
            )
        )
        await session.commit()
    return principal_id, personal_group


async def test_search_stays_inside_the_caller_scope(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    # uuid7().hex[:12] is the millisecond timestamp, so two ids minted in the same
    # millisecond share it. The suffixes keep the two groups distinct regardless.
    member_group = f"p:{uuid7().hex[:12]}m"
    other_group = f"p:{uuid7().hex[:12]}o"
    ws_id = await _provision_and_publish(
        sessionmaker, make_container, graphiti_engine, group=member_group, obj="prodeksmy"
    )
    await _provision_and_publish(
        sessionmaker, make_container, graphiti_engine, group=other_group, obj="secretcluster"
    )
    principal_id, _ = await _create_member(sessionmaker, workspace_id=ws_id)

    container = make_container(graphiti_engine)
    service = VeraMcpService(container, SqlAlchemyScopeResolver(sessionmaker))

    hits = await service.search(principal_id, query="paymentapi", limit=10)
    facts = " ".join(h["fact"] for h in hits)
    assert "prodeksmy" in facts  # the fact in the member's own workspace
    assert "secretcluster" not in facts  # a fact in a workspace the principal cannot see


async def test_recent_changes_and_get_source_follow_scope(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    ws_id = await _provision_and_publish(
        sessionmaker, make_container, graphiti_engine, group=group, obj="prodeksmy"
    )
    principal_id, _ = await _create_member(sessionmaker, workspace_id=ws_id)

    container = make_container(graphiti_engine)
    service = VeraMcpService(container, SqlAlchemyScopeResolver(sessionmaker))

    changes = await service.recent_changes(principal_id, limit=20)
    assert changes, "the published fact should show up as a recent change"
    source_id = changes[0]["source_id"]

    prov = await service.get_source(principal_id, source_id=source_id)
    assert prov is not None
    assert prov["source_id"] == source_id
    assert prov["verification"] == "human_verified"


async def test_propose_lands_in_personal_scope_unverified(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    ws_id = await _provision_and_publish(
        sessionmaker, make_container, graphiti_engine, group=group, obj="prodeksmy"
    )
    principal_id, personal_group = await _create_member(sessionmaker, workspace_id=ws_id)

    container = make_container(graphiti_engine)
    service = VeraMcpService(container, SqlAlchemyScopeResolver(sessionmaker))

    result = await service.propose(
        principal_id, subject="cacheapi", predicate="RUNSON", obj="stagingeks"
    )
    assert result["status"] == "proposed"
    assert result["claim_ids"]

    async with sessionmaker() as s:
        unverified = await s.scalar(
            text(
                "SELECT count(*) FROM candidate_claims "
                "WHERE group_id = :g AND verification_status = 'unverified'"
            ),
            {"g": personal_group},
        )
        published = await s.scalar(
            text("SELECT count(*) FROM published_episodes WHERE group_id = :g"),
            {"g": personal_group},
        )
    assert unverified >= 1  # the proposal enters the personal scope unverified
    assert published == 0  # a tier-4 proposal is never auto-published


async def test_feedback_is_recorded_in_personal_scope(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}"
    ws_id = await _provision_and_publish(
        sessionmaker, make_container, graphiti_engine, group=group, obj="prodeksmy"
    )
    principal_id, personal_group = await _create_member(sessionmaker, workspace_id=ws_id)

    container = make_container(graphiti_engine)
    service = VeraMcpService(container, SqlAlchemyScopeResolver(sessionmaker))

    result = await service.feedback(principal_id, result_ref=str(uuid7()), signal="down")
    assert result["status"] == "recorded"

    async with sessionmaker() as s:
        recorded = await s.scalar(
            text("SELECT count(*) FROM retrieval_feedback WHERE group_id = :g"),
            {"g": personal_group},
        )
    assert recorded == 1


async def test_unknown_principal_has_no_scope(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    from vera.entrypoints.mcp.service import ScopeError

    container = make_container(graphiti_engine)
    service = VeraMcpService(container, SqlAlchemyScopeResolver(sessionmaker))
    with pytest.raises(ScopeError):
        await service.search(uuid7(), query="paymentapi")
