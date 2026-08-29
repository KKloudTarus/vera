"""The MCP service resolves scope server-side and isolates tenants.

Exercises the surface an AI client sees through the MCP tools, but calls the service
directly so the test needs no HTTP transport or JWT. Two projects live in separate
workspaces; the principal is a member of one. Every read must stay inside that scope,
and a proposal must land in the principal's personal scope as an unverified claim.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
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
from vera.adapters.persistence.repositories.sync import SqlAlchemySyncStateStore
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.connectors import SyncRunner
from vera.application.curation import CurationService, IngestArtifact
from vera.bootstrap import Container
from vera.domain.ports.connectors import ConnectorBatch, ConnectorRecord
from vera.entrypoints.knowledge.service import KnowledgeService
from vera.entrypoints.mcp.service import VeraMcpService
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.ids import uuid7
from vera.shared.time import utc_now
from vera.shared.types import JsonDict

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _SingleBatchConnector:
    def __init__(self, record: ConnectorRecord) -> None:
        self._record = record

    @property
    def kind(self) -> str:
        return "acceptance"

    async def fetch_changes(self, cursor: JsonDict | None) -> ConnectorBatch:
        return ConnectorBatch(records=(self._record,), next_cursor={"done": True})


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
    fabric: bool = False,
    body: str = "",
    via_connector: bool = False,
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
        triple: dict[str, object] = {
            "subject": "paymentapi",
            "predicate": "RUNS_ON",
            "object": obj,
        }
        quote = f"paymentapi runs on {obj}"
        if quote in body:
            start = body.index(quote)
            triple.update(
                source_quote=quote,
                quote_start=start,
                quote_end=start + len(quote),
            )
        if not via_connector:
            await CurationService(uow, StructuredClaimExtractor()).ingest_artifact(
                IngestArtifact(
                    source_id=source_id,
                    group_id=group,
                    external_id=f"rec-{obj}",
                    body=body,
                    knowledge_type="fact_triple",
                    metadata={"triples": [triple]},
                )
            )
        await uow.commit()
        workspace_id = ws.id

    if via_connector:
        outcome = await SyncRunner(
            uow_factory=lambda: SqlAlchemyUnitOfWork(sessionmaker),
            extractor=StructuredClaimExtractor(),
            state=SqlAlchemySyncStateStore(sessionmaker),
        ).sync(
            source_id=source_id,
            group_id=group,
            connector=_SingleBatchConnector(
                ConnectorRecord(
                    external_id=f"rec-{obj}",
                    body=body,
                    knowledge_type="fact_triple",
                    metadata={"triples": [triple]},
                    reference_time=utc_now(),
                )
            ),
        )
        assert outcome.processed == 1

    container = make_container(graphiti_engine)
    if fabric:
        container.settings = container.settings.model_copy(
            update={
                "memory": container.settings.memory.model_copy(
                    update={"fabric_write_mode": "fabric"}
                )
            }
        )
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


@pytest.mark.issue6_acceptance
async def test_knowledge_agent_contracts_resolve_scope_and_retrieve_frozen_pack(
    sessionmaker: async_sessionmaker[AsyncSession],
    make_container: Callable[[object], Container],
    graphiti_engine: GraphitiMemoryEngine,
) -> None:
    group = f"p:{uuid7().hex[:12]}k"
    other_group = f"p:{uuid7().hex[:12]}x"
    workspace_id = await _provision_and_publish(
        sessionmaker,
        make_container,
        graphiti_engine,
        group=group,
        obj="prodeksmy",
        fabric=True,
        body="# src/payment.py\n\npaymentapi runs on prodeksmy",
        via_connector=True,
    )
    other_workspace_id = await _provision_and_publish(
        sessionmaker,
        make_container,
        graphiti_engine,
        group=other_group,
        obj="secretcluster",
        fabric=True,
    )
    principal_id, _ = await _create_member(sessionmaker, workspace_id=workspace_id)
    other_principal_id, _ = await _create_member(sessionmaker, workspace_id=other_workspace_id)
    async with sessionmaker() as session:
        source_id = str(
            await session.scalar(
                text(
                    "SELECT s.id FROM knowledge_sources s JOIN projects p ON p.id = s.project_id "
                    "WHERE p.group_id = :g"
                ),
                {"g": group},
            )
        )
        entity_id = str(
            await session.scalar(
                text("SELECT id FROM canonical_entities WHERE group_id = :g"), {"g": group}
            )
        )
        fact_key = str(
            await session.scalar(
                text("SELECT fact_key FROM facts WHERE group_id = :g"), {"g": group}
            )
        )
        await session.execute(
            text(
                "UPDATE knowledge_sources SET trust_tier = 2, "
                "config = CAST(:config AS jsonb) WHERE id::text = :id"
            ),
            {
                "id": source_id,
                "config": ('{"repository":"vera","branch":"main","document_type":"adr"}'),
            },
        )
        await session.commit()

    contract_container = make_container(graphiti_engine)
    scope_resolver = SqlAlchemyScopeResolver(sessionmaker)
    service = KnowledgeService(contract_container, scope_resolver)
    fact = await service.get_fact(principal_id, fact_key=fact_key)
    assert fact is not None and fact["object"] == "prodeksmy"
    entity = await service.get_entity(principal_id, entity_id=entity_id)
    assert entity is not None
    assert entity["canonical_name"] == "paymentapi"
    assert any(item["fact_key"] == fact_key for item in entity["facts"])
    source = await service.get_source(principal_id, source_id=source_id)
    assert source is not None
    assert source["freshness"]["artifact_count"] == 1
    assert source["artifacts"][0]["versions"]
    explored = await VeraMcpService(contract_container, scope_resolver).explore(
        principal_id, entity="paymentapi", depth=2, limit=20
    )
    explored_facts = " ".join(str(item["fact"]) for item in explored)
    assert "prodeksmy" in explored_facts
    assert "secretcluster" not in explored_facts

    created = await service.get_context(
        principal_id,
        query="prodeksmy",
        project=f"pr-{group}",
        repository="vera",
        branch="main",
        code_path="src/payment.py",
        document_type="adr",
        source_type="cmdb",
        include_predicates=(str(fact["predicate"]),),
        max_trust_tier=2,
    )
    assert created["results"]
    assert all("excerpt" in result["citation"] for result in created["results"])
    fact_result = next(result for result in created["results"] if result["kind"] == "fact")
    assert fact_result["citation"]["excerpt"] == "paymentapi runs on prodeksmy"
    assert fact_result["citation"]["evidence_id"] is not None
    assert fact_result["citation"]["assertion_id"] is not None
    assert fact_result["citation"]["source_id"] == source_id
    assert fact_result["citation"]["chunk_id"] is not None
    assert fact_result["citation"]["artifact_version_id"] is not None
    assert (
        fact_result["citation"]["quote_hash"]
        == hashlib.sha256(b"paymentapi runs on prodeksmy").hexdigest()
    )
    fetched = await service.get_context_pack(principal_id, pack_id=str(created["pack_id"]))
    assert fetched == created
    assert await service.get_context_pack(principal_id, pack_id="invalid") is None
    assert (
        await service.get_context_pack(other_principal_id, pack_id=str(created["pack_id"])) is None
    )
    snapshot = await service.create_snapshot(principal_id, project=f"pr-{group}")
    assert snapshot["ontology_version_id"] is not None
    assert await service.get_snapshot(principal_id, snapshot_id=str(snapshot["snapshot_id"]))
    assert await service.get_snapshot(principal_id, snapshot_id="invalid") is None
    assert await service.get_entity(other_principal_id, entity_id=entity_id) is None
    assert await service.get_source(other_principal_id, source_id=source_id) is None

    for parameter, value in (
        ("repository", "other"),
        ("branch", "release"),
        ("code_path", "src/other.py"),
        ("document_type", "prd"),
        ("source_type", "confluence"),
    ):
        filtered = await service.get_context(
            principal_id,
            query="prodeksmy",
            project=f"pr-{group}",
            **{parameter: value},
        )
        assert filtered["result_count"] == 0, parameter
    included_predicate = await service.get_context(
        principal_id,
        query="prodeksmy",
        project=f"pr-{group}",
        include_predicates=(str(fact["predicate"]),),
    )
    assert any(result["kind"] == "fact" for result in included_predicate["results"])
    wrong_predicate = await service.get_context(
        principal_id,
        query="prodeksmy",
        project=f"pr-{group}",
        include_predicates=("DEPENDS_ON",),
    )
    assert not any(result["kind"] == "fact" for result in wrong_predicate["results"])
    excluded_predicate = await service.get_context(
        principal_id,
        query="prodeksmy",
        project=f"pr-{group}",
        exclude_predicates=(str(fact["predicate"]),),
    )
    assert not any(result["kind"] == "fact" for result in excluded_predicate["results"])
    strict_authority = await service.get_context(
        principal_id,
        query="prodeksmy",
        project=f"pr-{group}",
        min_authority=1.1,
    )
    assert strict_authority["result_count"] == 0
    strict_trust = await service.get_context(
        principal_id,
        query="prodeksmy",
        project=f"pr-{group}",
        max_trust_tier=1,
    )
    assert strict_trust["result_count"] == 0
    conflicts_only = await service.get_context(
        principal_id,
        query="prodeksmy",
        project=f"pr-{group}",
        conflict_handling="only",
    )
    assert conflicts_only["result_count"] == 0
    compact = await service.get_context(
        principal_id,
        query="prodeksmy",
        project=f"pr-{group}",
        citation_mode="compact",
    )
    assert compact["results"]
    assert all("excerpt" not in result["citation"] for result in compact["results"])

    async with sessionmaker() as session, session.begin():
        await session.execute(
            text("UPDATE facts SET valid_from = :future WHERE fact_key = :fact_key"),
            {"future": utc_now() + timedelta(days=1), "fact_key": fact_key},
        )
    temporal = await service.get_context(
        principal_id,
        query="prodeksmy",
        project=f"pr-{group}",
        as_of=utc_now(),
    )
    assert not any(result["kind"] == "fact" for result in temporal["results"])


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
