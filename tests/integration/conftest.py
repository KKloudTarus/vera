"""Fixtures for integration tests that run against the live compose database.

The local ``.env`` is the source of truth for the DSN (it overrides the placeholder
the unit-test conftest sets). If the database is unreachable the tests skip rather
than fail, so `make test` stays green without Docker.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.identity import ApiKeyAuthenticator, CompositeAuthenticator
from vera.adapters.objectstore.s3_adapter import S3ObjectStore
from vera.adapters.persistence.repositories import SqlAlchemyRetrievalReadModel
from vera.adapters.persistence.repositories.scope import SqlAlchemyScopeResolver
from vera.adapters.persistence.repositories.sync import SqlAlchemySyncStateStore
from vera.adapters.persistence.repositories.usage import SqlAlchemyUsageSink
from vera.adapters.queue.postgres_queue import PostgresJobQueue
from vera.application.identity import ScopeResolutionService
from vera.bootstrap import Container
from vera.config.settings import get_settings
from vera.domain.ports.memory_engine import MemoryEngine


def _database_dsn() -> str:
    # Loaded lazily inside the fixture, not at import, so collecting these modules
    # during a unit-only run does not mutate the environment (which would flip the
    # memory provider for the whole session).
    load_dotenv(override=True)
    return os.environ.get("VERA_DB__DSN", "")


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(_database_dsn())
    try:
        async with eng.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        await eng.dispose()
        pytest.skip("integration database not reachable")
    try:
        yield eng
    finally:
        await eng.dispose()


_MUTABLE_TABLES = (
    "ingestion_jobs, retrieval_feedback, graph_node_map, graph_edge_map, published_episodes, "
    "group_embedding_state, rerank_weights, "
    "context_packs, snapshot_facts, knowledge_snapshots, "
    "knowledge_events, evidence, fact_relations, assertions, facts, chunks, "
    "reviews, candidate_claims, entity_aliases, canonical_entities, artifact_versions, artifacts, "
    "knowledge_sources, memberships, credentials, service_accounts, principals, sync_jobs, "
    "sync_cursors, projects, workspaces, organizations, audit_events, llm_usage"
)


@pytest_asyncio.fixture(autouse=True)
async def _reset_state(engine: AsyncEngine) -> AsyncIterator[None]:
    # Truncate mutable tables after each integration test so a shared database does not
    # leak state (e.g. leftover queued jobs) between tests. Neo4j stays isolated by
    # per-test group_ids.
    yield
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {_MUTABLE_TABLES} RESTART IDENTITY CASCADE"))


@pytest.fixture
def sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def make_container(
    engine: AsyncEngine, sessionmaker: async_sessionmaker[AsyncSession]
) -> Callable[[MemoryEngine], Container]:
    """Build a Container wired to the live DB with a caller-supplied memory engine."""

    def _make(memory: MemoryEngine, *, visibility_timeout_s: int = 300) -> Container:
        settings = get_settings()
        return Container(
            settings=settings,
            engine=engine,
            sessionmaker=sessionmaker,
            queue=PostgresJobQueue(sessionmaker, visibility_timeout_s=visibility_timeout_s),
            memory=memory,
            object_store=S3ObjectStore(settings.objectstore),
            retrieval_read=SqlAlchemyRetrievalReadModel(sessionmaker),
            authenticator=CompositeAuthenticator(api_key=ApiKeyAuthenticator(sessionmaker)),
            scopes=ScopeResolutionService(SqlAlchemyScopeResolver(sessionmaker)),
            usage_sink=SqlAlchemyUsageSink(sessionmaker),
            sync_state=SqlAlchemySyncStateStore(sessionmaker),
            extractor=StructuredClaimExtractor(),
            judge=None,
            entity_judge=None,
            embedder=None,
        )

    return _make
