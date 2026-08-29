"""Live ingestion chunking + per-chunk extraction (gaps 5 and 6), against the live database.

Proves the ingestion path now persists structure-aware chunks and extracts per chunk (never
one unbounded call over the whole document), and that the content-hash no-op still holds.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.domain.ports.curation import ExtractedClaim
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_MARKDOWN = (
    "# Guide\n\nThe payment service runs on eks in prod.\n\n"
    "## Setup\n\nBilling depends on postgres now.\n\n"
    "## Cache\n\nThe cache uses redis today.\n"
)


class _PerChunkExtractor:
    """Returns one distinct triple per call, so the number of calls equals the number of
    chunks the ingestion fed it (proving per-chunk, bounded extraction).
    """

    def __init__(self) -> None:
        self.bodies: list[str] = []

    @property
    def provider(self) -> str:
        return "test"

    @property
    def model(self) -> str:
        return "per-chunk"

    async def extract(
        self, *, body: str, knowledge_type: str, metadata: object
    ) -> list[ExtractedClaim]:
        self.bodies.append(body)
        n = len(self.bodies)
        lines = body.strip().splitlines()
        quote = lines[-1] if lines else None
        start = body.index(quote) if quote is not None else None
        return [
            ExtractedClaim(
                statement=f"svc{n} RUNS_ON obj{n}",
                subject=f"svc{n}",
                predicate="RUNS_ON",
                object=f"obj{n}",
                source_quote=quote,
                quote_start=start,
                quote_end=start + len(quote) if start is not None and quote is not None else None,
            )
        ]


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


async def _source(sessionmaker: async_sessionmaker[AsyncSession], group: str, tier: int) -> object:
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        org = await uow.tenancy.create_organization(
            slug=f"o-{group}", name="O", group_id=f"o:{group}"
        )
        ws = await uow.tenancy.create_workspace(
            org_id=org.id, slug=f"w-{group}", name="W", group_id=f"w:{group}"
        )
        proj = await uow.tenancy.create_project(
            workspace_id=ws.id, slug=f"pr-{group}", name="P", group_id=group
        )
        source_id = await uow.sources.create(
            workspace_id=ws.id, project_id=proj.id, kind="confluence", name="C", trust_tier=tier
        )
        await uow.commit()
    return source_id


async def _chunk_count(
    sessionmaker: async_sessionmaker[AsyncSession], group: str, version_id: str
) -> int:
    async with _tenant(sessionmaker, group) as s:
        return await s.scalar(  # type: ignore[return-value]
            text("SELECT count(*) FROM chunks WHERE group_id = :g AND artifact_version_id = :v"),
            {"g": group, "v": version_id},
        )


async def test_live_ingest_chunks_and_extracts_per_chunk(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:i-{uuid7().hex[:12]}"
    source_id = await _source(sessionmaker, group, tier=3)  # review-required: no publish
    extractor = _PerChunkExtractor()

    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        svc = CurationService(uow, extractor)
        result = await svc.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=group,
                external_id="page-1",
                body=_MARKDOWN,
                knowledge_type="text",
            )
        )
        await uow.commit()

    version_id = result.value.artifact_version_id
    chunks = await _chunk_count(sessionmaker, group, version_id)
    assert chunks == 3  # one per heading section (gap 5: chunks persisted in live ingest)
    assert len(extractor.bodies) == 3  # extraction ran once per chunk (gap 6), never whole-doc
    assert all(len(b) < len(_MARKDOWN) for b in extractor.bodies)  # each call is a bounded chunk
    assert len(result.value.claim_ids) == 3  # one claim per chunk, not capped at a whole-doc call

    # Re-ingesting identical content is a no-op: no new chunks, no new extraction calls.
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        again = await CurationService(uow, extractor).ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=group,
                external_id="page-1",
                body=_MARKDOWN,
                knowledge_type="text",
            )
        )
        await uow.commit()
    assert again.value.action == "unchanged"
    assert len(extractor.bodies) == 3  # no further extraction
    assert await _chunk_count(sessionmaker, group, version_id) == 3


async def test_structured_ingest_extracts_once_from_metadata(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:i-{uuid7().hex[:12]}"
    source_id = await _source(sessionmaker, group, tier=3)
    extractor = _PerChunkExtractor()
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        svc = CurationService(uow, extractor)
        await svc.ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=group,
                external_id="cmdb-1",
                body="",
                knowledge_type="fact_triple",
                metadata={"triples": [{"subject": "a", "predicate": "RUNS_ON", "object": "b"}]},
            )
        )
        await uow.commit()
    # Structured triples come from metadata: one whole-payload extract call, no chunk loop.
    assert len(extractor.bodies) == 1


async def test_structured_document_quote_is_rebased_to_its_chunk(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    group = f"p:i-{uuid7().hex[:12]}"
    source_id = await _source(sessionmaker, group, tier=3)
    extractor = _PerChunkExtractor()
    body = "# Runtime\n\nVera stores facts in PostgreSQL.\n"
    async with SqlAlchemyUnitOfWork(sessionmaker) as uow:
        await uow.use_tenant(group)
        result = await CurationService(uow, extractor).ingest_artifact(
            IngestArtifact(
                source_id=source_id,
                group_id=group,
                external_id="cmdb-cited",
                body=body,
                knowledge_type="fact_triple",
                metadata={"triples": [{"subject": "a", "predicate": "RUNS_ON", "object": "b"}]},
            )
        )
        await uow.commit()

    async with _tenant(sessionmaker, group) as s:
        row = (
            (
                await s.execute(
                    text(
                        "SELECT cc.source_quote, cc.quote_start, cc.quote_end, cc.quote_hash, "
                        "cc.needs_review, c.text FROM candidate_claims cc "
                        "JOIN chunks c ON c.id = cc.chunk_id "
                        "WHERE cc.group_id = :g AND cc.id = :id"
                    ),
                    {"g": group, "id": result.value.claim_ids[0]},
                )
            )
            .mappings()
            .one()
        )
    assert row["text"][row["quote_start"] : row["quote_end"]] == row["source_quote"]
    assert row["quote_hash"] is not None
    assert row["needs_review"] is False
