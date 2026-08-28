"""Worker cutover with real provenance (Phase A slice), against the live database.

With memory.fabric_enabled the ingestion worker reconciles each episode's triples into the
authoritative fact store using the real trust/authority/version provenance the publish path
puts in the job's `_fabric` block (no hard-coded authority), and a new artifact version
withdraws a dropped proposition's assertion and retracts the fact that loses its support.
Uses the null memory engine (the fabric step reads the job payload, not the graph).
"""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vera.adapters.curation.extractor import StructuredClaimExtractor
from vera.adapters.graph.null import NullMemoryEngine
from vera.adapters.persistence.models.knowledge import ArtifactRow, ArtifactVersionRow
from vera.adapters.persistence.repositories.knowledge_read import SqlAlchemyKnowledgeReadModel
from vera.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from vera.application.curation import CurationService, IngestArtifact
from vera.bootstrap import Container
from vera.domain.curation.trust import authority_for_tier
from vera.domain.ports.curation import ExtractedClaim
from vera.domain.ports.memory_engine import EpisodeSpec, IngestReceipt
from vera.domain.ports.projection import ProjectedFact
from vera.entrypoints.worker.lane_pool import LanePool
from vera.entrypoints.worker.main import run_until_empty
from vera.shared.errors import is_err
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.time import utc_now
from vera.shared.types import GroupId, SourceId

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PROVENANCE_BODY = "# Runtime\n\nVera stores authoritative facts in PostgreSQL.\n"
_PROVENANCE_QUOTE = "Vera stores authoritative facts in PostgreSQL."


class _QuoteExtractor:
    def __init__(self, *, aligned: bool) -> None:
        self._aligned = aligned

    @property
    def provider(self) -> str:
        return "test-provider"

    @property
    def model(self) -> str:
        return "test-extractor-v1"

    async def extract(
        self, *, body: str, knowledge_type: str, metadata: object
    ) -> list[ExtractedClaim]:
        del knowledge_type, metadata
        start = body.index(_PROVENANCE_QUOTE)
        quote = _PROVENANCE_QUOTE if self._aligned else "Vera uses a relational database."
        return [
            ExtractedClaim(
                statement="Vera RUNS_ON PostgreSQL",
                subject="Vera",
                predicate="RUNS_ON",
                object="PostgreSQL",
                confidence=0.95,
                source_quote=quote,
                quote_start=start,
                quote_end=start + len(quote),
            )
        ]


class _PerChunkQuoteExtractor:
    def __init__(self) -> None:
        self.calls = 0

    @property
    def provider(self) -> str:
        return "test-provider"

    @property
    def model(self) -> str:
        return "per-chunk-v1"

    async def extract(
        self, *, body: str, knowledge_type: str, metadata: object
    ) -> list[ExtractedClaim]:
        del knowledge_type, metadata
        self.calls += 1
        quote = body.strip().splitlines()[-1]
        start = body.index(quote)
        subject, obj = ("payments", "postgresql") if self.calls == 1 else ("telemetry", "valkey")
        return [
            ExtractedClaim(
                statement=f"{subject} RUNS_ON {obj}",
                subject=subject,
                predicate="RUNS_ON",
                object=obj,
                source_quote=quote,
                quote_start=start,
                quote_end=start + len(quote),
            )
        ]


class _RecordingMemoryEngine(NullMemoryEngine):
    def __init__(self) -> None:
        self.episodes: list[EpisodeSpec] = []

    async def ingest_episode(self, episode: EpisodeSpec) -> IngestReceipt:
        self.episodes.append(episode)
        return await super().ingest_episode(episode)


class _RecordingFactProjection:
    def __init__(self) -> None:
        self.facts: dict[str, ProjectedFact] = {}

    async def project(self, fact: ProjectedFact) -> None:
        self.facts[fact.fact_key] = fact

    async def remove(self, *, group_id: str, fact_key: str) -> None:
        del group_id
        self.facts.pop(fact_key, None)

    async def projected_fact_keys(self, *, group_id: str) -> set[str]:
        return {key for key, fact in self.facts.items() if fact.group_id == group_id}

    async def clear(self, *, group_id: str) -> None:
        self.facts = {key: fact for key, fact in self.facts.items() if fact.group_id != group_id}


@pytest.fixture
def fabric_container(make_container: Callable[[object], Container]) -> Container:
    container = make_container(NullMemoryEngine())
    memory = container.settings.memory.model_copy(update={"fabric_enabled": True})
    settings = container.settings.model_copy(update={"memory": memory})
    return dataclasses.replace(container, settings=settings)


@asynccontextmanager
async def _tenant(
    sessionmaker: async_sessionmaker[AsyncSession], group: str
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session, session.begin():
        await session.execute(text("SET LOCAL ROLE vera_app"))
        await session.execute(text("SELECT set_config('vera.group_id', :g, true)"), {"g": group})
        yield session


async def _artifact_versions(
    container: Container, group: str, *, tier: int = 1, ordered: bool = False
) -> tuple[UUID, UUID]:
    """One artifact with two versions in a real tenancy; returns (v1_id, v2_id)."""
    sm = container.sessionmaker
    async with SqlAlchemyUnitOfWork(sm) as uow:
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
    async with _tenant(sm, group) as s:
        art = ArtifactRow(
            source_id=source_id,
            external_id="page-1",
            content_hash="h1",
            s3_key="k1",
            reference_time=utc_now(),
        )
        s.add(art)
        await s.flush()
        v1 = ArtifactVersionRow(
            artifact_id=art.id, version=1, content_hash="h1", s3_key="k1", reference_time=utc_now()
        )
        s.add(v1)
        await s.flush()
        v2 = ArtifactVersionRow(
            artifact_id=art.id,
            version=2,
            content_hash="h2",
            s3_key="k2",
            reference_time=utc_now(),
            predecessor_version_id=v1.id if ordered else None,
        )
        if ordered:
            art.current_version = 2
        s.add(v2)
        await s.flush()
        return v1.id, v2.id


def _fabric_meta(tier: int, version_id: UUID) -> dict[str, object]:
    return {
        "trust_tier": tier,
        "authority": authority_for_tier(tier),
        "confidence": 0.9,
        "verification": "human_verified",
        "ontology_version_id": None,
        "artifact_version_id": str(version_id),
    }


async def _enqueue(
    container: Container, group: str, source: str, triples: list[dict], meta: dict
) -> None:
    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={"triples": triples, "_fabric": meta},
    )


async def _drain(container: Container) -> None:
    pool = LanePool(container, lanes=1, queue_maxsize=8)
    pool.start()
    try:
        await run_until_empty(container, pool, batch_size=10)
    finally:
        await pool.stop()


async def _fact_state(container: Container, group: str, obj: str) -> str | None:
    # Edge-predicate objects are canonical entities (gap 17); scalar attributes stay scalar.
    # Match either representation by name.
    async with _tenant(container.sessionmaker, group) as s:
        return await s.scalar(
            text(
                "SELECT f.lifecycle_state FROM facts f "
                "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id "
                "WHERE f.group_id = :g AND (f.object_scalar = :o OR co.canonical_name = :o) "
                "ORDER BY f.system_from DESC LIMIT 1"
            ),
            {"g": group, "o": obj},
        )


async def test_worker_uses_real_source_authority_not_hardcoded(fabric_container: Container) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    v1, _ = await _artifact_versions(container, group)
    source = f"{group}:{uuid7()}"
    # A Tier 2 (curated) source: authority must be 0.85, never the old hard-coded 1.0.
    await _enqueue(
        container,
        group,
        source,
        [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}],
        _fabric_meta(2, v1),
    )
    await _drain(container)
    async with _tenant(container.sessionmaker, group) as s:
        authority = await s.scalar(
            text("SELECT authority FROM facts WHERE group_id = :g"), {"g": group}
        )
    assert authority == pytest.approx(0.85)
    assert await _fact_state(container, group, "eks") == "active"

    # Replaying the same episode does not duplicate.
    await _enqueue(
        container,
        group,
        source,
        [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}],
        _fabric_meta(2, v1),
    )
    await _drain(container)
    async with _tenant(container.sessionmaker, group) as s:
        n = await s.scalar(text("SELECT count(*) FROM facts WHERE group_id = :g"), {"g": group})
    assert n == 1


async def test_new_version_withdraws_dropped_proposition_on_live_path(
    fabric_container: Container,
) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    v1, v2 = await _artifact_versions(container, group)

    # v1 asserts two facts; both active.
    await _enqueue(
        container,
        group,
        f"{group}:{uuid7()}",
        [
            {"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"},
            {"subject": "paymentapi", "predicate": "DEPENDS_ON", "object": "postgres"},
        ],
        _fabric_meta(1, v1),
    )
    await _drain(container)
    assert await _fact_state(container, group, "eks") == "active"
    assert await _fact_state(container, group, "postgres") == "active"

    # v2 of the SAME artifact keeps eks but drops depends_on postgres.
    await _enqueue(
        container,
        group,
        f"{group}:{uuid7()}",
        [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}],
        _fabric_meta(1, v2),
    )
    await _drain(container)

    assert await _fact_state(container, group, "eks") == "active"  # still supported by v2
    assert await _fact_state(container, group, "postgres") == "retracted"  # lost its only support
    # The prior version's postgres assertion is withdrawn (history preserved, not deleted).
    async with _tenant(container.sessionmaker, group) as s:
        withdrawn = await s.scalar(
            text("SELECT count(*) FROM assertions WHERE group_id = :g AND state = 'withdrawn'"),
            {"g": group},
        )
    assert withdrawn >= 1


async def test_stale_predecessor_job_cannot_regress_the_current_fact(
    fabric_container: Container,
) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    v1, v2 = await _artifact_versions(container, group, ordered=True)

    await _enqueue(
        container,
        group,
        f"{group}:{uuid7()}",
        [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "ecs"}],
        _fabric_meta(1, v2),
    )
    await _drain(container)
    await _enqueue(
        container,
        group,
        f"{group}:{uuid7()}",
        [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}],
        _fabric_meta(1, v1),
    )
    await _drain(container)

    assert await _fact_state(container, group, "ecs") == "active"
    assert await _fact_state(container, group, "eks") is None


async def test_empty_new_version_retracts_the_previous_fact(
    fabric_container: Container,
) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    seed_version, _ = await _artifact_versions(container, group)
    async with _tenant(container.sessionmaker, group) as s:
        source_id = await s.scalar(
            text(
                "SELECT a.source_id FROM artifacts a "
                "JOIN artifact_versions v ON v.artifact_id = a.id WHERE v.id = :v"
            ),
            {"v": str(seed_version)},
        )
    assert source_id is not None

    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group)
        service = CurationService(uow, StructuredClaimExtractor())
        await service.ingest_artifact(
            IngestArtifact(
                source_id=UUID(str(source_id)),
                group_id=group,
                external_id="removable-page",
                body="",
                knowledge_type="fact_triple",
                metadata={
                    "triples": [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}]
                },
                source_revision=1,
            )
        )
        await uow.commit()
    await _drain(container)
    assert await _fact_state(container, group, "eks") == "active"

    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group)
        await CurationService(uow, StructuredClaimExtractor()).ingest_artifact(
            IngestArtifact(
                source_id=UUID(str(source_id)),
                group_id=group,
                external_id="removable-page",
                body="fact removed",
                knowledge_type="text",
                source_revision=2,
            )
        )
        await uow.commit()
    await _drain(container)
    assert await _fact_state(container, group, "eks") == "retracted"


async def test_worker_does_not_touch_fabric_when_disabled(
    make_container: Callable[[object], Container],  # default has fabric_enabled off
) -> None:
    container = make_container(NullMemoryEngine())
    assert container.settings.memory.fabric_enabled is False
    group = f"p:w-{uuid7().hex[:12]}"
    source = f"{group}:{uuid7()}"
    await container.queue.enqueue(
        group_id=GroupId(group),
        source_id=SourceId(source),
        dedup_uuid=deterministic_id(source),
        payload={"triples": [{"subject": "a", "predicate": "RUNS_ON", "object": "b"}]},
    )
    await _drain(container)
    async with _tenant(container.sessionmaker, group) as s:
        facts = await s.scalar(text("SELECT count(*) FROM facts WHERE group_id = :g"), {"g": group})
    assert facts == 0  # legacy path only


async def test_fabric_mode_commits_facts_before_outbox_projection(
    make_container: Callable[[object], Container],
) -> None:
    memory = _RecordingMemoryEngine()
    projection = _RecordingFactProjection()
    base = make_container(memory)
    settings = base.settings.model_copy(
        update={
            "memory": base.settings.memory.model_copy(
                update={"fabric_enabled": False, "fabric_write_mode": "fabric"}
            )
        }
    )
    container = dataclasses.replace(base, settings=settings, fact_projection=projection)
    group = f"p:w-{uuid7().hex[:12]}"
    version_id, _ = await _artifact_versions(container, group)
    await _enqueue(
        container,
        group,
        f"{group}:{uuid7()}",
        [{"subject": "paymentapi", "predicate": "RUNS_ON", "object": "eks"}],
        _fabric_meta(1, version_id),
    )

    await _drain(container)

    assert memory.episodes == []
    assert len(projection.facts) == 1
    assert next(iter(projection.facts.values())).object_name == "eks"
    async with _tenant(container.sessionmaker, group) as s:
        fact_count = await s.scalar(
            text("SELECT count(*) FROM facts WHERE group_id = :g AND lifecycle_state = 'active'"),
            {"g": group},
        )
        projection_jobs = await s.scalar(
            text(
                "SELECT count(*) FROM ingestion_jobs WHERE group_id = :g "
                "AND payload->>'job_kind' = 'project_facts' AND status = 'done'"
            ),
            {"g": group},
        )
    assert fact_count == 1
    assert projection_jobs == 1


async def _ingest_provenance_claim(
    container: Container,
    group: str,
    *,
    aligned: bool,
    tier: int = 1,
    drain: bool = True,
) -> UUID:
    version_id, _ = await _artifact_versions(container, group, tier=tier)
    async with _tenant(container.sessionmaker, group) as s:
        source_id = await s.scalar(
            text(
                "SELECT a.source_id FROM artifacts a "
                "JOIN artifact_versions v ON v.artifact_id = a.id WHERE v.id = :v"
            ),
            {"v": str(version_id)},
        )
    assert source_id is not None
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group)
        result = await CurationService(uow, _QuoteExtractor(aligned=aligned)).ingest_artifact(
            IngestArtifact(
                source_id=UUID(str(source_id)),
                group_id=group,
                external_id="provenance-page",
                body=_PROVENANCE_BODY,
                knowledge_type="text",
            )
        )
        assert result.value.claim_ids
        claim_id = UUID(result.value.claim_ids[0])
        await uow.commit()
    if drain:
        await _drain(container)
    return claim_id


async def test_live_path_persists_exact_chunk_quote_and_extraction_lineage(
    fabric_container: Container,
) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    await _ingest_provenance_claim(container, group, aligned=True)

    async with _tenant(container.sessionmaker, group) as s:
        row = (
            (
                await s.execute(
                    text(
                        "SELECT f.fact_key, a.extraction_run_id AS assertion_run_id, "
                        "e.extraction_run_id AS evidence_run_id, e.chunk_id, e.quote_start, "
                        "e.quote_end, e.quote_hash, c.text AS chunk_text, r.provider, r.model, "
                        "r.prompt_version, r.pipeline_version "
                        "FROM facts f JOIN assertions a ON a.fact_id = f.id "
                        "JOIN evidence e ON e.assertion_id = a.id "
                        "JOIN chunks c ON c.id = e.chunk_id "
                        "JOIN extraction_runs r ON r.id = a.extraction_run_id "
                        "WHERE f.group_id = :g"
                    ),
                    {"g": group},
                )
            )
            .mappings()
            .one()
        )

    start, end = row["quote_start"], row["quote_end"]
    assert row["assertion_run_id"] == row["evidence_run_id"]
    assert row["chunk_id"] is not None
    assert row["chunk_text"][start:end] == _PROVENANCE_QUOTE
    assert row["quote_hash"] == hashlib.sha256(_PROVENANCE_QUOTE.encode()).hexdigest()
    assert row["provider"] == "test-provider"
    assert row["model"] == "test-extractor-v1"
    assert row["prompt_version"] == "2"
    assert row["pipeline_version"]["extractor"] == "2"

    evidence = await SqlAlchemyKnowledgeReadModel(container.sessionmaker).get_evidence(
        group_ids=[group], fact_key=str(row["fact_key"])
    )
    assert evidence is not None
    assert evidence[0]["excerpt"] == _PROVENANCE_QUOTE
    assert evidence[0]["extraction_run_id"] == str(row["assertion_run_id"])


async def test_misaligned_quote_goes_to_review_without_synthetic_evidence(
    fabric_container: Container,
) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    await _ingest_provenance_claim(container, group, aligned=False)

    async with _tenant(container.sessionmaker, group) as s:
        row = (
            (
                await s.execute(
                    text(
                        "SELECT f.lifecycle_state, a.state AS assertion_state, "
                        "a.extraction_run_id, count(e.id) AS evidence_count "
                        "FROM facts f JOIN assertions a ON a.fact_id = f.id "
                        "LEFT JOIN evidence e ON e.assertion_id = a.id "
                        "WHERE f.group_id = :g "
                        "GROUP BY f.lifecycle_state, a.state, a.extraction_run_id"
                    ),
                    {"g": group},
                )
            )
            .mappings()
            .one()
        )
        candidate = (
            (
                await s.execute(
                    text(
                        "SELECT verification_status, needs_review, quote_hash "
                        "FROM candidate_claims WHERE group_id = :g"
                    ),
                    {"g": group},
                )
            )
            .mappings()
            .one()
        )
        published = await s.scalar(
            text("SELECT count(*) FROM published_episodes WHERE group_id = :g"), {"g": group}
        )

    assert row["lifecycle_state"] == "proposed"
    assert row["assertion_state"] == "needs_review"
    assert row["extraction_run_id"] is not None
    assert row["evidence_count"] == 0
    assert candidate == {
        "verification_status": "pending",
        "needs_review": True,
        "quote_hash": None,
    }
    assert published == 0


async def test_one_extraction_run_reconciles_every_chunk_claim(
    fabric_container: Container,
) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    version_id, _ = await _artifact_versions(container, group)
    async with _tenant(container.sessionmaker, group) as s:
        source_id = await s.scalar(
            text(
                "SELECT a.source_id FROM artifacts a "
                "JOIN artifact_versions v ON v.artifact_id = a.id WHERE v.id = :v"
            ),
            {"v": str(version_id)},
        )
    assert source_id is not None
    extractor = _PerChunkQuoteExtractor()
    body = "# One\n\nService one runs here.\n\n# Two\n\nService two runs there.\n"
    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group)
        result = await CurationService(uow, extractor).ingest_artifact(
            IngestArtifact(
                source_id=UUID(str(source_id)),
                group_id=group,
                external_id="multi-chunk-page",
                body=body,
                knowledge_type="text",
            )
        )
        await uow.commit()
    assert extractor.calls == 2
    assert result.value.published == 2

    await _drain(container)
    async with _tenant(container.sessionmaker, group) as s:
        row = (
            (
                await s.execute(
                    text(
                        "SELECT count(DISTINCT f.id) AS facts, "
                        "count(DISTINCT a.run_key) AS episode_runs, "
                        "count(DISTINCT a.extraction_run_id) AS extraction_runs "
                        "FROM facts f JOIN assertions a ON a.fact_id = f.id "
                        "WHERE f.group_id = :g"
                    ),
                    {"g": group},
                )
            )
            .mappings()
            .one()
        )
    assert row == {"facts": 2, "episode_runs": 2, "extraction_runs": 1}


@pytest.mark.parametrize("tier", [3, 4])
async def test_misaligned_quote_queues_review_for_every_trust_tier(
    fabric_container: Container, tier: int
) -> None:
    container = fabric_container
    group = f"p:w-{uuid7().hex[:12]}"
    await _ingest_provenance_claim(container, group, aligned=False, tier=tier)
    async with _tenant(container.sessionmaker, group) as s:
        row = (
            (
                await s.execute(
                    text(
                        "SELECT f.lifecycle_state, a.state, count(e.id) AS evidence_count "
                        "FROM facts f JOIN assertions a ON a.fact_id = f.id "
                        "LEFT JOIN evidence e ON e.assertion_id = a.id "
                        "WHERE f.group_id = :g GROUP BY f.lifecycle_state, a.state"
                    ),
                    {"g": group},
                )
            )
            .mappings()
            .one()
        )
    assert row == {
        "lifecycle_state": "proposed",
        "state": "needs_review",
        "evidence_count": 0,
    }


async def test_invalid_provenance_cannot_be_approved_or_ingested_into_graph(
    make_container: Callable[[object], Container],
) -> None:
    memory = _RecordingMemoryEngine()
    base = make_container(memory)
    settings = base.settings.model_copy(
        update={"memory": base.settings.memory.model_copy(update={"fabric_enabled": True})}
    )
    container = dataclasses.replace(base, settings=settings)
    group = f"p:w-{uuid7().hex[:12]}"
    claim_id = await _ingest_provenance_claim(container, group, aligned=False, drain=False)
    async with _tenant(container.sessionmaker, group) as s:
        await s.execute(
            text(
                "UPDATE ingestion_jobs SET payload = "
                "jsonb_set(payload, '{_fabric,needs_review}', 'false'::jsonb) "
                "WHERE group_id = :g"
            ),
            {"g": group},
        )
    await _drain(container)
    assert memory.episodes == []

    async with SqlAlchemyUnitOfWork(container.sessionmaker) as uow:
        await uow.use_tenant(group)
        approved = await CurationService(uow, _QuoteExtractor(aligned=True)).review_claim(
            claim_id=claim_id,
            reviewer_principal_id=None,
            approve=True,
        )
        await uow.rollback()
    assert is_err(approved)
    assert approved.error.code == "policy_rejected"
