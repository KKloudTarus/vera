"""Bounded, hash-routed lane pool for ingestion.

Each group_id maps to one lane (``crc32(group_id) % lanes``), so all jobs for a
group run on the same lane, one at a time. Different groups spread across lanes and
run concurrently. Bounded lane queues provide backpressure: when a lane is full,
``submit`` blocks, which stalls the dispatcher and leaves work in Postgres. While
processing, a per-group advisory lock guards against a second replica touching the
same group, and the job is marked done in the same transaction that holds the lock.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import time
import zlib
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.repositories import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyCanonicalEntityRepository,
    SqlAlchemyEvidenceRepository,
    SqlAlchemyFactEmbeddingRepository,
    SqlAlchemyFactRelationRepository,
    SqlAlchemyFactRepository,
    SqlAlchemyGraphMapRepository,
    SqlAlchemyKnowledgeEventLog,
)
from vera.adapters.persistence.repositories.embedding_state import (
    SqlAlchemyEmbeddingStateRepository,
)
from vera.adapters.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from vera.adapters.persistence.repositories.projection import SqlAlchemyProjectionSource
from vera.application.curation.entity_resolver import SemanticEntityResolver
from vera.application.curation.reconciliation import (
    ArtifactReconciliation,
    ReconciliationService,
    ResolvedProposition,
)
from vera.application.projection.service import FactProjectionService
from vera.bootstrap import Container
from vera.config.settings import active_embedding
from vera.domain.curation.trust import TrustTier, authority_for_tier
from vera.domain.knowledge.fabric import FactEmbedding, fact_semantic_text
from vera.domain.ontology import is_edge_predicate
from vera.domain.ports.job_queue import QueuedJob
from vera.domain.ports.memory_engine import EpisodeSpec, IngestReceipt
from vera.observability import bind_log_context, clear_log_context, get_logger, span
from vera.observability.cost import UsageContext, reset_usage_context, set_usage_context
from vera.observability.metrics import record_ingestion
from vera.shared.ids import deterministic_id, uuid7
from vera.shared.time import utc_now
from vera.shared.types import GroupId, JsonDict, SourceId

log = get_logger(__name__)

_MARK_DONE = text("UPDATE ingestion_jobs SET status = 'done', last_error = NULL WHERE id = :id")
_GROUP_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended(:g, 0))")
_EPISODE_BY_SOURCE = text(
    "SELECT id FROM published_episodes WHERE group_id = :group_id AND source_id = :source_id"
)
_REFERENCE_TIME_BY_SOURCE = text(
    "SELECT reference_time FROM published_episodes "
    "WHERE group_id = :group_id AND source_id = :source_id"
)
_JOB_IS_INFLIGHT = text("SELECT 1 FROM ingestion_jobs WHERE id=:job_id AND status='inflight'")
_FACTS_TO_EMBED = text(
    "SELECT f.id, cs.canonical_name AS subject_name, f.predicate, "
    "COALESCE(co.canonical_name, f.object_scalar, '') AS object_name, "
    "f.object_type, f.qualifiers, fe.content_hash, fe.active "
    "FROM facts f "
    "JOIN canonical_entities cs ON cs.id = f.subject_entity_id AND cs.group_id = f.group_id "
    "LEFT JOIN canonical_entities co ON co.id = f.object_entity_id AND co.group_id = f.group_id "
    "LEFT JOIN fact_embeddings fe ON fe.fact_id = f.id AND fe.group_id = f.group_id "
    "AND fe.provider = :provider AND fe.model = :model AND fe.model_version = :model_version "
    "AND fe.dimension = :dimension "
    "WHERE f.group_id = :g AND f.lifecycle_state IN ('active', 'disputed') "
    "ORDER BY f.created_at, f.id"
)


def lane_for(group_id: str, lanes: int) -> int:
    """Stable, process-independent lane assignment (crc32 is not hash-salted)."""
    return zlib.crc32(group_id.encode("utf-8")) % lanes


def _correlation(trace_context: JsonDict) -> dict[str, str]:
    cid = trace_context.get("correlation_id")
    return {"correlation_id": str(cid)} if cid else {}


async def _triple_needs_review(
    session: AsyncSession,
    *,
    group_id: str,
    meta: dict[str, Any],
    triple: dict[str, Any],
) -> bool:
    if bool(meta.get("needs_review")):
        return True
    chunk_value = meta.get("chunk_id")
    version_value = meta.get("artifact_version_id")
    quote_hash = meta.get("quote_hash")
    source_quote = triple.get("source_quote")
    quote_start = triple.get("quote_start")
    quote_end = triple.get("quote_end")
    has_provenance = any(
        value is not None
        for value in (chunk_value, quote_hash, source_quote, quote_start, quote_end)
    )
    if not has_provenance:
        return False
    if (
        chunk_value is None
        or version_value is None
        or not isinstance(source_quote, str)
        or not source_quote
        or type(quote_start) is not int
        or type(quote_end) is not int
        or not isinstance(quote_hash, str)
        or quote_start < 0
        or quote_end <= quote_start
    ):
        return True
    try:
        chunk_id = UUID(str(chunk_value))
        version_id = UUID(str(version_value))
    except ValueError:
        return True
    chunk = (
        (
            await session.execute(
                text(
                    "SELECT text, artifact_version_id FROM chunks WHERE group_id = :g AND id = :c"
                ),
                {"g": group_id, "c": str(chunk_id)},
            )
        )
        .mappings()
        .one_or_none()
    )
    return not (
        chunk is not None
        and UUID(str(chunk["artifact_version_id"])) == version_id
        and quote_end <= len(str(chunk["text"]))
        and str(chunk["text"])[quote_start:quote_end] == source_quote
        and hashlib.sha256(source_quote.encode("utf-8")).hexdigest() == quote_hash
    )


async def _artifact_version_is_superseded(
    session: AsyncSession, *, group_id: str, version_id: UUID
) -> bool:
    return bool(
        await session.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM artifact_versions current "
                "JOIN artifacts a ON a.id = current.artifact_id "
                "JOIN knowledge_sources s ON s.id = a.source_id "
                "LEFT JOIN projects p ON p.id = s.project_id "
                "WHERE current.id = :v "
                "AND EXISTS (SELECT 1 FROM artifact_versions newer "
                "WHERE newer.predecessor_version_id = current.id) "
                "AND (p.group_id = :g OR EXISTS (SELECT 1 FROM candidate_claims c "
                "WHERE c.artifact_version_id = current.id AND c.group_id = :g)))"
            ),
            {"g": group_id, "v": str(version_id)},
        )
    )


class LanePool:
    def __init__(
        self,
        container: Container,
        *,
        lanes: int,
        queue_maxsize: int,
        backoff_base_s: float = 1.0,
        backoff_cap_s: float = 60.0,
    ) -> None:
        self._container = container
        self._lanes = lanes
        self._queues: list[asyncio.Queue[QueuedJob]] = [
            asyncio.Queue(maxsize=queue_maxsize) for _ in range(lanes)
        ]
        self._workers: list[asyncio.Task[None]] = []
        self._backoff_base_s = backoff_base_s
        self._backoff_cap_s = backoff_cap_s
        self._resolver = SemanticEntityResolver(
            container.embedder,
            threshold=container.settings.memory.semantic_dedup_threshold,
            block_threshold=container.settings.memory.semantic_dedup_block_threshold,
            enabled=container.settings.memory.semantic_dedup_enabled,
            judge=container.entity_judge,
        )
        self._fabric_resolver = SemanticEntityResolver(None, enabled=False)

    def start(self) -> None:
        self._workers = [
            asyncio.create_task(self._run_lane(i), name=f"lane-{i}") for i in range(self._lanes)
        ]

    async def submit(self, job: QueuedJob) -> None:
        await self._queues[lane_for(str(job.group_id), self._lanes)].put(job)

    async def join(self) -> None:
        """Wait until every queued job has been processed."""
        await asyncio.gather(*(q.join() for q in self._queues))

    async def stop(self) -> None:
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []

    def _backoff(self, attempts: int) -> float:
        ceiling = min(self._backoff_cap_s, self._backoff_base_s * (2**attempts))
        return random.uniform(0, ceiling)  # noqa: S311  jitter for retry spacing

    async def _run_lane(self, index: int) -> None:
        queue = self._queues[index]
        while True:
            job = await queue.get()
            try:
                await self._process(job)
            except Exception as exc:
                clear_log_context()
                record_ingestion(result="failed", duration_s=0.0)
                retry_in = self._backoff(job.attempts)
                await self._container.queue.fail(job.id, error=str(exc), retry_in_s=retry_in)
                log.warning(
                    "ingest.failed", job_id=str(job.id), lane=index, retry_in_s=round(retry_in, 2)
                )
            finally:
                queue.task_done()

    async def _process(self, job: QueuedJob) -> None:
        # A retraction/erasure cleanup job carries the edge uuids and S3 keys to remove; it
        # is the durable safety net for RetractionService (see persistence/retraction.py).
        if job.payload.get("job_kind") == "retract_cleanup":
            await self._process_retract_cleanup(job)
            return
        if job.payload.get("job_kind") == "project_facts":
            await self._process_project_facts(job)
            return
        if job.payload.get("job_kind") == "embed_facts":
            async with asyncio.timeout(self._container.settings.resilience.per_episode_timeout_s):
                await self._process_embed_facts(job)
            return
        if job.payload.get("job_kind") == "ingest_graph":
            await self._process_graph_ingest(job)
            return
        bind_log_context(
            group_id=str(job.group_id),
            source_id=str(job.source_id),
            **_correlation(job.trace_context),
        )
        # Attribute any provider tokens spent during this ingest to the episode.
        usage_token = set_usage_context(
            UsageContext(request_kind="ingest", group_id=str(job.group_id), ref=str(job.source_id))
        )
        started = time.perf_counter()
        episode_budget = self._container.settings.resilience.per_episode_timeout_s
        episode_uuid: str | None = None
        try:
            # A per-episode deadline bounds a hung provider call: on timeout the job
            # errors, the lane is freed, and the queue retries it (not left pinned).
            with span("ingest.job", group_id=str(job.group_id)):
                async with asyncio.timeout(episode_budget):
                    write_mode = self._container.settings.memory.effective_fabric_write_mode
                    async with self._container.workers() as session, session.begin():
                        await session.execute(_GROUP_LOCK, {"g": str(job.group_id)})
                        if await session.scalar(_JOB_IS_INFLIGHT, {"job_id": job.id}) is None:
                            await session.execute(_MARK_DONE, {"id": job.id})
                            log.info("ingest.skipped_retracted", source_id=str(job.source_id))
                            return
                        fabric_meta = cast("dict[str, Any]", job.payload.get("_fabric") or {})
                        triples = cast("list[dict[str, Any]]", job.payload.get("triples") or [])
                        needs_review = bool(fabric_meta.get("needs_review"))
                        for triple in triples:
                            needs_review = needs_review or await _triple_needs_review(
                                session,
                                group_id=str(job.group_id),
                                meta=fabric_meta,
                                triple=triple,
                            )
                        reconcile_only = job.payload.get("job_kind") == "fabric_reconcile_version"
                        if write_mode != "legacy":
                            await self._reconcile_to_fabric(session, job)
                            if not needs_review and not reconcile_only and write_mode != "fabric":
                                await self._enqueue_graph_ingest(session, job)
                            await session.execute(_MARK_DONE, {"id": job.id})
                    if write_mode == "legacy":
                        if not needs_review and not reconcile_only:
                            episode_uuid = await self._ingest_graph(job)
                            if episode_uuid is None:
                                return
                        async with self._container.workers() as session, session.begin():
                            await session.execute(_MARK_DONE, {"id": job.id})
            record_ingestion(result="done", duration_s=time.perf_counter() - started)
            log.info("ingest.done", episode_uuid=episode_uuid)
        finally:
            reset_usage_context(usage_token)
            clear_log_context()

    async def _enqueue_graph_ingest(self, session: AsyncSession, job: QueuedJob) -> None:
        payload = dict(job.payload)
        payload["job_kind"] = "ingest_graph"
        await SqlAlchemyOutboxRepository(session).add(
            group_id=str(job.group_id),
            source_id=str(job.source_id),
            dedup_uuid=deterministic_id(f"graph:{job.source_id}"),
            payload=payload,
            trace_context=job.trace_context,
        )

    async def _process_graph_ingest(self, job: QueuedJob) -> None:
        bind_log_context(
            group_id=str(job.group_id),
            source_id=str(job.source_id),
            **_correlation(job.trace_context),
        )
        usage_token = set_usage_context(
            UsageContext(request_kind="ingest", group_id=str(job.group_id), ref=str(job.source_id))
        )
        started = time.perf_counter()
        try:
            with span("ingest.graph", group_id=str(job.group_id)):
                async with asyncio.timeout(
                    self._container.settings.resilience.per_episode_timeout_s
                ):
                    episode_uuid = await self._ingest_graph(job)
                    if episode_uuid is None:
                        return
                    async with self._container.workers() as session, session.begin():
                        await session.execute(_MARK_DONE, {"id": job.id})
            record_ingestion(result="done", duration_s=time.perf_counter() - started)
            log.info("ingest_graph.done", episode_uuid=episode_uuid)
        finally:
            reset_usage_context(usage_token)
            clear_log_context()

    async def _ingest_graph(self, job: QueuedJob) -> str | None:
        group = str(job.group_id)
        async with self._container.workers() as session, session.begin():
            await session.execute(_GROUP_LOCK, {"g": group})
            if await session.scalar(_JOB_IS_INFLIGHT, {"job_id": job.id}) is None:
                log.info("ingest_graph.skipped_retracted", source_id=str(job.source_id))
                return None
            published_reference_time = await session.scalar(
                _REFERENCE_TIME_BY_SOURCE,
                {"group_id": group, "source_id": str(job.source_id)},
            )
            model_name, dim = active_embedding(self._container.settings)
            await SqlAlchemyEmbeddingStateRepository(session).ensure_compatible(
                group_id=group, model=model_name, dim=dim
            )
        episode = EpisodeSpec(
            source_id=SourceId(str(job.source_id)),
            group_id=GroupId(group),
            body=str(job.payload.get("body", "")),
            reference_time=published_reference_time or job.created_at,
            metadata=job.payload,
        )
        receipt = await self._container.memory.ingest_episode(episode)
        async with self._container.workers() as session, session.begin():
            await session.execute(_GROUP_LOCK, {"g": group})
            await self._stitch(session, group, str(job.source_id), receipt)
        return str(receipt.episode_uuid)

    async def _reconcile_to_fabric(self, session: AsyncSession, job: QueuedJob) -> None:
        """Populate the authoritative fact store from the same triples, so the /v2 knowledge
        surface reflects live ingest. Gated by the dual/fabric write modes. Idempotent on replay:
        an episode already reconciled (by its source run key) is skipped. The real trust,
        authority, confidence, ontology version, and artifact/version provenance come from the
        publish path (the ``_fabric`` block); nothing is assumed authoritative. A meta-less job
        defaults to the safest (unverified) tier, never to authoritative.
        """
        triples = cast("list[dict[str, Any]]", job.payload.get("triples") or [])
        reconcile_only = job.payload.get("job_kind") == "fabric_reconcile_version"
        if not triples and not reconcile_only:
            return
        group = str(job.group_id)
        meta = cast("dict[str, Any]", job.payload.get("_fabric") or {})
        extraction_run_id = (
            UUID(str(meta["extraction_run_id"])) if meta.get("extraction_run_id") else None
        )
        run_key = f"episode:{job.source_id}"
        already = await session.scalar(
            text("SELECT 1 FROM assertions WHERE group_id = :g AND run_key = :r LIMIT 1"),
            {"g": group, "r": run_key},
        )
        if already:
            await self._enqueue_fact_embeddings(session, group, str(job.source_id))
            return

        trust_tier = int(meta.get("trust_tier", int(TrustTier.UNVERIFIED)))
        human_verified = meta.get("verification") == "human_verified"
        source_authority = float(meta.get("authority", authority_for_tier(trust_tier)))
        confidence = float(meta.get("confidence", 0.5))
        ontology_version_id = (
            UUID(str(meta["ontology_version_id"])) if meta.get("ontology_version_id") else None
        )
        version_id = (
            UUID(str(meta["artifact_version_id"])) if meta.get("artifact_version_id") else None
        )
        if version_id is not None and await _artifact_version_is_superseded(
            session, group_id=group, version_id=version_id
        ):
            return
        # The artifact id lets reconciliation withdraw the previous version's assertions when a
        # new version of the same artifact drops a proposition (live update path).
        artifact_id: UUID | None = None
        knowledge_source_id: UUID | None = None
        valid_from: datetime | None = None
        if version_id is not None:
            found = (
                await session.execute(
                    text(
                        "SELECT av.artifact_id, av.reference_time, a.source_id "
                        "FROM artifact_versions av "
                        "JOIN artifacts a ON a.id = av.artifact_id WHERE av.id = :v"
                    ),
                    {"v": str(version_id)},
                )
            ).first()
            if found is not None:
                artifact_id = UUID(str(found.artifact_id))
                knowledge_source_id = UUID(str(found.source_id))
                valid_from = found.reference_time

        canonical = SqlAlchemyCanonicalEntityRepository(session)
        propositions: list[ResolvedProposition] = []
        for triple in triples:
            subject = str(triple.get("subject", "")).strip()
            predicate = str(triple.get("predicate", "")).strip()
            obj = str(triple.get("object", "")).strip()
            if not (subject and predicate and obj):
                continue
            source_quote = triple.get("source_quote")
            quote_start = triple.get("quote_start")
            quote_end = triple.get("quote_end")
            subject_entity_type = str(triple.get("entity_type") or "Entity")
            object_entity_type = str(triple["object_type"]) if triple.get("object_type") else None
            qualifier_value = triple.get("qualifiers")
            qualifiers = (
                cast("JsonDict", qualifier_value) if isinstance(qualifier_value, dict) else {}
            )
            chunk_id = UUID(str(meta["chunk_id"])) if meta.get("chunk_id") else None
            quote_hash = str(meta["quote_hash"]) if meta.get("quote_hash") else None
            needs_review = await _triple_needs_review(
                session,
                group_id=group,
                meta=meta,
                triple=triple,
            )
            entity = await self._fabric_resolver.resolve_or_create(
                canonical,
                group_id=group,
                name=subject,
                entity_type=subject_entity_type,
            )
            # An edge predicate relates two entities, so resolve the object to a canonical
            # entity and record the object side of the graph edge; scalar attributes stay scalar.
            object_entity_id: UUID | None = None
            object_scalar: str | None = obj
            if is_edge_predicate(predicate):
                object_entity = await self._fabric_resolver.resolve_or_create(
                    canonical,
                    group_id=group,
                    name=obj,
                    entity_type=object_entity_type or "Entity",
                )
                object_entity_id = object_entity.id
                object_scalar = None
            propositions.append(
                ResolvedProposition(
                    subject_entity_id=entity.id,
                    predicate=predicate,
                    object_entity_id=object_entity_id,
                    object_scalar=object_scalar,
                    subject_entity_type=subject_entity_type,
                    object_entity_type=object_entity_type,
                    qualifiers=qualifiers,
                    extractor_confidence=confidence,
                    chunk_id=chunk_id,
                    excerpt=source_quote if isinstance(source_quote, str) else None,
                    quote_start=(quote_start if isinstance(quote_start, int) else None),
                    quote_end=quote_end if isinstance(quote_end, int) else None,
                    evidence_content_hash=quote_hash,
                    structured_record=dict(triple) if chunk_id is None else None,
                    needs_review=needs_review,
                )
            )
        if not propositions and not reconcile_only:
            return
        service = ReconciliationService(
            facts=SqlAlchemyFactRepository(session),
            assertions=SqlAlchemyAssertionRepository(session),
            evidence=SqlAlchemyEvidenceRepository(session),
            relations=SqlAlchemyFactRelationRepository(session),
            events=SqlAlchemyKnowledgeEventLog(session),
        )
        await service.reconcile(
            ArtifactReconciliation(
                group_id=group,
                source_authority=source_authority,
                trust_tier=trust_tier,
                propositions=propositions,
                human_verified=human_verified,
                valid_from=valid_from,
                artifact_version_id=version_id,
                knowledge_source_id=knowledge_source_id,
                artifact_id=artifact_id,
                ontology_version_id=ontology_version_id,
                extraction_run_id=extraction_run_id,
                run_key=run_key,
                actor="worker",
            )
        )
        await self._enqueue_fact_embeddings(session, group, str(job.source_id))
        await self._enqueue_fact_projection(session, group, str(job.source_id))

    async def _enqueue_fact_embeddings(
        self, session: AsyncSession, group: str, source_id: str
    ) -> None:
        if (
            not self._container.settings.memory.vector_search_enabled
            or self._container.embedder is None
        ):
            return
        pending = await session.scalar(
            text(
                "SELECT 1 FROM ingestion_jobs WHERE group_id = :g "
                "AND status IN ('pending','inflight') "
                "AND payload->>'job_kind' = 'embed_facts' LIMIT 1"
            ),
            {"g": group},
        )
        if pending:
            return
        await SqlAlchemyOutboxRepository(session).add(
            group_id=group,
            source_id=source_id,
            dedup_uuid=uuid7(),
            payload={"job_kind": "embed_facts", "group_id": group},
        )

    async def _enqueue_fact_projection(
        self, session: AsyncSession, group: str, source_id: str
    ) -> None:
        """Enqueue an outbox job to project this group's facts into the graph, so graph mutation
        is downstream of the fact store (ADR-0003) rather than synchronous with reconciliation.
        Only when a graph is configured, and coalesced: while a projection job for the group is
        pending or in flight, no second one is added (project_group is whole-group idempotent).
        """
        if self._container.fact_projection is None:
            return
        pending = await session.scalar(
            text(
                "SELECT 1 FROM ingestion_jobs WHERE group_id = :g "
                "AND status IN ('pending','inflight') "
                "AND payload->>'job_kind' = 'project_facts' LIMIT 1"
            ),
            {"g": group},
        )
        if pending:
            return
        await SqlAlchemyOutboxRepository(session).add(
            group_id=group,
            source_id=source_id,
            dedup_uuid=uuid7(),
            payload={"job_kind": "project_facts", "group_id": group},
        )

    async def _process_project_facts(self, job: QueuedJob) -> None:
        """Project the group's active facts into the graph (RELATES_TO edges), then mark done.
        Idempotent and rebuildable: it upserts the current active fact set, so a retry or a
        coalesced burst converges to the same projection.
        """
        group = str(job.group_id)
        projection = self._container.fact_projection
        if projection is not None:
            # The source reads active facts from Postgres (worker role) and the projection
            # writes them to the graph; lane routing already serializes the group.
            service = FactProjectionService(
                source=SqlAlchemyProjectionSource(self._container.workers), projection=projection
            )
            projected = await service.project_group(group)
            log.info("project_facts.done", group_id=group, projected=projected)
        async with self._container.workers() as session, session.begin():
            await session.execute(_MARK_DONE, {"id": job.id})

    async def _process_embed_facts(self, job: QueuedJob) -> None:
        group = str(job.group_id)
        embedder = self._container.embedder
        if not self._container.settings.memory.vector_search_enabled or embedder is None:
            async with self._container.workers() as session, session.begin():
                await session.execute(_MARK_DONE, {"id": job.id})
            return
        memory = self._container.settings.memory
        model, dimension = active_embedding(self._container.settings)
        params: dict[str, object] = {
            "g": group,
            "provider": memory.embedder,
            "model": model,
            "model_version": memory.embedding_model_version,
            "dimension": dimension,
        }
        async with self._container.workers() as session:
            rows = (await session.execute(_FACTS_TO_EMBED, params)).mappings().all()
        usage_token = set_usage_context(
            UsageContext(request_kind="ingest", group_id=group, ref=str(job.source_id))
        )
        embedded = 0
        try:
            for row in rows:
                fact_text = fact_semantic_text(
                    subject_name=str(row["subject_name"]),
                    predicate=str(row["predicate"]),
                    object_name=str(row["object_name"]),
                    object_type=str(row["object_type"]),
                    qualifiers=cast("JsonDict", row["qualifiers"] or {}),
                )
                content_hash = hashlib.sha256(fact_text.encode()).hexdigest()
                if row["content_hash"] == content_hash and bool(row["active"]):
                    continue
                vector = await embedder.embed(fact_text)
                if len(vector) != dimension:
                    raise ValueError(
                        f"embedding dimension mismatch: expected {dimension}, got {len(vector)}"
                    )
                async with self._container.workers() as session, session.begin():
                    await session.execute(_GROUP_LOCK, {"g": group})
                    await SqlAlchemyFactEmbeddingRepository(session).upsert(
                        FactEmbedding(
                            id=uuid7(),
                            group_id=group,
                            fact_id=UUID(str(row["id"])),
                            provider=memory.embedder,
                            model=model,
                            model_version=memory.embedding_model_version,
                            dimension=dimension,
                            embedding=vector,
                            content_hash=content_hash,
                            created_at=utc_now(),
                        )
                    )
                embedded += 1
        finally:
            reset_usage_context(usage_token)
        async with self._container.workers() as session, session.begin():
            await session.execute(_MARK_DONE, {"id": job.id})
        log.info("embed_facts.done", group_id=group, embedded=embedded)

    async def _process_retract_cleanup(self, job: QueuedJob) -> None:
        # Idempotent: removing already-removed edges is a no-op and deleting an absent object
        # is a no-op, so a retry (or running after the in-process cleanup already ran) is safe.
        bind_log_context(
            group_id=str(job.group_id),
            source_id=str(job.source_id),
            **_correlation(job.trace_context),
        )
        edge_uuids = [str(u) for u in job.payload.get("edge_uuids", [])]
        s3_keys = [str(k) for k in job.payload.get("s3_keys", [])]
        erase = bool(job.payload.get("erase", False))
        budget = self._container.settings.resilience.per_episode_timeout_s
        try:
            with span("retract.cleanup", group_id=str(job.group_id)):
                async with asyncio.timeout(budget):
                    if edge_uuids:
                        await self._container.memory.retract_episode(
                            group_id=str(job.group_id), edge_uuids=edge_uuids
                        )
                    if erase:
                        for key in s3_keys:
                            await self._container.object_store.delete(key=key)
                    async with self._container.workers() as session, session.begin():
                        await session.execute(_MARK_DONE, {"id": job.id})
            log.info(
                "retract.cleanup.done",
                group_id=str(job.group_id),
                source_id=str(job.source_id),
                edges=len(edge_uuids),
                erased=erase,
            )
        finally:
            clear_log_context()

    async def _stitch(
        self, session: AsyncSession, group_id: str, source_id: str, receipt: IngestReceipt
    ) -> None:
        # Map each graph node to a canonical entity (resolve or create) and record the
        # node and edge uuids against the published episode this job came from. The
        # worker runs as a trusted role, so RLS is bypassed; the per-group advisory lock
        # already serializes canonical writes for the group.
        if not receipt.nodes and not receipt.edge_uuids:
            return
        episode_id = await session.scalar(
            _EPISODE_BY_SOURCE, {"group_id": group_id, "source_id": source_id}
        )
        canonical = SqlAlchemyCanonicalEntityRepository(session)
        graph_map = SqlAlchemyGraphMapRepository(session)
        for node in receipt.nodes:
            entity = await self._resolver.resolve_or_create(
                canonical, group_id=group_id, name=node.name, entity_type=node.entity_type
            )
            await graph_map.record_node(
                group_id=group_id,
                node_uuid=UUID(node.uuid),
                canonical_entity_id=entity.id,
                published_episode_id=episode_id,
            )
        for edge_uuid in receipt.edge_uuids:
            await graph_map.record_edge(
                group_id=group_id, edge_uuid=UUID(edge_uuid), published_episode_id=episode_id
            )
