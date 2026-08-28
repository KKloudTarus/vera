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
import random
import time
import zlib
from typing import Any, cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from vera.adapters.persistence.repositories import (
    SqlAlchemyAssertionRepository,
    SqlAlchemyCanonicalEntityRepository,
    SqlAlchemyEvidenceRepository,
    SqlAlchemyFactRelationRepository,
    SqlAlchemyFactRepository,
    SqlAlchemyGraphMapRepository,
    SqlAlchemyKnowledgeEventLog,
)
from vera.adapters.persistence.repositories.embedding_state import (
    SqlAlchemyEmbeddingStateRepository,
)
from vera.application.curation.entity_resolver import SemanticEntityResolver
from vera.application.curation.reconciliation import (
    ArtifactReconciliation,
    ReconciliationService,
    ResolvedProposition,
)
from vera.bootstrap import Container
from vera.config.settings import active_embedding
from vera.domain.curation.trust import TrustTier, authority_for_tier
from vera.domain.ports.job_queue import QueuedJob
from vera.domain.ports.memory_engine import EpisodeSpec, IngestReceipt
from vera.observability import bind_log_context, clear_log_context, get_logger, span
from vera.observability.cost import UsageContext, reset_usage_context, set_usage_context
from vera.observability.metrics import record_ingestion
from vera.shared.time import utc_now
from vera.shared.types import GroupId, JsonDict, SourceId

log = get_logger(__name__)

_MARK_DONE = text("UPDATE ingestion_jobs SET status = 'done', last_error = NULL WHERE id = :id")
_GROUP_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended(:g, 0))")
_EPISODE_BY_SOURCE = text(
    "SELECT id FROM published_episodes WHERE group_id = :group_id AND source_id = :source_id"
)


def lane_for(group_id: str, lanes: int) -> int:
    """Stable, process-independent lane assignment (crc32 is not hash-salted)."""
    return zlib.crc32(group_id.encode("utf-8")) % lanes


def _correlation(trace_context: JsonDict) -> dict[str, str]:
    cid = trace_context.get("correlation_id")
    return {"correlation_id": str(cid)} if cid else {}


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
        try:
            # A per-episode deadline bounds a hung provider call: on timeout the job
            # errors, the lane is freed, and the queue retries it (not left pinned).
            with span("ingest.job", group_id=str(job.group_id)):
                async with asyncio.timeout(episode_budget):
                    async with self._container.sessionmaker() as session, session.begin():
                        await session.execute(_GROUP_LOCK, {"g": str(job.group_id)})
                        # One embedding dimension per group: refuse a write under a changed
                        # model/dim (job dead-letters with a clear message) until reprocess.
                        model_name, dim = active_embedding(self._container.settings)
                        await SqlAlchemyEmbeddingStateRepository(session).ensure_compatible(
                            group_id=str(job.group_id), model=model_name, dim=dim
                        )
                        episode = EpisodeSpec(
                            source_id=SourceId(str(job.source_id)),
                            group_id=GroupId(str(job.group_id)),
                            body=str(job.payload.get("body", "")),
                            reference_time=utc_now(),
                            metadata=job.payload,
                        )
                        receipt = await self._container.memory.ingest_episode(episode)
                        await self._stitch(session, str(job.group_id), str(job.source_id), receipt)
                        if self._container.settings.memory.fabric_enabled:
                            await self._reconcile_to_fabric(session, job)
                        await session.execute(_MARK_DONE, {"id": job.id})
            record_ingestion(result="done", duration_s=time.perf_counter() - started)
            log.info("ingest.done", episode_uuid=receipt.episode_uuid)
        finally:
            reset_usage_context(usage_token)
            clear_log_context()

    async def _reconcile_to_fabric(self, session: AsyncSession, job: QueuedJob) -> None:
        """Populate the authoritative fact store from the same triples, so the /v2 knowledge
        surface reflects live ingest. Gated by memory.fabric_enabled. Idempotent on replay:
        an episode already reconciled (by its extraction_run_id) is skipped. The real trust,
        authority, confidence, ontology version, and artifact/version provenance come from the
        publish path (the ``_fabric`` block); nothing is assumed authoritative. A meta-less job
        defaults to the safest (unverified) tier, never to authoritative.
        """
        triples = cast("list[dict[str, Any]]", job.payload.get("triples") or [])
        if not triples:
            return
        group = str(job.group_id)
        run_id = f"episode:{job.source_id}"
        already = await session.scalar(
            text("SELECT 1 FROM assertions WHERE group_id = :g AND extraction_run_id = :r LIMIT 1"),
            {"g": group, "r": run_id},
        )
        if already:
            return

        meta = cast("dict[str, Any]", job.payload.get("_fabric") or {})
        trust_tier = int(meta.get("trust_tier", int(TrustTier.UNVERIFIED)))
        source_authority = float(meta.get("authority", authority_for_tier(trust_tier)))
        confidence = float(meta.get("confidence", 0.5))
        ontology_version_id = (
            UUID(str(meta["ontology_version_id"])) if meta.get("ontology_version_id") else None
        )
        version_id = (
            UUID(str(meta["artifact_version_id"])) if meta.get("artifact_version_id") else None
        )
        # The artifact id lets reconciliation withdraw the previous version's assertions when a
        # new version of the same artifact drops a proposition (live update path).
        artifact_id: UUID | None = None
        if version_id is not None:
            found = await session.scalar(
                text("SELECT artifact_id FROM artifact_versions WHERE id = :v"),
                {"v": str(version_id)},
            )
            artifact_id = UUID(str(found)) if found is not None else None

        canonical = SqlAlchemyCanonicalEntityRepository(session)
        propositions: list[ResolvedProposition] = []
        for triple in triples:
            subject = str(triple.get("subject", "")).strip()
            predicate = str(triple.get("predicate", "")).strip()
            obj = str(triple.get("object", "")).strip()
            if not (subject and predicate and obj):
                continue
            entity = await self._resolver.resolve_or_create(
                canonical,
                group_id=group,
                name=subject,
                entity_type=str(triple.get("entity_type", "Entity")),
            )
            propositions.append(
                ResolvedProposition(
                    subject_entity_id=entity.id,
                    predicate=predicate,
                    object_scalar=obj,
                    extractor_confidence=confidence,
                    excerpt=f"{subject} {predicate} {obj}",
                )
            )
        if not propositions:
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
                artifact_version_id=version_id,
                artifact_id=artifact_id,
                ontology_version_id=ontology_version_id,
                extraction_run_id=run_id,
                actor="worker",
            )
        )

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
                    async with self._container.sessionmaker() as session, session.begin():
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
