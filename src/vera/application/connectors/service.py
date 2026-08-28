"""SyncRunner: pull a source's changes and feed them through curation.

For each changed record the runner ingests an artifact in its own transaction, so one
bad record does not roll back the batch. Ingestion is content-idempotent, so a record
seen again unchanged is a no-op. After the batch it persists the connector's next
cursor, which is what makes the following run incremental.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from vera.application.curation.service import CurationService, IngestArtifact
from vera.domain.ports.connectors import SourceConnector, SyncOutcome, SyncStateStore
from vera.domain.ports.curation import ClaimExtractor, ContradictionJudge
from vera.domain.ports.embedder import Embedder
from vera.domain.ports.object_store import ObjectStore
from vera.domain.ports.unit_of_work import UnitOfWork
from vera.shared.errors import is_ok

UnitOfWorkFactory = Callable[[], UnitOfWork]

# A safety bound so a misbehaving connector that always reports has_more cannot loop forever.
_MAX_PAGES_PER_SYNC = 10_000


class SyncRunner:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        extractor: ClaimExtractor,
        state: SyncStateStore,
        object_store: ObjectStore | None = None,
        judge: ContradictionJudge | None = None,
        embedder: Embedder | None = None,
        embedding_provider: str = "unknown",
        embedding_model: str = "unknown",
        embedding_model_version: str = "1",
        embedding_dimension: int | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._extractor = extractor
        self._state = state
        self._object_store = object_store
        self._judge = judge
        self._embedder = embedder
        self._embedding_provider = embedding_provider
        self._embedding_model = embedding_model
        self._embedding_model_version = embedding_model_version
        self._embedding_dimension = embedding_dimension

    async def sync(
        self, *, source_id: UUID, group_id: str, connector: SourceConnector
    ) -> SyncOutcome:
        cursor = await self._state.get_cursor(source_id)
        job_id = await self._state.start_job(source_id)
        try:
            processed = 0
            unchanged = 0
            pages = 0
            # Drain the connector's pagination: keep fetching while it reports more, following
            # the cursor it returns each time, so a run that spans many API pages loses nothing.
            while True:
                batch = await connector.fetch_changes(cursor)
                for record in batch.records:
                    async with self._uow_factory() as uow:
                        await uow.use_tenant(group_id)
                        result = await CurationService(
                            uow,
                            self._extractor,
                            object_store=self._object_store,
                            judge=self._judge,
                            embedder=self._embedder,
                            embedding_provider=self._embedding_provider,
                            embedding_model=self._embedding_model,
                            embedding_model_version=self._embedding_model_version,
                            embedding_dimension=self._embedding_dimension,
                        ).ingest_artifact(
                            IngestArtifact(
                                source_id=source_id,
                                group_id=group_id,
                                external_id=record.external_id,
                                body=record.body,
                                knowledge_type=record.knowledge_type,
                                title=record.title,
                                metadata=record.metadata,
                                reference_time=record.reference_time,
                                source_revision=record.source_revision,
                                source_updated_at=record.source_updated_at,
                                source_version_id=record.source_version_id,
                            )
                        )
                        await uow.commit()
                    if is_ok(result) and result.value.action in {"unchanged", "stale"}:
                        unchanged += 1
                    else:
                        processed += 1
                cursor = batch.next_cursor
                pages += 1
                if not batch.has_more or pages >= _MAX_PAGES_PER_SYNC:
                    break
            await self._state.save_cursor(source_id, cursor)
            await self._state.finish_job(job_id, processed=processed, unchanged=unchanged)
            return SyncOutcome(processed=processed, unchanged=unchanged, cursor=cursor)
        except Exception as exc:
            await self._state.fail_job(job_id, error=str(exc))
            raise
