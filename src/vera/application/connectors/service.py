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
from vera.domain.ports.curation import ClaimExtractor
from vera.domain.ports.object_store import ObjectStore
from vera.domain.ports.unit_of_work import UnitOfWork
from vera.shared.errors import is_ok

UnitOfWorkFactory = Callable[[], UnitOfWork]


class SyncRunner:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        extractor: ClaimExtractor,
        state: SyncStateStore,
        object_store: ObjectStore | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._extractor = extractor
        self._state = state
        self._object_store = object_store

    async def sync(
        self, *, source_id: UUID, group_id: str, connector: SourceConnector
    ) -> SyncOutcome:
        cursor = await self._state.get_cursor(source_id)
        job_id = await self._state.start_job(source_id)
        try:
            batch = await connector.fetch_changes(cursor)
            processed = 0
            unchanged = 0
            for record in batch.records:
                async with self._uow_factory() as uow:
                    await uow.use_tenant(group_id)
                    result = await CurationService(
                        uow, self._extractor, self._object_store
                    ).ingest_artifact(
                        IngestArtifact(
                            source_id=source_id,
                            group_id=group_id,
                            external_id=record.external_id,
                            body=record.body,
                            knowledge_type=record.knowledge_type,
                            title=record.title,
                            metadata=record.metadata,
                        )
                    )
                    await uow.commit()
                if is_ok(result) and result.value.action == "unchanged":
                    unchanged += 1
                else:
                    processed += 1
            await self._state.save_cursor(source_id, batch.next_cursor)
            await self._state.finish_job(job_id, processed=processed, unchanged=unchanged)
            return SyncOutcome(processed=processed, unchanged=unchanged, cursor=batch.next_cursor)
        except Exception as exc:
            await self._state.fail_job(job_id, error=str(exc))
            raise
