"""S3ObjectStore: raw artifact storage over the S3-compatible API.

Targets the S3 API only (works with MinIO/Ceph/etc.), never AWS-proprietary
features, so it stays cloud-portable. Skeleton: install extra 'objectstore'.
"""

from __future__ import annotations

from vera.config.settings import ObjectStoreSettings
from vera.domain.ports.object_store import StoredObject

_NOT_WIRED = "S3ObjectStore is not wired yet. Install extra 'objectstore' and implement it."


class S3ObjectStore:
    """Concrete ``ObjectStore`` backed by an S3-compatible endpoint."""

    def __init__(self, settings: ObjectStoreSettings) -> None:
        self._settings = settings

    async def put(self, *, key: str, data: bytes, content_type: str) -> StoredObject:
        raise NotImplementedError(_NOT_WIRED)

    async def get(self, *, key: str) -> bytes:
        raise NotImplementedError(_NOT_WIRED)

    async def presigned_url(self, *, key: str, expires_in_s: int = 3600) -> str:
        raise NotImplementedError(_NOT_WIRED)
