"""The ``ObjectStore`` port for raw artifact storage on an S3-compatible API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class StoredObject:
    key: str
    size: int
    content_hash: str


class ObjectStore(Protocol):
    async def put(self, *, key: str, data: bytes, content_type: str) -> StoredObject: ...

    async def get(self, *, key: str) -> bytes: ...

    async def presigned_url(self, *, key: str, expires_in_s: int = 3600) -> str: ...

    async def delete(self, *, key: str) -> None:
        """Delete an object. Idempotent: deleting a missing key is not an error."""
        ...
