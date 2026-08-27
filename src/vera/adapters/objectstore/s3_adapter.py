"""S3ObjectStore: raw artifact storage over the S3-compatible API.

Targets the S3 API only (works with MinIO, Ceph, or any S3-compatible endpoint), never
an AWS-proprietary feature, so it stays cloud-portable. The bucket is created on first
write if it does not exist. Content is addressed by the caller's key; the stored object's
sha256 is returned so callers can verify integrity.
"""

from __future__ import annotations

import hashlib
from typing import Any

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from vera.config.settings import ObjectStoreSettings
from vera.domain.ports.object_store import StoredObject
from vera.shared.errors import InfrastructureError


class S3ObjectStore:
    """Concrete ``ObjectStore`` backed by an S3-compatible endpoint."""

    def __init__(self, settings: ObjectStoreSettings) -> None:
        self._settings = settings
        self._session: Any = aioboto3.Session()  # aioboto3 ships no type stubs
        self._bucket_ready = False

    def _client(self) -> Any:
        access = self._settings.access_key.get_secret_value() if self._settings.access_key else None
        secret = self._settings.secret_key.get_secret_value() if self._settings.secret_key else None
        return self._session.client(
            "s3",
            endpoint_url=self._settings.endpoint_url,
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            region_name=self._settings.region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    async def _ensure_bucket(self, client: Any) -> None:
        if self._bucket_ready:
            return
        try:
            await client.head_bucket(Bucket=self._settings.bucket)
        except ClientError:
            try:
                await client.create_bucket(Bucket=self._settings.bucket)
            except ClientError as exc:  # a concurrent create is fine
                if "BucketAlreadyOwnedByYou" not in str(exc) and "BucketAlreadyExists" not in str(
                    exc
                ):
                    raise InfrastructureError(f"cannot ensure bucket: {exc}") from exc
        self._bucket_ready = True

    async def put(self, *, key: str, data: bytes, content_type: str) -> StoredObject:
        content_hash = "sha256:" + hashlib.sha256(data).hexdigest()
        async with self._client() as client:
            await self._ensure_bucket(client)
            await client.put_object(
                Bucket=self._settings.bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
                Metadata={"content-hash": content_hash},
            )
        return StoredObject(key=key, size=len(data), content_hash=content_hash)

    async def get(self, *, key: str) -> bytes:
        async with self._client() as client:
            try:
                response = await client.get_object(Bucket=self._settings.bucket, Key=key)
                async with response["Body"] as stream:
                    return await stream.read()
            except ClientError as exc:
                raise InfrastructureError(f"object not found: {key}") from exc

    async def presigned_url(self, *, key: str, expires_in_s: int = 3600) -> str:
        async with self._client() as client:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._settings.bucket, "Key": key},
                ExpiresIn=expires_in_s,
            )

    async def delete(self, *, key: str) -> None:
        # S3 delete_object is idempotent: deleting an absent key returns success.
        async with self._client() as client:
            await client.delete_object(Bucket=self._settings.bucket, Key=key)
