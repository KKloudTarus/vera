"""S3ObjectStore against the live MinIO (compose). Skips if MinIO is unreachable."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from vera.adapters.objectstore.s3_adapter import S3ObjectStore
from vera.config.settings import ObjectStoreSettings
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _store() -> S3ObjectStore:
    return S3ObjectStore(
        ObjectStoreSettings(
            endpoint_url="http://localhost:9000",
            bucket=f"vera-test-{uuid7().hex[:8]}",
            access_key=SecretStr("minioadmin"),
            secret_key=SecretStr("minioadmin"),
        )
    )


async def test_put_get_roundtrip_and_presign() -> None:
    store = _store()
    key = f"artifacts/{uuid7().hex}/v1"
    payload = b"paymentapi runs on prod-eks"
    try:
        stored = await store.put(key=key, data=payload, content_type="text/plain")
    except Exception as exc:  # MinIO not reachable
        pytest.skip(f"object store not reachable: {exc}")

    assert stored.key == key
    assert stored.size == len(payload)
    assert stored.content_hash.startswith("sha256:")

    fetched = await store.get(key=key)
    assert fetched == payload

    url = await store.presigned_url(key=key)
    assert key in url and url.startswith("http")


async def test_delete_is_idempotent() -> None:
    store = _store()
    key = f"artifacts/{uuid7().hex}/v1"
    try:
        await store.put(key=key, data=b"to be erased", content_type="text/plain")
    except Exception as exc:  # MinIO not reachable
        pytest.skip(f"object store not reachable: {exc}")

    await store.delete(key=key)
    # The object is gone.
    with pytest.raises(Exception):  # noqa: B017  any not-found error is acceptable
        await store.get(key=key)
    # Deleting again is a no-op, not an error (idempotent erasure).
    await store.delete(key=key)
