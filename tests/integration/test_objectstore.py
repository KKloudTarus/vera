"""S3ObjectStore against the live MinIO Compose service."""

from __future__ import annotations

import pytest

from vera.adapters.objectstore.s3_adapter import S3ObjectStore
from vera.config.settings import get_settings
from vera.shared.ids import uuid7

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _store() -> S3ObjectStore:
    settings = get_settings().objectstore.model_copy(
        update={"bucket": f"vera-test-{uuid7().hex[:8]}"}
    )
    return S3ObjectStore(settings)


async def test_put_get_roundtrip_and_presign() -> None:
    store = _store()
    key = f"artifacts/{uuid7().hex}/v1"
    payload = b"paymentapi runs on prod-eks"
    stored = await store.put(key=key, data=payload, content_type="text/plain")

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
    await store.put(key=key, data=b"to be erased", content_type="text/plain")

    await store.delete(key=key)
    # The object is gone.
    with pytest.raises(Exception):  # noqa: B017  any not-found error is acceptable
        await store.get(key=key)
    # Deleting again is a no-op, not an error (idempotent erasure).
    await store.delete(key=key)
