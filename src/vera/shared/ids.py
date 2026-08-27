"""Identifier helpers. Two distinct axes that must never be mixed.

* :func:`uuid7` gives a time-ordered id (RFC 9562) for internal surrogate keys
  minted in the app, keeping the B-tree locality that random UUIDv4 loses. Prefer
  PostgreSQL 18's native ``uuidv7()`` server-side default where possible.
* :func:`deterministic_id` gives a UUIDv5 over a fixed namespace for idempotency.
  The same logical input always yields the same id, so retries de-duplicate and a
  full rebuild from source converges to the same graph.
"""

from __future__ import annotations

import secrets
import time
import uuid

# Fixed, checked-in namespace so deterministic ids are stable across deployments.
VERA_NAMESPACE = uuid.UUID("6f1c9a2e-8b47-5d3a-9e11-0c7a2f4b6d80")


def uuid7() -> uuid.UUID:
    """Generate a time-ordered UUIDv7 (RFC 9562)."""
    unix_ms = int(time.time() * 1000) & 0xFFFFFFFFFFFF
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = unix_ms << 80
    value |= 0x7 << 76  # version 7
    value |= rand_a << 64
    value |= 0b10 << 62  # RFC 4122 variant
    value |= rand_b
    return uuid.UUID(int=value)


def deterministic_id(*parts: str) -> uuid.UUID:
    """UUIDv5 idempotency key from stable parts, e.g. ``deterministic_id(source_id)``.

    Joins parts with ``\\x1f`` (unit separator) so ``("a", "bc")`` and ``("ab", "c")``
    never collide.
    """
    if not parts:
        raise ValueError("deterministic_id requires at least one part")
    name = "\x1f".join(parts)
    return uuid.uuid5(VERA_NAMESPACE, name)
