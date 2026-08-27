"""Identity value objects.

Wrapping ``group_id`` / ``source_id`` / ``canonical_entity_id`` in typed value
objects (instead of passing bare ``str``) makes illegal states unrepresentable
and prevents the classic "wrong tenant's id in the wrong slot" bug.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias

JsonDict: TypeAlias = dict[str, Any]
"""An arbitrary JSON object. Values are Any because payloads are producer-defined."""


def empty_json() -> JsonDict:
    """Typed default factory for a ``JsonDict`` field."""
    return {}


@dataclass(frozen=True, slots=True)
class _StrId:
    """Base: a non-empty, stripped string identifier."""

    value: str

    def __post_init__(self) -> None:
        if not self.value or not self.value.strip():
            raise ValueError(f"{type(self).__name__} must be a non-empty string")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class GroupId(_StrId):
    """Tenancy/graph partition key. The one thing a client may never choose itself."""


@dataclass(frozen=True, slots=True)
class SourceId(_StrId):
    """Stable external identifier of a source record, e.g. ``confluence:page:128172:v17``."""


@dataclass(frozen=True, slots=True)
class CanonicalEntityId(_StrId):
    """VERA-owned durable identity that stitches graph fragments across scopes."""
