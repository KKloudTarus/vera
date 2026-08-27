"""Guard a group's embedding fingerprint so its vectors stay one dimension.

A group's graph vectors must all come from the same embedding model and dimension, or
cosine similarity across them is meaningless. This compares the model/dimension a group
was first built with against the current configuration: a match (or a fresh group) is
fine, a change is refused so the operator reprocesses the group to re-embed it rather
than silently mixing dimensions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from vera.shared.errors import VeraError


@dataclass(frozen=True, slots=True)
class EmbeddingFingerprint:
    model: str
    dim: int


def reconcile(
    existing: EmbeddingFingerprint | None, current: EmbeddingFingerprint
) -> Literal["ok", "initialize"]:
    """Return "initialize" for a fresh group (caller records the fingerprint), "ok" when
    it matches, and raise ``VeraError`` when the group was built under a different model or
    dimension (reprocess it to re-embed under the new configuration).
    """
    if existing is None:
        return "initialize"
    if existing == current:
        return "ok"
    raise VeraError(
        f"group embedded with {existing.model}@{existing.dim}, "
        f"configuration is {current.model}@{current.dim}; reprocess the group to re-embed it"
    )
