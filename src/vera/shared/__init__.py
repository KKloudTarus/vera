"""Shared kernel: cross-context primitives usable by any layer.

Deliberately tiny and dependency-free (stdlib + pydantic only). Nothing here
imports SQLAlchemy, Graphiti, FastAPI, or any adapter.
"""

from vera.shared.errors import (
    Conflict,
    DomainError,
    Err,
    InfrastructureError,
    NotFound,
    Ok,
    PolicyRejected,
    Result,
    VeraError,
    is_err,
    is_ok,
)
from vera.shared.ids import VERA_NAMESPACE, deterministic_id, uuid7
from vera.shared.time import utc_now
from vera.shared.types import (
    CanonicalEntityId,
    GroupId,
    JsonDict,
    SourceId,
    empty_json,
)

__all__ = [
    "VERA_NAMESPACE",
    "CanonicalEntityId",
    "Conflict",
    "DomainError",
    "Err",
    "GroupId",
    "InfrastructureError",
    "JsonDict",
    "NotFound",
    "Ok",
    "PolicyRejected",
    "Result",
    "SourceId",
    "VeraError",
    "deterministic_id",
    "empty_json",
    "is_err",
    "is_ok",
    "utc_now",
    "uuid7",
]
