"""Error model.

Two kinds, deliberately kept apart:

* **Domain outcomes**: expected business branches such as not found, conflict, or
  policy rejection. Returned as values inside :data:`Result`, never raised, so the
  type signature forces the caller to handle them.
* **Infrastructure failures**: Postgres down, queue unreachable, graph error.
  Raised as exceptions (subclasses of :class:`VeraError`) and handled at a
  boundary (HTTP problem+json for the API, retry then DLQ for the worker).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeGuard, TypeVar, Union

# --------------------------------------------------------------- domain errors ---


@dataclass(frozen=True, slots=True)
class DomainError:
    """Base for expected domain outcomes (used as a value, not raised)."""

    message: str
    code: str = "domain_error"


@dataclass(frozen=True, slots=True)
class NotFound(DomainError):
    code: str = "not_found"


@dataclass(frozen=True, slots=True)
class Conflict(DomainError):
    code: str = "conflict"


@dataclass(frozen=True, slots=True)
class PolicyRejected(DomainError):
    code: str = "policy_rejected"


@dataclass(frozen=True, slots=True)
class Forbidden(DomainError):
    """The actor is authenticated but lacks the role for this action."""

    code: str = "forbidden"


# ---------------------------------------------------------------- Result type ---

T = TypeVar("T")
E = TypeVar("E", bound=DomainError)


@dataclass(frozen=True, slots=True)
class Ok(Generic[T]):
    value: T


@dataclass(frozen=True, slots=True)
class Err(Generic[E]):
    error: E


Result: TypeAlias = Union[Ok[T], Err[E]]  # noqa: UP007  Union stays subscriptable as an alias
"""A value or a domain error. Infra failures are exceptions, never an ``Err``."""


def is_ok(result: Result[T, E]) -> TypeGuard[Ok[T]]:
    return isinstance(result, Ok)


def is_err(result: Result[T, E]) -> TypeGuard[Err[E]]:
    return isinstance(result, Err)


# ------------------------------------------------------- infrastructure errors ---


class VeraError(Exception):
    """Base for infrastructure/programmer errors that should bubble to a boundary."""


class InfrastructureError(VeraError):
    """A dependency (DB, queue, object store, graph) failed."""


class ConfigError(VeraError):
    """Invalid or missing configuration detected at startup."""
