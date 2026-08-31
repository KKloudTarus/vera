"""Stable, structured errors for the MCP surface.

A client and its agent need to branch on an error, so every anticipated failure is
raised as an ``MCPError`` carrying a JSON-RPC integer ``code`` plus a stable string
``code`` in the error ``data``. The SDK re-raises ``MCPError`` as a top-level protocol
error (an ordinary exception becomes an ``isError`` result with a generic message), so
raising these keeps the machine-readable contract intact while leaking nothing from an
unexpected crash. The messages here are safe by construction: they never embed a query,
a principal id, or an internal exception string.
"""

from __future__ import annotations

from typing import Any

from mcp.shared.exceptions import MCPError
from mcp_types import INTERNAL_ERROR, INVALID_PARAMS

# Application error codes in the JSON-RPC server-reserved range (-32000..-32099). The
# stable string in ``data.code`` is the contract a client branches on; the integer is
# for transport-level tooling.
_UNAUTHENTICATED = -32001
_UNAUTHORIZED = -32002
_QUOTA_EXCEEDED = -32003
_AMBIGUOUS_PROJECT = -32004
_PROJECT_OUT_OF_SCOPE = -32005
_EXPIRED_CONTEXT_PACK = -32006
_UNSUPPORTED_VERSION = -32007


def _error(code: int, slug: str, message: str, **details: Any) -> MCPError:
    data: dict[str, Any] = {"code": slug}
    data.update(details)
    return MCPError(code=code, message=message, data=data)


def unauthenticated(message: str = "authentication required") -> MCPError:
    return _error(_UNAUTHENTICATED, "unauthenticated", message)


def unauthorized(required_scope: str) -> MCPError:
    """The caller is authenticated but lacks the tool's authorization class."""
    return _error(
        _UNAUTHORIZED,
        "unauthorized",
        "this tool requires an additional authorization scope",
        required_scope=required_scope,
    )


def invalid_input(field: str, reason: str) -> MCPError:
    """A bounded input was out of range. Names the field, never its value."""
    return _error(
        INVALID_PARAMS, "invalid_input", f"invalid value for {field}: {reason}", field=field
    )


def quota_exceeded(bucket: str) -> MCPError:
    return _error(
        _QUOTA_EXCEEDED,
        "quota_exceeded",
        "rate quota exceeded for this principal; retry later",
        bucket=bucket,
    )


def ambiguous_project() -> MCPError:
    return _error(
        _AMBIGUOUS_PROJECT,
        "ambiguous_project",
        "the principal can read several projects; pass an explicit project",
    )


def project_out_of_scope() -> MCPError:
    return _error(
        _PROJECT_OUT_OF_SCOPE, "project_out_of_scope", "the requested project is not in scope"
    )


def expired_context_pack() -> MCPError:
    return _error(
        _EXPIRED_CONTEXT_PACK,
        "expired_context_pack",
        "the context pack has expired or does not exist",
    )


def unsupported_version(detail: str) -> MCPError:
    return _error(_UNSUPPORTED_VERSION, "unsupported_version", detail)


def internal() -> MCPError:
    """A redacted stand-in for an unexpected failure, carrying no internal text."""
    return _error(INTERNAL_ERROR, "internal_error", "an internal error occurred")
