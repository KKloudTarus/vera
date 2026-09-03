"""One decorator that hardens every MCP tool: authorize, bound, quota, redact.

``Guard.tool`` replaces a bare ``@server.tool()``. It registers the tool with its
behavioral annotations and wraps the body so that, before it runs, the caller's
authorization class, input bounds, and abuse quota are checked, and so that a scope
failure from the service layer is turned into a stable structured error rather than a
message that names the principal. The wrapper keeps the wrapped function's signature
(``functools.wraps`` plus the SDK's ``inspect.signature`` follow of ``__wrapped__``), so
the advertised tool schema is unchanged.
"""

from __future__ import annotations

import functools
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.mcpserver import MCPServer
from mcp.shared.exceptions import MCPError

from vera.application.snapshot import (
    ContextPackExpiredError,
    ContextPackQuotaExceededError,
    SnapshotNotFoundError,
    SnapshotNotReproducibleError,
)
from vera.config.settings import Settings
from vera.domain.ports.resilience import QuotaLimiter
from vera.entrypoints.knowledge.service import InputError
from vera.entrypoints.knowledge.service import ScopeError as KnowledgeScopeError
from vera.entrypoints.mcp import errors, policy
from vera.entrypoints.mcp.policy import ToolClass
from vera.entrypoints.mcp.profiles import tool_is_visible
from vera.entrypoints.mcp.service import ScopeError as McpScopeError
from vera.observability import get_logger
from vera.observability.metrics import record_mcp_tool
from vera.observability.tracing import span
from vera.shared.errors import VeraError

_Tool = TypeVar("_Tool", bound=Callable[..., Awaitable[Any]])
log = get_logger(__name__)


def _map_scope_error(exc: Exception) -> MCPError:
    """Redact a service scope failure into a stable code. The raw message can name the
    principal or a private reason, so only its shape (ambiguous vs. out of scope) crosses
    to the client.
    """
    if "ambiguous" in str(exc):
        return errors.ambiguous_project()
    return errors.project_out_of_scope()


class Guard:
    """Applies the per-tool policy at registration and at call time.

    In the local-dev profile (local environment, no token verifier) the single local
    principal holds every class, so scope checks are skipped; quotas and bounds still
    apply. In the remote-authenticated profile the caller's token scopes decide access.
    """

    def __init__(self, server: MCPServer, settings: Settings, quota: QuotaLimiter) -> None:
        self._server = server
        self._settings = settings
        self._quota = quota
        self._local = (
            settings.environment == "local"
            and settings.mcp.jwt_secret is None
            and settings.mcp.oauth_signing_key is None
            and settings.mcp.oauth_jwks_url is None
        )

    def tool(
        self,
        tool_class: ToolClass,
        *,
        read_only: bool | None = None,
        idempotent: bool | None = None,
        destructive: bool = False,
    ) -> Callable[[_Tool], _Tool]:
        annotations = policy.annotations_for(
            tool_class,
            read_only=read_only,
            idempotent=idempotent,
            destructive=destructive,
        )

        def deco(fn: _Tool) -> _Tool:
            name = fn.__name__
            if not tool_is_visible(self._settings.mcp.tool_profile, name):
                return fn

            @self._server.tool(annotations=annotations)
            @functools.wraps(fn)
            async def wrapper(**kwargs: Any) -> Any:
                failure: MCPError | None = None
                result: Any = None
                with span("vera.mcp.tool", tool=name, tool_class=tool_class.value) as tool_span:
                    started = time.perf_counter()
                    try:
                        await self._enforce(name, tool_class, kwargs)
                        result = await fn(**kwargs)
                    except MCPError as exc:
                        failure = exc
                    except InputError as exc:
                        failure = errors.invalid_input(exc.field, exc.reason)
                    except ContextPackExpiredError:
                        failure = errors.expired_context_pack()
                    except ContextPackQuotaExceededError:
                        failure = errors.quota_exceeded("context_storage")
                    except (SnapshotNotFoundError, SnapshotNotReproducibleError):
                        failure = errors.invalid_input("snapshot_id", "is unavailable")
                    except (KnowledgeScopeError, McpScopeError) as exc:
                        failure = _map_scope_error(exc)
                    except VeraError:
                        # Infrastructure messages can carry connection or tenant details.
                        failure = errors.internal()
                    except Exception as exc:
                        log.error(
                            "mcp_tool_unexpected_error",
                            tool=name,
                            exception_type=type(exc).__name__,
                        )
                        failure = errors.internal()

                    if failure is not None:
                        tool_span.set_attribute("vera.result", "error")
                        record_mcp_tool(
                            tool=name,
                            result="error",
                            duration_s=time.perf_counter() - started,
                        )
                    else:
                        tool_span.set_attribute("vera.result", "success")
                        record_mcp_tool(
                            tool=name,
                            result="success",
                            duration_s=time.perf_counter() - started,
                        )

                if failure is not None:
                    raise failure from None
                return result

            return wrapper  # type: ignore[return-value]  # wraps preserves fn's signature

        return deco

    def require(self, tool_class: ToolClass) -> str:
        """Require an additional authorization class for a conditional write path."""
        return self._authorize(tool_class)

    async def _enforce(self, name: str, tool_class: ToolClass, kwargs: dict[str, Any]) -> None:
        principal = self._authorize(tool_class)
        policy.validate_bounds(name, kwargs)
        rule = policy.quota_for(name, tool_class, self._settings.mcp)
        if rule is not None:
            key = f"{principal}:{rule.bucket}"
            if not await self._quota.allow(
                key, limit=rule.limit, window_seconds=rule.window_seconds
            ):
                raise errors.quota_exceeded(rule.bucket)

    def _authorize(self, tool_class: ToolClass) -> str:
        """Return the caller's principal id, enforcing the tool's scope in remote mode."""
        if self._local:
            return str(self._settings.mcp.local_principal_id)
        token = get_access_token()
        if token is None or not token.subject:
            raise errors.unauthenticated()
        required = policy.scope_for(tool_class, self._settings.mcp)
        if required not in set(token.scopes):
            raise errors.unauthorized(required)
        return token.subject
