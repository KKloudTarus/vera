"""VERA MCP server: the safe, minimal surface AI clients connect to.

Stateless (MCP spec 2026-07-28), so it scales behind an ordinary load balancer. When
a JWT secret is configured it runs as an OAuth 2.1 Resource Server (RFC 9728) and the
SDK returns 401 with protected-resource metadata for unauthenticated calls. Tools
expose only reads and proposals, never raw graph mutation, and every tool resolves the
caller's scopes server-side from its principal.
"""
# Tools are registered by the @server.tool() decorator's side effect, so the local
# function names are intentionally not referenced again.
# pyright: reportUnusedFunction=false

from __future__ import annotations

from typing import Any
from uuid import UUID

from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer
from pydantic import AnyHttpUrl

from vera import __version__
from vera.adapters.mcp.auth import JwtTokenVerifier
from vera.adapters.persistence.repositories.scope import SqlAlchemyScopeResolver
from vera.bootstrap import Container, build_container
from vera.config.settings import Settings, get_settings
from vera.entrypoints.mcp.service import VeraMcpService
from vera.observability import configure_logging, get_logger

log = get_logger(__name__)


def _principal_id() -> UUID:
    token = get_access_token()
    if token is None or not token.subject:
        raise PermissionError("no authenticated principal")
    return UUID(token.subject)


def build_server(container: Container, settings: Settings) -> MCPServer:
    service = VeraMcpService(container, SqlAlchemyScopeResolver(container.sessionmaker))

    token_verifier = None
    auth = None
    if settings.mcp.jwt_secret is not None:
        token_verifier = JwtTokenVerifier(
            secret=settings.mcp.jwt_secret.get_secret_value(),
            algorithm=settings.mcp.jwt_algorithm,
            issuer=settings.mcp.auth_issuer,
            audience=settings.mcp.auth_audience,
            required_scopes=settings.mcp.required_scopes,
        )
        auth = AuthSettings(
            issuer_url=AnyHttpUrl(settings.mcp.auth_issuer),
            resource_server_url=AnyHttpUrl(settings.mcp.auth_audience),
            required_scopes=settings.mcp.required_scopes,
        )

    server: MCPServer = MCPServer(
        name="vera",
        version=__version__,
        instructions=(
            "Verified organizational memory. Search shared knowledge and propose new facts."
        ),
        token_verifier=token_verifier,
        auth=auth,
    )

    @server.tool()
    async def memory_search(query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Search verified memory in the caller's scopes. Returns ranked facts with provenance."""
        return await service.search(_principal_id(), query=query, limit=limit)

    @server.tool()
    async def memory_get_context(query: str, limit: int = 5) -> list[dict[str, Any]]:
        """Return the most relevant verified facts as context for a question."""
        return await service.get_context(_principal_id(), query=query, limit=limit)

    @server.tool()
    async def memory_explain(query: str) -> list[dict[str, Any]]:
        """Explain the top matches for a query with their source and verification."""
        return await service.explain(_principal_id(), query=query)

    @server.tool()
    async def memory_get_source(source_id: str) -> dict[str, Any] | None:
        """Return the provenance of one published fact, if the caller may see it."""
        return await service.get_source(_principal_id(), source_id=source_id)

    @server.tool()
    async def memory_recent_changes(limit: int = 20) -> list[dict[str, Any]]:
        """List recently published facts across the caller's scopes."""
        return await service.recent_changes(_principal_id(), limit=limit)

    @server.tool()
    async def memory_propose(subject: str, predicate: str, object: str) -> dict[str, Any]:
        """Propose a fact. It enters the caller's personal scope as an unverified proposal."""
        return await service.propose(
            _principal_id(), subject=subject, predicate=predicate, obj=object
        )

    @server.tool()
    async def memory_feedback(
        result_ref: str,
        signal: str,
        query: str = "",
        signals: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """Give feedback on a result. `signal` is 'up' or 'down'. Pass back the `query`
        and the result's `signals` from search so the vote can calibrate ranking.
        """
        return await service.feedback(
            _principal_id(), result_ref=result_ref, signal=signal, query=query, signals=signals
        )

    return server


def create_app() -> Any:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=settings.log_level)
    container = build_container(settings)
    log.info("mcp.startup", auth="jwt" if settings.mcp.jwt_secret else "disabled")
    return build_server(container, settings).streamable_http_app()


def main() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run(create_app(), host=settings.mcp.host, port=settings.mcp.port)


if __name__ == "__main__":
    main()
