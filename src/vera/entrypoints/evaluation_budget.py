"""Propagate an isolated evaluation action's provider budget across HTTP boundaries."""

from __future__ import annotations

from starlette.types import ASGIApp, Receive, Scope, Send

from vera.observability.cost import (
    ProviderBudgetContext,
    reset_provider_budget_context,
    set_provider_budget_context,
)


class EvaluationBudgetMiddleware:
    def __init__(self, app: ASGIApp, *, scope_id: str) -> None:
        self._app = app
        self._scope_id = scope_id

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        token: object | None = None
        if scope["type"] == "http":
            headers = {key.lower(): value for key, value in scope.get("headers", [])}
            declared_scope = headers.get(b"x-vera-eval-scope", b"").decode("utf-8", "ignore")
            action_key = headers.get(b"x-vera-provider-budget", b"").decode("utf-8", "ignore")
            if declared_scope == self._scope_id and 0 < len(action_key) <= 512:
                token = set_provider_budget_context(ProviderBudgetContext(action_key))
        try:
            await self._app(scope, receive, send)
        finally:
            if token is not None:
                reset_provider_budget_context(token)
