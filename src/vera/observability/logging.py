"""Structured logging with ``structlog``.

Design decisions (see the VERA Engineering Standards):
- JSON in production, pretty console in dev.
- ``contextvars`` binding so ``correlation_id`` / ``tenant`` / ``group_id`` ride
  every log line across ``await`` boundaries and ``TaskGroup`` tasks.
- A mandatory redaction processor so secrets and PII never reach the log stream, even
  if a caller passes a whole settings/headers dict.
- The stdlib root logger is routed through the same renderer so third-party logs
  (uvicorn, sqlalchemy, graphiti) come out in the same format.
"""

from __future__ import annotations

import logging
from collections.abc import MutableMapping
from typing import Any, cast

import structlog
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars,
    merge_contextvars,
)
from structlog.typing import EventDict, WrappedLogger

# Keys whose values must never be logged, matched case-insensitively.
_SENSITIVE_KEYS: frozenset[str] = frozenset(
    {
        "authorization",
        "api_key",
        "apikey",
        "openai_api_key",
        "anthropic_api_key",
        "access_key",
        "secret_key",
        "password",
        "passwd",
        "token",
        "access_token",
        "refresh_token",
        "secret",
        "cookie",
        "set-cookie",
    }
)
_REDACTED = "[redacted]"


def _redact_sensitive(_logger: WrappedLogger, _method: str, event_dict: EventDict) -> EventDict:
    """Redact sensitive keys in the event dict, checking one level of nesting."""
    for key in list(event_dict.keys()):
        if key.lower() in _SENSITIVE_KEYS:
            event_dict[key] = _REDACTED
            continue
        value = event_dict[key]
        if isinstance(value, MutableMapping):
            nested = cast("MutableMapping[str, Any]", value)
            for inner_key in list(nested.keys()):
                if inner_key.lower() in _SENSITIVE_KEYS:
                    nested[inner_key] = _REDACTED
    return event_dict


def configure_logging(*, json: bool, level: str = "INFO") -> None:
    """Configure structlog + the stdlib root logger. Call once at process start."""
    numeric_level = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)

    shared_processors: list[Any] = [
        merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        _redact_sensitive,
    ]

    renderer: Any = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        cache_logger_on_first_use=True,
    )

    # Route stdlib logs (uvicorn, sqlalchemy, graphiti, …) through the same renderer.
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processor=renderer,
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(numeric_level)

    # Tame the noisiest third parties.
    for noisy in ("uvicorn.access", "sqlalchemy.engine.Engine"):
        logging.getLogger(noisy).setLevel(max(numeric_level, logging.WARNING))


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound logger. Prefer ``get_logger(__name__)`` per module."""
    return structlog.get_logger(name)


def bind_log_context(**kwargs: Any) -> None:
    """Bind key/values to the current async context (survives ``await``)."""
    bind_contextvars(**kwargs)


def clear_log_context() -> None:
    """Clear the current async logging context (call at request/message boundaries)."""
    clear_contextvars()
