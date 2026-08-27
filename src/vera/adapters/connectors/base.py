"""Shared helpers for connectors."""

from __future__ import annotations

import re
from datetime import datetime

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def strip_html(value: str) -> str:
    """A naive tag strip good enough to turn storage/HTML into searchable text."""
    return _WS.sub(" ", _TAG.sub(" ", value)).strip()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
