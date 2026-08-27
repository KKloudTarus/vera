"""Point-in-time query parsing for the MCP search tool."""

from __future__ import annotations

from datetime import UTC, datetime

from vera.entrypoints.mcp.service import _parse_as_of


def test_none_and_empty_are_none() -> None:
    assert _parse_as_of(None) is None
    assert _parse_as_of("") is None


def test_iso_with_offset_parses() -> None:
    parsed = _parse_as_of("2026-01-02T03:04:05+00:00")
    assert parsed == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def test_trailing_z_is_accepted() -> None:
    parsed = _parse_as_of("2026-01-02T03:04:05Z")
    assert parsed == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
