"""Shared helpers for connectors."""

from __future__ import annotations

import re
from datetime import datetime
from html.parser import HTMLParser

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_SPACES = re.compile(r"[ \t]+")
_BLANKS = re.compile(r"\n{3,}")
_HEADINGS = frozenset({"h1", "h2", "h3", "h4", "h5", "h6"})


def strip_html(value: str) -> str:
    """A naive tag strip good enough to turn storage/HTML into searchable text."""
    return _WS.sub(" ", _TAG.sub(" ", value)).strip()


class _MarkdownExtractor(HTMLParser):
    """Turn Confluence storage XHTML into deterministic Markdown, preserving the heading
    hierarchy, lists, tables and links that structure-aware chunking needs. Unknown macros
    (``ac:*``/``ri:*``) are dropped to their text content.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._link_href: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _HEADINGS:
            self._parts.append("\n\n" + "#" * int(tag[1]) + " ")
        elif tag == "p":
            self._parts.append("\n\n")
        elif tag == "li":
            self._parts.append("\n- ")
        elif tag == "br" or tag == "tr":
            self._parts.append("\n")
        elif tag in ("td", "th"):
            self._parts.append(" | ")
        elif tag == "a":
            self._link_href = dict(attrs).get("href")

    def handle_endtag(self, tag: str) -> None:
        if tag in _HEADINGS or tag == "p":
            self._parts.append("\n\n")
        elif tag == "a" and self._link_href:
            self._parts.append(f" ({self._link_href})")
            self._link_href = None

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def markdown(self) -> str:
        text = "".join(self._parts)
        text = _SPACES.sub(" ", text)
        text = _BLANKS.sub("\n\n", text)
        # Trim trailing spaces on each line so chunk keys are stable across runs.
        text = "\n".join(line.rstrip() for line in text.splitlines())
        return text.strip()


def storage_to_markdown(value: str) -> str:
    """Convert Confluence storage-format XHTML to Markdown, keeping headings and structure so
    the artifact can be chunked by heading rather than flattened to one text blob (gap 10).
    """
    if not value:
        return ""
    parser = _MarkdownExtractor()
    parser.feed(value)
    parser.close()
    return parser.markdown()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
