"""Structure-aware chunking (Phase 2, ADR-0005).

Turns an artifact version's text into citable, retrieval-sized ``Chunk`` value objects with
exact source coordinates. Markdown and Confluence split by heading, keeping the heading path;
prose splits on sentence boundaries into bounded token windows with a small overlap; code
splits by top-level symbol where the language is recognized, else by line windows. Everything
is deterministic: the same input yields the same chunks and the same ``chunk_key``, so
re-chunking an unchanged version is a no-op.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from vera.domain.knowledge.fabric import Chunk, chunk_key
from vera.shared.ids import uuid7

_HEADING = re.compile(r"^(#{1,6})[ \t]+(.*?)\s*$")
# Split after sentence punctuation when the next token starts a new sentence. The negative
# class (not space, digit, or ASCII lowercase) keeps English behavior (no split on "e.g." or
# a decimal) while also breaking before an accented uppercase start (Vietnamese Đ/Ơ/Ư, etc.).
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[^\s\da-z])")
_PY_SYMBOL = re.compile(r"^(async\s+def|def|class)\s+([A-Za-z_][A-Za-z0-9_]*)")

_DEFAULT_MAX_TOKENS = 512
_OVERLAP_SENTENCES = 1


def _estimate_tokens(text: str) -> int:
    return max(1, (len(text) + 3) // 4)


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class _Segment:
    heading_path: str | None
    body: str
    base_offset: int  # offset of body[0] in the original text


def _markdown_segments(text: str) -> list[_Segment]:
    """Split into one segment per heading section, carrying the full heading path (e.g.
    'Guide > Setup > Auth'). Text before the first heading is its own untitled segment.
    """
    segments: list[_Segment] = []
    stack: list[tuple[int, str]] = []  # (level, title)
    body_lines: list[str] = []
    body_start = 0
    offset = 0

    def flush(current_path: str | None) -> None:
        body = "".join(body_lines)
        if body.strip():
            segments.append(_Segment(heading_path=current_path, body=body, base_offset=body_start))

    current_path: str | None = None
    for line in text.splitlines(keepends=True):
        match = _HEADING.match(line.rstrip("\n"))
        if match is not None:
            flush(current_path)
            level = len(match.group(1))
            title = match.group(2).strip()
            while stack and stack[-1][0] >= level:
                stack.pop()
            stack.append((level, title))
            current_path = " > ".join(t for _, t in stack)
            body_lines = []
            body_start = offset + len(line)
        else:
            if not body_lines:
                body_start = offset
            body_lines.append(line)
        offset += len(line)
    flush(current_path)
    return segments or [_Segment(heading_path=None, body=text, base_offset=0)]


def _sentences(body: str, base_offset: int) -> list[tuple[str, int, int]]:
    """Sentences as (text, start_offset, end_offset) with offsets into the original text."""
    stripped = body.strip()
    if not stripped:
        return []
    pieces = _SENTENCE_SPLIT.split(body)
    result: list[tuple[str, int, int]] = []
    cursor = 0
    for piece in pieces:
        if not piece.strip():
            cursor += len(piece)
            continue
        start = body.index(piece, cursor)
        end = start + len(piece)
        cursor = end
        result.append((piece.strip(), base_offset + start, base_offset + end))
    return result


def _enforce_sentence_bound(
    sentences: list[tuple[str, int, int]], max_tokens: int
) -> list[tuple[str, int, int]]:
    """Hard-split any single sentence that alone exceeds the token budget, so no chunk can be
    unbounded (a page with one giant sentence still yields bounded chunks). Offsets are kept.
    """
    budget = max_tokens * 4  # ~4 chars per token
    out: list[tuple[str, int, int]] = []
    for text, start, end in sentences:
        if _estimate_tokens(text) <= max_tokens:
            out.append((text, start, end))
            continue
        for offset in range(0, len(text), budget):
            piece = text[offset : offset + budget]
            out.append((piece, start + offset, start + offset + len(piece)))
    return out


def _window(
    sentences: list[tuple[str, int, int]], max_tokens: int, overlap: int
) -> list[tuple[str, int, int]]:
    """Pack sentences into bounded token windows with a small sentence overlap. Each sentence
    is already bounded (see ``_enforce_sentence_bound``), so every window is bounded too.
    """
    if not sentences:
        return []
    windows: list[tuple[str, int, int]] = []
    i = 0
    n = len(sentences)
    while i < n:
        window: list[tuple[str, int, int]] = []
        tokens = 0
        j = i
        while j < n:
            text, _, _ = sentences[j]
            t = _estimate_tokens(text)
            if window and tokens + t > max_tokens:
                break
            window.append(sentences[j])
            tokens += t
            j += 1
        text = " ".join(s for s, _, _ in window)
        windows.append((text, window[0][1], window[-1][2]))
        if j >= n:
            break
        i = max(j - overlap, i + 1)  # small overlap, always make progress
    return windows


def _code_units(text: str) -> list[tuple[str | None, str, int, int]]:
    """Top-level Python symbols as (symbol_name, text, start_line, end_line); a preamble
    before the first symbol is emitted with no symbol name. Falls back to one unit if no
    symbols are found (a line-window split is applied by the caller).
    """
    lines = text.splitlines(keepends=True)
    starts: list[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        m = _PY_SYMBOL.match(line)
        if m is not None:
            starts.append((idx, m.group(2)))
    if not starts:
        return []
    units: list[tuple[str | None, str, int, int]] = []
    if starts[0][0] > 0:
        units.append((None, "".join(lines[: starts[0][0]]), 1, starts[0][0]))
    for k, (line_idx, name) in enumerate(starts):
        end_idx = starts[k + 1][0] if k + 1 < len(starts) else len(lines)
        units.append((name, "".join(lines[line_idx:end_idx]), line_idx + 1, end_idx))
    return units


def chunk_artifact(
    *,
    text: str,
    content_type: str,
    artifact_version_id: object,
    group_id: str,
    max_tokens: int = _DEFAULT_MAX_TOKENS,
    overlap_sentences: int = _OVERLAP_SENTENCES,
) -> list[Chunk]:
    """Chunk an artifact version's text into citable pieces. ``artifact_version_id`` is a UUID
    (accepted as ``object`` so the caller need not import uuid here).
    """
    ct = content_type.lower()
    chunks: list[Chunk] = []
    ordinal = 0

    def emit(
        body: str,
        *,
        heading_path: str | None,
        start_offset: int | None,
        end_offset: int | None,
        symbol_name: str | None = None,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> None:
        nonlocal ordinal
        content = body.strip()
        if not content:
            return
        content_hash = _hash(content)
        chunks.append(
            Chunk(
                id=uuid7(),
                artifact_version_id=artifact_version_id,  # type: ignore[arg-type]
                group_id=group_id,
                chunk_key=chunk_key(
                    artifact_version_id=artifact_version_id,  # type: ignore[arg-type]
                    ordinal=ordinal,
                    content_hash=content_hash,
                ),
                ordinal=ordinal,
                text=content,
                content_hash=content_hash,
                token_count=_estimate_tokens(content),
                heading_path=heading_path,
                start_offset=start_offset,
                end_offset=end_offset,
                symbol_name=symbol_name,
                start_line=start_line,
                end_line=end_line,
            )
        )
        ordinal += 1

    if "python" in ct or ct.endswith("x-python"):
        units = _code_units(text)
        if units:
            for symbol_name, body, start_line, end_line in units:
                emit(
                    body,
                    heading_path=None,
                    start_offset=None,
                    end_offset=None,
                    symbol_name=symbol_name,
                    start_line=start_line,
                    end_line=end_line,
                )
            return chunks
        # No symbols recognized: fall through to windowed prose over the raw text.

    if "markdown" in ct or "confluence" in ct or ct in ("text/md", "text/x-markdown"):
        segments = _markdown_segments(text)
    else:
        segments = [_Segment(heading_path=None, body=text, base_offset=0)]

    for segment in segments:
        windows = _window(
            _enforce_sentence_bound(_sentences(segment.body, segment.base_offset), max_tokens),
            max_tokens,
            overlap_sentences,
        )
        if not windows and segment.body.strip():
            windows = [
                (segment.body.strip(), segment.base_offset, segment.base_offset + len(segment.body))
            ]
        for body, start_offset, end_offset in windows:
            emit(
                body,
                heading_path=segment.heading_path,
                start_offset=start_offset,
                end_offset=end_offset,
            )
    return chunks
