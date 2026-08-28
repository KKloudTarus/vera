"""Structure-aware chunking (Phase 2, ADR-0005)."""

from __future__ import annotations

from uuid import UUID

from vera.application.curation.chunking import chunk_artifact

_VERSION = UUID("44444444-4444-4444-4444-444444444444")


def _chunk(text: str, content_type: str, **kw: object) -> list:
    return chunk_artifact(
        text=text, content_type=content_type, artifact_version_id=_VERSION, group_id="g", **kw
    )


def test_markdown_splits_by_heading_and_keeps_the_path() -> None:
    text = (
        "# Guide\n\nIntro prose here.\n\n"
        "## Setup\n\nInstall the thing.\n\n"
        "### Auth\n\nUse an API key.\n"
    )
    chunks = _chunk(text, "text/markdown")
    paths = {c.heading_path for c in chunks}
    assert "Guide" in paths
    assert "Guide > Setup" in paths
    assert "Guide > Setup > Auth" in paths


def test_re_chunking_unchanged_text_is_deterministic() -> None:
    text = "# A\n\nFirst sentence. Second sentence.\n"
    a = _chunk(text, "text/markdown")
    b = _chunk(text, "text/markdown")
    assert [c.chunk_key for c in a] == [c.chunk_key for c in b]
    assert [c.ordinal for c in a] == list(range(len(a)))


def test_large_document_splits_into_bounded_chunks() -> None:
    sentences = " ".join(f"This is sentence number {i} with some filler words." for i in range(200))
    chunks = _chunk(sentences, "text/plain", max_tokens=40)
    assert len(chunks) > 5  # a long page yields many chunks
    assert all(c.token_count <= 80 for c in chunks)  # each bounded near the target


def test_prose_offsets_point_into_the_source() -> None:
    text = "Alpha runs first. Beta runs second. Gamma runs third."
    chunks = _chunk(text, "text/plain", max_tokens=8)
    for c in chunks:
        assert c.start_offset is not None and c.end_offset is not None
        assert 0 <= c.start_offset < c.end_offset <= len(text)


def test_code_splits_by_symbol_with_line_ranges() -> None:
    code = (
        "import os\n\n"
        "def alpha():\n    return 1\n\n"
        "class Beta:\n    def method(self):\n        return 2\n"
    )
    chunks = _chunk(code, "text/x-python")
    symbols = [c.symbol_name for c in chunks if c.symbol_name]
    assert "alpha" in symbols
    assert "Beta" in symbols
    for c in chunks:
        if c.symbol_name:
            assert c.start_line is not None and c.end_line is not None
            assert c.start_line <= c.end_line


def test_empty_text_yields_no_chunks() -> None:
    assert _chunk("   \n  ", "text/markdown") == []
