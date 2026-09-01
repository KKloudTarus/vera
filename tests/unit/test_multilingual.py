"""Multilingual tokenization: sentence splitting and fulltext terms keep non-ASCII text."""

from __future__ import annotations

from vera.adapters.graph.graphiti_adapter import _FULLTEXT_TOKEN
from vera.application.curation.chunking import _SENTENCE_SPLIT
from vera.shared.text import normalize_name


def test_normalize_name_preserves_vietnamese_diacritics() -> None:
    assert normalize_name("Đội nền tảng thanh toán") == "đội nền tảng thanh toán"
    assert normalize_name("Nguyễn Văn A") == "nguyễn văn a"


def test_normalize_name_collapses_separators_and_trims() -> None:
    assert normalize_name("  Payment_Service!!  Core ") == "payment service core"


def test_normalize_name_keeps_accents_significant() -> None:
    # Accents change meaning in Vietnamese, so accented and unaccented forms differ.
    assert normalize_name("má") != normalize_name("ma")
    assert normalize_name("Đội") != normalize_name("Doi")


def test_sentence_split_breaks_before_accented_uppercase() -> None:
    # A Vietnamese sentence starting with an accented capital (Đ) must start a new piece.
    parts = _SENTENCE_SPLIT.split("Câu một ở đây. Đây là câu hai.")
    assert parts == ["Câu một ở đây.", "Đây là câu hai."]


def test_sentence_split_keeps_english_abbreviations_and_decimals() -> None:
    # A lowercase or digit continuation is not a sentence boundary, so English abbreviations
    # and decimals are preserved.
    assert _SENTENCE_SPLIT.split("See e.g. the note.") == ["See e.g. the note."]
    assert _SENTENCE_SPLIT.split("Version 3.14 shipped.") == ["Version 3.14 shipped."]


def test_sentence_split_still_breaks_ascii_sentences() -> None:
    assert _SENTENCE_SPLIT.split("First one. Second one.") == ["First one.", "Second one."]


def test_fulltext_tokenizer_preserves_unicode_terms() -> None:
    terms = _FULLTEXT_TOKEN.findall("Đội nền tảng payment_service 42!")
    # Accented Vietnamese words survive intact; underscore splits; punctuation is dropped.
    assert "Đội" in terms
    assert "nền" in terms and "tảng" in terms
    assert terms == ["Đội", "nền", "tảng", "payment", "service", "42"]
